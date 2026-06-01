"""
JSON-RPC client for Polygon endpoints.

Pure library: no environment reads, no CLI, no global process state. The
caller supplies the RPC URL and request parameters. The client manages
per-thread persistent HTTP keep-alive connections, handles retries with
exponential backoff for transient failures, and surfaces structured
exception types so the caller can decide between automatic retry,
range-split, and fail-fast.

The client is fully reusable: one instance can serve many threads (each
gets its own connection via ``threading.local``). Use one client per
unique RPC URL.

Public API:

    RpcClient(rpc_url, config=..., stop_event=...)
    .get_block_number() -> int
    .get_logs(addresses, from_block, to_block, topics=...) -> list[dict]
    .close()

Exception types (every one inherits from ``RPCError``):

    TooManyResults  — provider rejected ``eth_getLogs`` range as too
                      large, or response was truncated. Never retryable;
                      caller must split the range.
    RPCTimeout      — ``eth_getLogs`` request timed out. Never
                      retryable; caller must reduce concurrency or
                      shrink the range.
    HTTPError       — HTTP-level error. Retryable for 408/425/429/5xx;
                      not retryable otherwise.
    JSONRPCError    — JSON-RPC ``error`` field returned by the provider.
                      Not retryable.

Every exception carries an ``is_retryable: bool`` attribute. The client
retries transient errors internally; the caller only sees exceptions
that were either non-retryable or that exhausted the retry budget.
"""

from __future__ import annotations

import gzip
import http.client
import itertools
import json
import ssl
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

from .errors import OperationCancelled


# Keywords used to detect "your range is too big" responses from various
# providers. Only consulted when ``method == "eth_getLogs"`` so we never
# misclassify unrelated errors.
_TOO_MANY_RESULTS_KEYWORDS = (
    "too many",
    "limit",
    "exceed",
    "too large",
    "block range",
    "query returned",
    "response size",
)

# HTTP status codes that warrant retry. Everything else is treated as
# permanent failure.
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class RPCError(Exception):
    """Base class for every error raised by ``RpcClient``.

    Every subclass sets ``is_retryable`` as a class attribute (or, for
    ``HTTPError``, as an instance attribute computed from the status
    code). The retry loop inside ``RpcClient`` consults this attribute
    to decide whether to back off and try again or to raise immediately.
    """

    is_retryable: bool = False


class TooManyResults(RPCError):
    """Provider rejected the ``eth_getLogs`` range as too large, or the
    response was truncated past the provider's size limit.

    The caller is expected to split the block range in half and retry
    each half. The client does NOT do this automatically because it has
    no knowledge of the caller's partition layout.
    """

    is_retryable = False


class RPCTimeout(RPCError):
    """``eth_getLogs`` request timed out at the provider.

    The caller is expected to reduce concurrency or shrink the range
    before retrying. The client does NOT automatically retry because
    re-sending the same request immediately would worsen the overload.
    """

    is_retryable = False


class HTTPError(RPCError):
    """HTTP-level error from the provider.

    ``is_retryable`` is computed from ``status_code``: True for 408,
    425, 429, 500, 502, 503, 504; False otherwise.
    """

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.is_retryable = status_code in _RETRYABLE_HTTP_STATUSES
        super().__init__(f"HTTP {status_code}")


class JSONRPCError(RPCError):
    """JSON-RPC ``error`` field returned by the provider.

    The client never retries any JSON-RPC error — these are signs of a
    malformed request or a provider-side rejection, not a transient
    failure.
    """

    is_retryable = False

    def __init__(self, code: int | None, message: str) -> None:
        self.code = code
        super().__init__(message)


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------

def _parse_hex(value: str | None) -> int:
    """Parse a ``0x``-prefixed hex string into an int.

    ``None`` and ``"0x"`` both return 0; both are how some providers
    spell "the result is zero."
    """
    if not value or value == "0x":
        return 0
    return int(value, 16)


def _to_hex(value: int) -> str:
    """Render a non-negative int as a ``0x``-prefixed lowercase hex
    string suitable for JSON-RPC."""
    return hex(value)


def _is_timeout_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "timed out" in msg or "timeout" in msg


def _is_too_many_results_message(message: str) -> bool:
    msg = message.lower()
    return any(keyword in msg for keyword in _TOO_MANY_RESULTS_KEYWORDS)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RpcClientConfig:
    """Tunable knobs for ``RpcClient``."""

    call_timeout_sec: float = 30.0
    """Per-call socket timeout for normal RPC calls."""

    block_number_timeout_sec: float = 5.0
    """Shorter timeout for ``eth_blockNumber``, which should always be fast."""

    max_retries: int = 3
    """Maximum total attempts per call (including the first)."""

    retry_backoff_base_sec: float = 0.5
    """Base for exponential backoff: ``base * 2**attempt`` seconds."""

    user_agent: str = "polymarket-scraper-v3/1.0"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class RpcClient:
    """Thread-safe JSON-RPC client using per-thread persistent HTTP connections.

    One ``RpcClient`` instance is safe to share across many threads. Each
    thread that calls into the client gets its own
    ``http.client.HTTPSConnection`` via ``threading.local``; this avoids
    TLS-handshake cost on every call without serializing requests
    through a lock.

    Surface area is deliberately small: only ``get_block_number`` and
    ``get_logs`` are exposed. If the caller needs another JSON-RPC
    method, add a new typed method to this class rather than exposing
    the underlying transport.

    The client handles, transparently to the caller:

      * Persistent keep-alive (with a single reconnect on server-closed
        sockets).
      * Gzip-encoded responses.
      * Exponential backoff retry for transient errors. Non-retryable
        errors (``TooManyResults``, ``RPCTimeout``, ``JSONRPCError``,
        permanent ``HTTPError``) raise immediately.
      * Detection of "range too large" responses for ``eth_getLogs``
        (both HTTP 400 and provider-specific JSON error messages),
        which are raised as ``TooManyResults`` for the caller to handle.
    """

    def __init__(
        self,
        rpc_url: str,
        config: RpcClientConfig | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.config = config or RpcClientConfig()
        self.stop_event = stop_event

        parsed = urllib.parse.urlparse(rpc_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported RPC URL scheme: {parsed.scheme}")
        if not parsed.hostname:
            raise ValueError("RPC URL is missing hostname")

        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._path = parsed.path or "/"
        if parsed.query:
            self._path += "?" + parsed.query
        self._is_https = parsed.scheme == "https"
        self._ssl_ctx = ssl.create_default_context() if self._is_https else None

        self._thread_local = threading.local()
        self._id_counter = itertools.count(1)

        self._headers = {
            "Content-Type": "application/json",
            "User-Agent": self.config.user_agent,
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip",
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the persistent HTTP connection for the calling thread.

        Connections are stored per-thread, so this only closes the
        caller's connection handle; connections held by other threads
        are unaffected. To close every thread's connection, each thread
        must call ``close()`` independently.

        Idempotent: safe to call when no connection is open.
        """
        self._discard_connection()

    # ------------------------------------------------------------------
    # Public RPC methods
    # ------------------------------------------------------------------

    def get_block_number(self) -> int:
        """Return the current head block number from the provider.

        Uses the configured ``block_number_timeout_sec`` (shorter than
        the normal call timeout) because providers serve this from a
        hot in-memory pointer.
        """
        result = self._rpc_call(
            "eth_blockNumber",
            [],
            timeout_sec=self.config.block_number_timeout_sec,
        )
        return _parse_hex(result)

    def get_logs(
        self,
        *,
        addresses: list[str],
        from_block: int,
        to_block: int,
        topics: list[list[str] | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Call ``eth_getLogs`` with a block range and an address filter.

        Args:
            addresses: List of contract addresses (case-insensitive hex
                with ``0x`` prefix). The provider returns only logs
                whose ``address`` matches one of these.
            from_block: Inclusive start block.
            to_block: Inclusive end block.
            topics: Optional topics filter, in JSON-RPC native form:
                a list whose ``i``-th element is either ``None``
                (anything matches at position ``i``), a single topic
                hex string, or a list of topic hex strings (any match).
                The most common use is
                ``[event_decoders.WANTED_TOPICS]`` to filter on
                ``topics[0]`` only.

        Returns:
            A list of log dicts in the order returned by the provider.
            Each dict has at least ``address``, ``blockNumber``,
            ``transactionHash``, ``transactionIndex``, ``logIndex``,
            ``topics``, and ``data`` keys.

        Raises:
            TooManyResults: range too large; caller must split.
            RPCTimeout: provider overloaded; caller must back off.
            HTTPError: permanent HTTP error or retry budget exhausted.
            JSONRPCError: provider returned a JSON-RPC error.
            OperationCancelled: caller's ``stop_event`` was set.
        """
        params: dict[str, Any] = {
            "address": addresses,
            "fromBlock": _to_hex(from_block),
            "toBlock": _to_hex(to_block),
        }
        if topics is not None:
            params["topics"] = topics
        result = self._rpc_call("eth_getLogs", [params])
        return result if isinstance(result, list) else []

    # ------------------------------------------------------------------
    # Private: connection management
    # ------------------------------------------------------------------

    def _get_connection(self, timeout_sec: float) -> http.client.HTTPConnection:
        conn = getattr(self._thread_local, "conn", None)
        if conn is not None:
            conn.timeout = timeout_sec
            if conn.sock is not None:
                conn.sock.settimeout(timeout_sec)
            return conn

        if self._is_https:
            conn = http.client.HTTPSConnection(
                self._host,
                self._port,
                timeout=timeout_sec,
                context=self._ssl_ctx,
            )
        else:
            conn = http.client.HTTPConnection(
                self._host,
                self._port,
                timeout=timeout_sec,
            )
        self._thread_local.conn = conn
        return conn

    def _discard_connection(self) -> None:
        conn = getattr(self._thread_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._thread_local.conn = None

    def _backoff_wait(self, attempt: int) -> None:
        delay = self.config.retry_backoff_base_sec * (2**attempt)
        if self.stop_event is not None:
            self.stop_event.wait(delay)
        else:
            time.sleep(delay)

    # ------------------------------------------------------------------
    # Private: the core JSON-RPC transport
    # ------------------------------------------------------------------

    def _rpc_call(
        self,
        method: str,
        params: list[Any],
        timeout_sec: float | None = None,
    ) -> Any:
        """Make one JSON-RPC call. Returns ``data["result"]`` or raises.

        This is the single transport primitive used by every public
        method. It is private because the public surface is the set of
        typed RPC methods (``get_block_number``, ``get_logs``, …) —
        callers should not be making arbitrary JSON-RPC calls through
        this object.

        Returns ``Any`` because the JSON-RPC ``result`` field is
        method-specific. Public methods wrap this with the concrete
        return type they document.
        """
        timeout = (
            timeout_sec if timeout_sec is not None else self.config.call_timeout_sec
        )
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": next(self._id_counter),
            }
        ).encode()

        for attempt in range(self.config.max_retries):
            if self.stop_event is not None and self.stop_event.is_set():
                raise OperationCancelled("RPC call interrupted")
            try:
                conn = self._get_connection(timeout)
                try:
                    conn.request(
                        "POST", self._path, body=payload, headers=self._headers
                    )
                    response = conn.getresponse()
                except (
                    http.client.RemoteDisconnected,
                    ConnectionResetError,
                    BrokenPipeError,
                    OSError,
                ):
                    # Server closed the keep-alive socket; reconnect once.
                    self._discard_connection()
                    conn = self._get_connection(timeout)
                    conn.request(
                        "POST", self._path, body=payload, headers=self._headers
                    )
                    response = conn.getresponse()

                status_code = response.status
                body = response.read()
                if response.getheader("Content-Encoding", "").lower() == "gzip":
                    body = gzip.decompress(body)

                # eth_getLogs returns HTTP 400 from some providers when
                # the range is too big — treat as TooManyResults so the
                # caller knows to split.
                if status_code == 400 and method == "eth_getLogs":
                    raise TooManyResults("HTTP 400")
                if status_code >= 400:
                    # Both retryable and non-retryable codes go through
                    # HTTPError; the retry logic below decides based on
                    # ``is_retryable``.
                    raise HTTPError(status_code)

                data = json.loads(body)
                if "error" in data:
                    error_data = data["error"]
                    message = error_data.get("message", str(error_data))
                    if method == "eth_getLogs" and _is_too_many_results_message(
                        message
                    ):
                        raise TooManyResults(message)
                    code = error_data.get("code")
                    raise JSONRPCError(code, message)
                return data.get("result")
            except (TooManyResults, RPCTimeout, OperationCancelled):
                # Never retried — caller handles range-split, concurrency
                # reduction, or shutdown.
                raise
            except json.JSONDecodeError as exc:
                self._discard_connection()
                # A truncated response is a strong "too many results"
                # signal for eth_getLogs.
                if method == "eth_getLogs":
                    raise TooManyResults(f"Truncated JSON response ({exc})")
                if attempt < self.config.max_retries - 1:
                    self._backoff_wait(attempt)
                    continue
                raise RuntimeError(
                    f"RPC call {method} failed after {self.config.max_retries} "
                    f"attempts: {exc}"
                ) from exc
            except RPCError as exc:
                self._discard_connection()
                if method == "eth_getLogs" and _is_timeout_error(exc):
                    raise RPCTimeout(str(exc)) from exc
                if not exc.is_retryable:
                    raise
                if attempt < self.config.max_retries - 1:
                    self._backoff_wait(attempt)
                    continue
                raise
            except Exception as exc:
                self._discard_connection()
                if method == "eth_getLogs" and _is_timeout_error(exc):
                    raise RPCTimeout(str(exc)) from exc
                if attempt < self.config.max_retries - 1:
                    self._backoff_wait(attempt)
                    continue
                raise RuntimeError(
                    f"RPC call {method} failed after {self.config.max_retries} "
                    f"attempts: {exc}"
                ) from exc

        raise RuntimeError(f"RPC call {method} failed after retries")
