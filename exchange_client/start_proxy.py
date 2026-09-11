#!/usr/bin/env python3
"""
Start the Tor → GOST → Caddy proxy stack and report the proxied IP address.

Launches three subprocesses (tor, gost, caddy) and waits for the proxy to
become available, then prints the IP address seen by httpbin.org.

Press Ctrl+C to shut down all three processes cleanly.
Press Enter to recheck the proxy IP address (useful to verify connection is still alive).
"""

import atexit
import json
import re
import select
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

from dotenv import load_dotenv

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
load_dotenv(_project_root / ".env", override=False)
load_dotenv(_script_dir / ".env", override=True)

PROXY_CONFIG_DIR = Path(_script_dir) / "proxy-config"
LOG_DIR = PROXY_CONFIG_DIR / "logs"
TOR_NOTICE_LOG = PROXY_CONFIG_DIR / "tor-data" / "notice.log"
# 127.0.0.1 not localhost — some stacks bind IPv4 only; Tor circuits are slow.
HTTPBIN_PROXY_URL = "http://127.0.0.1:9430/ip"

processes: list[subprocess.Popen] = []
_log_handles: list = []


def kill_existing():
    """Kill any leftover tor / gost / caddy processes so ports are free."""
    for name in ("tor", "gost", "caddy"):
        subprocess.run(["pkill", "-x", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1)


def cleanup():
    """Terminate every subprocess that is still running."""
    for proc in reversed(processes):
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 5
    for proc in reversed(processes):
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()
    for handle in _log_handles:
        try:
            handle.close()
        except Exception:
            pass


atexit.register(cleanup)


def handle_signal(signum, _frame):
    """Forward termination signals into the normal cleanup path."""
    cleanup()
    sys.exit(128 + signum)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def _check_children_alive() -> None:
    for proc in processes:
        if proc.poll() is not None:
            print(
                f"ERROR: {proc.args[0]!r} exited with code {proc.returncode}",
                file=sys.stderr,
            )
            _dump_logs()
            cleanup()
            sys.exit(1)


def _dump_logs() -> None:
    for name in ("tor", "gost", "caddy"):
        path = LOG_DIR / f"{name}.log"
        if not path.exists():
            continue
        tail = path.read_text(errors="replace").splitlines()[-20:]
        if not tail:
            continue
        print(f"--- {name}.log (last {len(tail)} lines) ---", file=sys.stderr)
        for line in tail:
            print(f"  {line}", file=sys.stderr)


def _tor_bootstrap_pct() -> int | None:
    if not TOR_NOTICE_LOG.exists():
        return None
    text = TOR_NOTICE_LOG.read_text(errors="replace")
    matches = re.findall(r"Bootstrapped (\d+)%", text)
    if not matches:
        return None
    return int(matches[-1])


def wait_for_tor_bootstrap(timeout: int = 180) -> None:
    """NL StrictNodes circuits can take minutes. Print bootstrap % instead of sitting silent."""
    print("Waiting for Tor bootstrap (ExitNodes {nl}) …")
    start = time.time()
    last_pct = -1
    while time.time() - start < timeout:
        _check_children_alive()
        pct = _tor_bootstrap_pct()
        if pct is not None and pct != last_pct:
            print(f"  tor bootstrap {pct}%")
            last_pct = pct
        if pct is not None and pct >= 100:
            return
        time.sleep(1)
    raise TimeoutError(
        f"Tor did not bootstrap within {timeout}s (last {last_pct if last_pct >= 0 else 0}%). "
        "NL exits can be scarce; see proxy-config/tor-data/notice.log"
    )


def wait_for_proxy(url: str, timeout: int = 90, interval: float = 2.0):
    """Poll *url* until it returns a 200 response or *timeout* seconds elapse."""
    start = time.time()
    last_error = None
    last_report = 0.0
    while time.time() - start < timeout:
        elapsed = time.time() - start
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                if resp.status == 200:
                    return json.loads(resp.read())
        except (urllib.error.URLError, OSError, ValueError, TimeoutError) as exc:
            last_error = exc
        _check_children_alive()
        if elapsed - last_report >= 10:
            print(f"  still waiting for httpbin via Tor … {elapsed:.0f}s  last={last_error}")
            last_report = elapsed
        time.sleep(interval)
    raise TimeoutError(f"Proxy did not become ready within {timeout}s (last error: {last_error})")


def fetch_proxy_ip() -> str:
    """Fetch and return the current proxied IP address."""
    try:
        with urllib.request.urlopen(HTTPBIN_PROXY_URL, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read())
                return data.get("origin", "unknown")
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return f"ERROR: {exc}"
    return "unknown"


def _spawn(name: str, args: list[str]) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{name}.log"
    handle = open(log_path, "w", encoding="utf-8")
    _log_handles.append(handle)
    print(f"Starting {name} …  log={log_path}")
    return subprocess.Popen(
        args,
        cwd=PROXY_CONFIG_DIR,
        stdout=handle,
        stderr=subprocess.STDOUT,
    )


def main():
    kill_existing()
    TOR_NOTICE_LOG.parent.mkdir(parents=True, exist_ok=True)
    if TOR_NOTICE_LOG.exists():
        TOR_NOTICE_LOG.unlink()

    processes.append(_spawn(
        "tor",
        ["tor", "-f", str(PROXY_CONFIG_DIR / "config.torrc"), "--runasdaemon", "0"],
    ))
    processes.append(_spawn("gost", ["gost", "-C", str(PROXY_CONFIG_DIR / "gost.yaml")]))
    processes.append(_spawn(
        "caddy",
        ["caddy", "run", "--config", str(PROXY_CONFIG_DIR / "Caddyfile")],
    ))

    try:
        wait_for_tor_bootstrap()
        print("Waiting for httpbin via the proxy …")
        data = wait_for_proxy(HTTPBIN_PROXY_URL)
    except TimeoutError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        _dump_logs()
        cleanup()
        sys.exit(1)

    origin = data.get("origin", "unknown")
    print(f"\nProxied IP address: {origin}")
    print("Proxy is running. Press Ctrl+C to stop all services.")
    print("Press Enter to recheck proxy IP address.\n")

    # Keep the script alive until interrupted
    try:
        while True:
            # If any process exits, report and bail out
            for proc in processes:
                if proc.poll() is not None:
                    print(f"ERROR: {proc.args[0]!r} exited unexpectedly (code {proc.returncode})", file=sys.stderr)
                    cleanup()
                    sys.exit(1)
            # Check for Enter key (non-blocking via select)
            readable, _, _ = select.select([sys.stdin], [], [], 2)
            if readable:
                sys.stdin.readline()  # consume the newline
                print(f"Proxied IP address: {fetch_proxy_ip()}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
