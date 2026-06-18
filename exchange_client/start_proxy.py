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
import os
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

PROXY_CONFIG_DIR = os.path.join(_script_dir, "proxy-config")
HTTPBIN_PROXY_URL = "http://localhost:9430/ip"

processes: list[subprocess.Popen] = []


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
    # Give them a moment, then force-kill stragglers
    deadline = time.time() + 5
    for proc in reversed(processes):
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()


atexit.register(cleanup)


def handle_signal(signum, _frame):
    """Forward termination signals into the normal cleanup path."""
    cleanup()
    sys.exit(128 + signum)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def wait_for_proxy(url: str, timeout: int = 120, interval: float = 2.0):
    """Poll *url* until it returns a 200 response or *timeout* seconds elapse."""
    start = time.time()
    last_error = None
    while time.time() - start < timeout:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    return json.loads(resp.read())
        except (urllib.error.URLError, OSError, ValueError) as exc:
            last_error = exc
        # Check if any process died unexpectedly
        for proc in processes:
            if proc.poll() is not None:
                print(f"ERROR: {proc.args[0]!r} exited with code {proc.returncode}", file=sys.stderr)
                cleanup()
                sys.exit(1)
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


def main():
    kill_existing()

    # 1) Tor
    print("Starting tor …")
    processes.append(subprocess.Popen(
        ["tor", "-f", os.path.join(PROXY_CONFIG_DIR, "config.torrc"), "--runasdaemon", "0"],
        cwd=PROXY_CONFIG_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ))

    # 2) GOST
    print("Starting gost …")
    processes.append(subprocess.Popen(
        ["gost", "-C", os.path.join(PROXY_CONFIG_DIR, "gost.yaml")],
        cwd=PROXY_CONFIG_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ))

    # 3) Caddy
    print("Starting caddy …")
    processes.append(subprocess.Popen(
        ["caddy", "run", "--config", os.path.join(PROXY_CONFIG_DIR, "Caddyfile")],
        cwd=PROXY_CONFIG_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ))

    # Wait for the full chain to come up
    print("Waiting for proxy to become ready …")
    try:
        data = wait_for_proxy(HTTPBIN_PROXY_URL)
    except TimeoutError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        cleanup()
        sys.exit(1)

    origin = data.get("origin", "unknown")
    wallet = os.environ.get("POLYMARKET_PROXY_WALLET", "not set")
    print(f"\nProxied IP address: {origin}")
    print(f"Wallet: {wallet}")
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
