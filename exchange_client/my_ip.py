#!/usr/bin/env python3
"""
Print the IP address and geolocation as seen through the Tor proxy.

Requires the proxy stack to be running (start_proxy.py). Fetches the exit
IP from httpbin.org via the local proxy, then looks up geolocation from
ipinfo.io.
"""

import json
import sys
import urllib.error
import urllib.request

HTTPBIN_PROXY_URL = "http://localhost:9430/ip"
IPINFO_URL = "https://ipinfo.io/{}/json"


def main():
    # Get exit IP through the proxy
    try:
        req = urllib.request.Request(HTTPBIN_PROXY_URL)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            exit_ip = data["origin"]
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        sys.exit(f"Could not reach proxy at {HTTPBIN_PROXY_URL} — is start_proxy.py running?\n{exc}")

    print(f"Exit IP: {exit_ip}")

    # Look up geolocation for that IP
    try:
        info_url = IPINFO_URL.format(exit_ip)
        req = urllib.request.Request(info_url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            info = json.loads(resp.read())
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        print(f"Could not look up geolocation: {exc}")
        return

    city = info.get("city", "?")
    region = info.get("region", "?")
    country = info.get("country", "?")
    org = info.get("org", "?")
    loc = info.get("loc", "?")

    print(f"Location: {city}, {region}, {country}")
    print(f"Coordinates: {loc}")
    print(f"Organization: {org}")


if __name__ == "__main__":
    main()
