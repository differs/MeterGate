#!/usr/bin/env python3
"""Full OIDC authorization-code flow against the mock IdP:
   /api/oidc/login → IdP /authorize (auto-approve) → /api/oidc/callback
   → MeterGate exchanges code, verifies id_token (JWKS), auto-registers,
     returns session JWT → JWT works on portal.
"""
import http.cookiejar
import json
import sys
import urllib.parse
import urllib.request

BASE = "http://localhost:3002"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj), NoRedirect)


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "oidc-sim"})
    try:
        with opener.open(req, timeout=10) as r:
            return r.status, r.geturl(), r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Location", e.geturl()), e.read().decode(errors="replace")


def main():
    # 1) start login → 302 to IdP authorize
    st, loc, _ = get(f"{BASE}/api/oidc/login")
    print(f"[1] /api/oidc/login -> {st} → IdP authorize")
    if st not in (302, 307) or "authorize" not in loc:
        print(f"FAIL: expected redirect to IdP, got {loc}")
        return 1

    # 2) IdP auto-approves → 302 back to our callback with ?code=
    st, loc, _ = get(loc)
    print(f"[2] IdP authorize -> {st} → callback?code=...")
    if st not in (302, 307) or "/api/oidc/callback" not in loc:
        print(f"FAIL: expected callback redirect, got {loc}")
        return 1

    # 3) MeterGate callback: exchange code + verify id_token (JWKS) + JWT
    st, _, body = get(loc)
    print(f"[3] /api/oidc/callback -> {st}")
    if st != 200:
        print(f"FAIL: callback error: {body[:200]}")
        return 1
    data = json.loads(body)
    token = data.get("token", "")
    print(f"[4] session JWT: {token[:40]}... (len={len(token)})")

    # 4) use the JWT against the portal (no admin key)
    req = urllib.request.Request(
        f"{BASE}/api/keys?user_id=1",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with opener.open(req, timeout=10) as r:
            print(f"[5] portal with JWT -> {r.status}: {r.read().decode()[:80]}")
            return 0
    except urllib.error.HTTPError as e:
        print(f"[5] portal with JWT -> {e.code}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
