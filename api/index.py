"""Vercel Python function entry point for AIM OSI."""
from __future__ import annotations

import json
import base64
import hashlib
import hmac
import os
import secrets
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as core
from http.server import BaseHTTPRequestHandler


class handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"{self.command} {urlparse(self.path).path} {args[1] if len(args)>1 else ''}")

    def json_response(self, status: int, value: object):
        body = json.dumps(value, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location: str, cookie: str | None = None):
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Cache-Control", "no-store")
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def github_auth(self, parsed, route):
        values = (os.getenv("GITHUB_CLIENT_ID"), os.getenv("GITHUB_CLIENT_SECRET"), os.getenv("SESSION_SECRET"))
        if not all(values):
            return self.json_response(503, {"error": "GitHub sign-in is not configured. Add GITHUB_CLIENT_ID, GITHUB_CLIENT_SECRET, and SESSION_SECRET to the Vercel project environment."})
        client_id, client_secret, secret = values
        if route == "/api/auth/github":
            state = secrets.token_urlsafe(32)
            callback = os.getenv("GITHUB_OAUTH_REDIRECT_URI")
            if not callback:
                scheme = self.headers.get("x-forwarded-proto", "https").split(",")[0]
                host = self.headers.get("x-forwarded-host", self.headers.get("host", "")).split(",")[0]
                callback = f"{scheme}://{host}/api/auth/callback"
            destination = "https://github.com/login/oauth/authorize?" + urlencode({"client_id": client_id, "redirect_uri": callback, "scope": "read:user", "state": state})
            return self.redirect(destination, f"aimosi_oauth_state={state}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=600")
        if route == "/api/auth/callback":
            query = parse_qs(parsed.query)
            cookie = self.headers.get("Cookie", "")
            expected = next((part.split("=",1)[1] for part in cookie.split("; ") if part.startswith("aimosi_oauth_state=")), "")
            state = (query.get("state") or [""])[0]
            code = (query.get("code") or [""])[0]
            if not state or not secrets.compare_digest(state, expected) or not code:
                return self.json_response(400, {"error": "GitHub sign-in state check failed. Please start again."})
            callback = os.getenv("GITHUB_OAUTH_REDIRECT_URI")
            if not callback:
                scheme = self.headers.get("x-forwarded-proto", "https").split(",")[0]
                host = self.headers.get("x-forwarded-host", self.headers.get("host", "")).split(",")[0]
                callback = f"{scheme}://{host}/api/auth/callback"
            req = Request("https://github.com/login/oauth/access_token", data=urlencode({"client_id": client_id, "client_secret": client_secret, "code": code, "redirect_uri": callback}).encode(), headers={"Accept":"application/json", "User-Agent":"AIM-OSI"})
            with urlopen(req, timeout=12) as response:
                token = json.loads(response.read().decode()).get("access_token")
            if not token:
                return self.json_response(502, {"error": "GitHub did not return an access token."})
            req = Request("https://api.github.com/user", headers={"Authorization": f"Bearer {token}", "Accept":"application/vnd.github+json", "User-Agent":"AIM-OSI"})
            with urlopen(req, timeout=12) as response:
                user = json.loads(response.read().decode())
            profile = {"login": user.get("login"), "name": user.get("name"), "avatar_url": user.get("avatar_url"), "html_url": user.get("html_url")}
            packed = base64.urlsafe_b64encode(json.dumps(profile, separators=(",", ":")).encode()).decode().rstrip("=")
            signature = hmac.new(secret.encode(), packed.encode(), hashlib.sha256).hexdigest()
            session = f"{packed}.{signature}"
            return self.redirect("/#overview", f"aimosi_session={session}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=2592000")
        if route == "/api/auth/logout":
            return self.redirect("/#overview", "aimosi_session=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0")
        return self.json_response(404, {"error":"Not found"})

    def do_GET(self):
        core.init_db()
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        route = query.get("path", [parsed.path])[0]
        if route in {"/api/auth/github", "/api/auth/callback", "/api/auth/logout"}:
            return self.github_auth(parsed, route)
        if route == "/api/me":
            secret = os.getenv("SESSION_SECRET")
            raw_cookie = self.headers.get("Cookie", "")
            token = next((part.split("=",1)[1] for part in raw_cookie.split("; ") if part.startswith("aimosi_session=")), "")
            if not secret or "." not in token:
                return self.json_response(200, {"user": None})
            packed, signature = token.rsplit(".", 1)
            expected = hmac.new(secret.encode(), packed.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature, expected):
                return self.json_response(200, {"user": None})
            try:
                decoded = base64.urlsafe_b64decode(packed + "=" * (-len(packed) % 4))
                return self.json_response(200, {"user": json.loads(decoded)})
            except (ValueError, json.JSONDecodeError):
                return self.json_response(200, {"user": None})
        if route == "/health":
            return self.json_response(200, {"status": "ok", "time": core.now_iso()})
        if route == "/api" or route.startswith("/api/"):
            status, body = core.payload(route, query)
            return self.json_response(status, body)
        return self.json_response(404, {"error": "Not found"})

    def do_POST(self):
        core.init_db()
        parsed = urlparse(self.path)
        route = parse_qs(parsed.query).get("path", [parsed.path])[0]
        if route != "/api/sync":
            return self.json_response(404, {"error": "Not found"})
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2048:
            return self.json_response(413, {"error": "Request too large"})
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            return self.json_response(400, {"error": "Invalid JSON"})
        community = body.get("community", "kubernetes")
        names = body.get("connectors", ["github", "youtube"])
        if community not in core.COMMUNITY_CATALOG:
            return self.json_response(400, {"error": "Unknown community workspace"})
        if not isinstance(names, list) or not names or len(names) > 3 or any(x not in {"github", "kubernetes-community", "youtube"} for x in names):
            return self.json_response(400, {"error": "Invalid connector list"})
        if "kubernetes-community" in names and community != "kubernetes":
            return self.json_response(400, {"error": "Kubernetes metadata is only available in its workspace"})
        results = [core.sync_connector(f"{name}:{community}") for name in names]
        return self.json_response(200, {"tasks": results})
