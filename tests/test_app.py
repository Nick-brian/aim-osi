import json
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch
from urllib.error import URLError

import app


class AppApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        app.DB_PATH = Path(cls.temp.name) / "test.sqlite3"
        app.DEMO = True
        app.init_db()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        cls.temp.cleanup()

    def get(self, path):
        with urlopen(self.base + path, timeout=3) as response:
            return response.status, response.headers, response.read()

    def test_health_and_api_manifest(self):
        status, _, body = self.get("/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["status"], "ok")
        manifest = json.loads(self.get("/api")[2])
        self.assertEqual(manifest["mode"], "demo")
        self.assertIn("/api/digest", manifest["endpoints"])

    def test_workspace_catalog_and_overview_are_selectable_and_scoped(self):
        catalog = json.loads(self.get("/api/workspaces")[2])
        self.assertEqual({item["id"] for item in catalog}, set(app.COMMUNITY_CATALOG))
        status, _, body = self.get("/api/overview?community=prometheus")
        self.assertEqual(status, 200)
        overview = json.loads(body)
        self.assertEqual(overview["workspace"]["name"], "Prometheus")
        self.assertEqual(overview["projects"][0]["repo_url"], "https://github.com/prometheus/prometheus")
        self.assertFalse(any(item["community_id"] == "kubernetes" for item in overview["signals"]))
        self.assertEqual(app.payload("/api/overview", {"community":["unknown"]})[0], 400)

    def test_github_adapter_uses_selected_workspace_repository(self):
        urls = []
        def fixture(url):
            urls.append(url)
            return []
        with patch.object(app, "github_get", side_effect=fixture):
            result = app.sync_connector("github:prometheus")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertTrue(all("/repos/prometheus/prometheus/" in url for url in urls))
        project = app.rows("SELECT * FROM projects WHERE id='prometheus'")[0]
        self.assertEqual(project["community_id"], "prometheus")

    def test_kubernetes_parser_scopes_names_and_meetings_to_sig_entries(self):
        raw = '''sigs:\n  - dir: sig-example\n    name: Example SIG\n    meetings:\n      - description: Weekly triage\n        day: Friday\n        time: "10:00"\n        tz: PT\n        frequency: weekly\n        url: https://meet.example.org\n        archive_url: https://notes.example.org\n    teams:\n      - name: nested project label\n'''
        records = app.parse_kubernetes_sigs(raw)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "SIG Example SIG")
        self.assertEqual(records[0]["id"], "kubernetes-sig-example")
        self.assertIn("Friday 10:00 PT weekly", records[0]["schedule"])
        self.assertEqual(records[0]["meeting_url"], "https://meet.example.org")
        self.assertEqual(records[0]["archive_url"], "https://notes.example.org")

    def test_youtube_feed_keeps_only_recent_valid_entries(self):
        feed = '''<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015"><entry><yt:videoId>abcdefghijk</yt:videoId><title>Recent official recording</title><published>2026-10-02T10:00:00+00:00</published></entry><entry><yt:videoId>oldabcdefgh</yt:videoId><title>Old recording</title><published>2026-09-20T10:00:00+00:00</published></entry><entry><title>Missing id</title><published>2026-10-02T10:00:00+00:00</published></entry></feed>'''
        parsed = app.parse_youtube_feed(feed, "kubernetes", app.datetime(2026, 10, 1, tzinfo=app.timezone.utc))
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["source_id"], "abcdefghijk")
        self.assertEqual(parsed[0]["url"], "https://www.youtube.com/watch?v=abcdefghijk")

    def test_vercel_api_rewrite_targets_python_function_route(self):
        config = json.loads((Path(app.ROOT) / "vercel.json").read_text())
        rewrites = {rule["source"]: rule["destination"] for rule in config["rewrites"]}
        self.assertEqual(rewrites["/api/:path*"], "/api?route=/api/:path*")
        self.assertEqual(rewrites["/health"], "/api?route=/health")

    def test_github_adapter_normalizes_sources_and_is_idempotent(self):
        def fixture(url):
            if "/pulls?" in url:
                return [{"id":101,"title":"A pull request","html_url":"https://github.com/kubernetes/kubernetes/pull/1","created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-02T00:00:00Z","state":"open"}]
            if "/issues?" in url:
                return [{"id":102,"number":2,"title":"A labeled issue","html_url":"https://github.com/kubernetes/kubernetes/issues/2","created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-02T00:00:00Z","state":"open","labels":[{"name":"good first issue"}]}]
            return [{"id":103,"name":"v1.0","tag_name":"v1.0","html_url":"https://github.com/kubernetes/kubernetes/releases/tag/v1.0","published_at":"2026-10-02T00:00:00Z","updated_at":"2026-10-02T00:00:00Z"}]
        with patch.object(app, "github_get", side_effect=fixture):
            first = app.sync_connector("github")
            second = app.sync_connector("github")
        self.assertEqual(first["status"], "SUCCESS")
        self.assertEqual(first["records"], 3)
        self.assertEqual(second["records"], 3)
        actual = app.rows("SELECT * FROM signals WHERE source_id IN ('101','102','103')")
        self.assertEqual(len(actual), 3)
        self.assertTrue(all(x["source"].startswith("https://api.github.com/") for x in actual))
        self.assertTrue(all(x["last_updated_at"] for x in actual))
        _, opportunities = app.payload("/api/issues", {"label":["good first issue"]})
        self.assertIn("github-102", {item["id"] for item in opportunities})

    def test_overview_includes_labeled_demo_provenance(self):
        data = json.loads(self.get("/api/overview")[2])
        self.assertEqual(data["mode"], "demo")
        self.assertTrue(data["communities"])
        self.assertTrue(any(row["demo"] == 1 for row in data["signals"]))
        self.assertTrue(all(row["source"] for row in data["signals"]))
        self.assertTrue(data["meetings"])

    def test_demo_mode_can_show_new_live_records_with_individual_provenance(self):
        with app.db() as connection:
            connection.execute("INSERT INTO signals(id,community_id,project_id,kind,title,summary,url,published_at,source,source_id,retrieved_at,demo,last_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?)", ("live-test", "kubernetes", "kubernetes", "pr", "Verified live fixture", "from test", "https://example.org/live", "2026-10-02T00:00:00+00:00", "test source", "live-1", "2026-10-02T00:00:00+00:00", "2026-10-02T00:00:00+00:00"))
        data = json.loads(self.get("/api/overview")[2])
        signal = next(x for x in data["signals"] if x["id"] == "live-test")
        self.assertEqual(data["mode"], "demo")
        self.assertEqual(signal["demo"], 0)

    def test_digest_has_sections_and_source_links(self):
        data = json.loads(self.get("/api/digest")[2])
        self.assertGreaterEqual(len(data["sections"]), 4)
        self.assertIn("factual", data["summary"].lower())
        self.assertTrue(all(item["url"] for section in data["sections"] for item in section["items"]))

    def test_static_app_has_security_headers(self):
        status, headers, body = self.get("/")
        self.assertEqual(status, 200)
        self.assertIn(b"AIM OSI", body)
        self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
        self.assertIn("default-src 'self'", headers.get("Content-Security-Policy"))

    def test_unknown_api_route_is_json_404(self):
        with self.assertRaises(HTTPError) as caught:
            self.get("/api/not-a-route")
        self.assertEqual(caught.exception.code, 404)
        caught.exception.close()

    def test_sync_rejects_unknown_connector_without_network_call(self):
        request = Request(self.base + "/api/sync", data=b'{"connectors":["untrusted"]}', headers={"Content-Type":"application/json"}, method="POST")
        with self.assertRaises(HTTPError) as caught:
            urlopen(request, timeout=3)
        self.assertEqual(caught.exception.code, 400)
        caught.exception.close()

    def test_sync_rejects_malformed_json(self):
        request = Request(self.base + "/api/sync", data=b'{oops', headers={"Content-Type":"application/json"}, method="POST")
        with self.assertRaises(HTTPError) as caught:
            urlopen(request, timeout=3)
        self.assertEqual(caught.exception.code, 400)
        caught.exception.close()

    def test_weekly_digest_omits_stale_signals(self):
        with app.db() as connection:
            connection.execute("INSERT INTO signals(id,kind,title,url,published_at,retrieved_at,demo,last_updated_at) VALUES(?,?,?,?,?,?,0,?)", ("stale-test", "pr", "Old record", "https://example.org/old", "2000-01-01T00:00:00+00:00", "2026-10-02T00:00:00+00:00", "2000-01-01T00:00:00+00:00"))
        data = json.loads(self.get("/api/digest")[2])
        self.assertNotIn("stale-test", {item["id"] for section in data["sections"] for item in section["items"]})

    def test_background_queue_records_connector_failure(self):
        with patch.object(app, "github_get", side_effect=URLError("offline in test")):
            task = app.queue_sync(["github"])[0]
            deadline = time.time() + 3
            current = None
            while time.time() < deadline:
                current = app.rows("SELECT * FROM tasks WHERE id=?", (task["id"],))[0]
                if current["status"] in {"FAILED", "SUCCESS"}:
                    break
                time.sleep(0.02)
        self.assertEqual(current["status"], "FAILED")
        self.assertIn("offline in test", current["error"])
        self.assertTrue(app.rows("SELECT * FROM sync_runs WHERE connector='github:kubernetes' AND status='FAILED'"))


if __name__ == "__main__":
    unittest.main()
