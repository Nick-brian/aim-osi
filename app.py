"""AIM OSI API and static application server. Standard-library only."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
WEB = ROOT / "web"
DEMO = os.getenv("DEMO_MODE", "false" if os.getenv("VERCEL") else "true").lower() in {"1", "true", "yes"}
DB_PATH = Path(os.getenv("DATABASE_PATH", "/tmp/aim-osi.sqlite3" if os.getenv("VERCEL") else str(ROOT / "data" / "aimosi.sqlite3")))
PORT = int(os.getenv("PORT", "8000"))
LOCK = threading.Lock()
TASK_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="aimosi-sync")
SYNC_REQUESTS: dict[str, float] = {}

# A workspace is a public community plus its authoritative primary repository.
# Additional repositories can be added without changing the data model.
COMMUNITY_CATALOG = {
    "kubernetes": {"id":"kubernetes","name":"Kubernetes","description":"Container orchestration community","website":"https://kubernetes.io","repo":"kubernetes/kubernetes","repo_name":"Kubernetes","repo_description":"Production-grade container orchestration","source_url":"https://github.com/kubernetes/community","youtube_url":"https://www.youtube.com/kubernetescommunity","youtube_user":"kubernetescommunity","youtube_terms":["kubernetes","kubecon","k8s"],"icon":"K"},
    "prometheus": {"id":"prometheus","name":"Prometheus","description":"Monitoring and alerting toolkit","website":"https://prometheus.io","repo":"prometheus/prometheus","repo_name":"Prometheus","repo_description":"The Prometheus monitoring system and time series database","source_url":"https://github.com/prometheus/prometheus","youtube_url":"https://www.youtube.com/@cncf","youtube_channel":"UCvqbFHwN-nwalWPjPUKpvTA","youtube_terms":["prometheus","promcon"],"icon":"P"},
    "etcd": {"id":"etcd","name":"etcd","description":"Distributed reliable key-value store","website":"https://etcd.io","repo":"etcd-io/etcd","repo_name":"etcd","repo_description":"Distributed reliable key-value store for the most critical data of a distributed system","source_url":"https://github.com/etcd-io/etcd","youtube_url":"https://www.youtube.com/channel/UC7tUWR24I5AR9NMsG-NYBlg","youtube_channel":"UC7tUWR24I5AR9NMsG-NYBlg","youtube_terms":["etcd"],"icon":"e"},
    "containerd": {"id":"containerd","name":"containerd","description":"Industry-standard container runtime","website":"https://containerd.io","repo":"containerd/containerd","repo_name":"containerd","repo_description":"An industry-standard container runtime","source_url":"https://github.com/containerd/containerd","youtube_url":"https://www.youtube.com/@CNCFcontainerd","youtube_user":"CNCFcontainerd","youtube_terms":["containerd"],"icon":"c"},
    "argo-cd": {"id":"argo-cd","name":"Argo CD","description":"Declarative GitOps continuous delivery for Kubernetes","website":"https://argo-cd.readthedocs.io","repo":"argoproj/argo-cd","repo_name":"Argo CD","repo_description":"Declarative continuous delivery with GitOps","source_url":"https://github.com/argoproj/argo-cd","youtube_url":"https://www.youtube.com/@cncf","youtube_channel":"UCvqbFHwN-nwalWPjPUKpvTA","youtube_terms":["argo project","argo cd","argocd","argocon"],"icon":"A"},
    "opentelemetry": {"id":"opentelemetry","name":"OpenTelemetry","description":"Observability framework and ecosystem","website":"https://opentelemetry.io","repo":"open-telemetry/opentelemetry-collector","repo_name":"OpenTelemetry Collector","repo_description":"Vendor-agnostic way to receive, process and export telemetry data","source_url":"https://github.com/open-telemetry/opentelemetry-collector","youtube_url":"https://www.youtube.com/channel/UCHZDBZTIfdy94xMjMKz-_MA","youtube_channel":"UCHZDBZTIfdy94xMjMKz-_MA","youtube_terms":["opentelemetry","opentelemetry collector","otel"],"icon":"O"},
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@contextmanager
def db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db() -> None:
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS communities(id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT, website TEXT, source_url TEXT, retrieved_at TEXT, demo INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, community_id TEXT NOT NULL REFERENCES communities(id), name TEXT NOT NULL, description TEXT, repo_url TEXT, source_url TEXT, retrieved_at TEXT, demo INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS groups(id TEXT PRIMARY KEY, community_id TEXT NOT NULL REFERENCES communities(id), name TEXT NOT NULL, description TEXT, schedule TEXT, timezone TEXT, meeting_url TEXT, archive_url TEXT, calendar_url TEXT, source_url TEXT, retrieved_at TEXT, demo INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY, community_id TEXT, project_id TEXT, kind TEXT NOT NULL, title TEXT NOT NULL, summary TEXT, url TEXT, published_at TEXT, source TEXT, source_id TEXT, retrieved_at TEXT, demo INTEGER NOT NULL DEFAULT 0, last_updated_at TEXT);
        CREATE TABLE IF NOT EXISTS sync_runs(id TEXT PRIMARY KEY, connector TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, records INTEGER DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY, connector TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, completed_at TEXT, records INTEGER DEFAULT 0, error TEXT, retry_count INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_signals_kind ON signals(kind);
        """)
        task_columns = {row["name"] for row in c.execute("PRAGMA table_info(tasks)")}
        if "retry_count" not in task_columns:
            c.execute("ALTER TABLE tasks ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        signal_columns = {row["name"] for row in c.execute("PRAGMA table_info(signals)")}
        if "last_updated_at" not in signal_columns:
            c.execute("ALTER TABLE signals ADD COLUMN last_updated_at TEXT")
        c.execute("UPDATE tasks SET status='FAILED',completed_at=?,error='Interrupted by server restart' WHERE status IN ('QUEUED','RUNNING','RETRYING')", (now_iso(),))
        if DEMO and c.execute("SELECT count(*) FROM communities").fetchone()[0] == 0:
            seed_demo(c)


def seed_demo(c: sqlite3.Connection) -> None:
    """Clearly tagged demo fixtures; dates are intentionally relative to preview runtime."""
    stamp = now_iso()
    c.execute("INSERT INTO communities VALUES(?,?,?,?,?,?,1)", ("kubernetes", "Kubernetes", "Container orchestration community intelligence", "https://kubernetes.io", "https://github.com/kubernetes/community", stamp))
    for p in [("kubernetes", "Kubernetes", "Production-grade container orchestration", "https://github.com/kubernetes/kubernetes"), ("etcd", "etcd", "Reliable distributed key-value store", "https://github.com/etcd-io/etcd"), ("containerd", "containerd", "Industry-standard container runtime", "https://github.com/containerd/containerd")]:
        c.execute("INSERT INTO projects VALUES(?,?,?,?,?,?,?,1)", (p[0], "kubernetes", p[1], p[2], p[3], p[3], stamp))
    groups = [
        ("sig-node", "SIG Node", "Node components, runtime, and resource management", "Weekly; see the official calendar", "America/Los_Angeles", "https://zoom.us/", "https://github.com/kubernetes/community/tree/master/sig-node", "https://calendar.google.com/calendar/u/0/r?cid=calendar%40kubernetes.io"),
        ("sig-docs", "SIG Docs", "Kubernetes documentation and contributor experience", "Weekly; see the official calendar", "America/Los_Angeles", "https://zoom.us/", "https://github.com/kubernetes/community/tree/master/sig-docs", "https://calendar.google.com/calendar/u/0/r?cid=calendar%40kubernetes.io"),
        ("sig-network", "SIG Network", "Networking APIs and implementations", "Biweekly; see the official calendar", "America/Los_Angeles", "https://zoom.us/", "https://github.com/kubernetes/community/tree/master/sig-network", "https://calendar.google.com/calendar/u/0/r?cid=calendar%40kubernetes.io"),
    ]
    for g in groups:
        c.execute("INSERT INTO groups VALUES(?,?,?,?,?,?,?,?,?,?,?,1)", (g[0], "kubernetes", g[1], g[2], g[3], g[4], g[5], g[6], g[7], "https://github.com/kubernetes/community/blob/master/sigs.yaml", stamp))
    fixtures = [
        ("pr", "Improve pod startup observability", "Illustrative preview signal. Live status and details require the GitHub connector.", "https://github.com/kubernetes/kubernetes/pulls", "kubernetes/kubernetes", "https://api.github.com/repos/kubernetes/kubernetes/pulls"),
        ("release", "Kubernetes release activity", "Review the official release page for current versions and notes.", "https://github.com/kubernetes/kubernetes/releases", "kubernetes/kubernetes", "https://api.github.com/repos/kubernetes/kubernetes/releases"),
        ("issue", "good first issue: improve documentation guidance", "Illustrative preview signal; issue labels and difficulty must be verified at the source.", "https://github.com/kubernetes/kubernetes/labels/good%20first%20issue", "kubernetes/kubernetes", "https://api.github.com/repos/kubernetes/kubernetes/issues?labels=good%20first%20issue"),
        ("meeting", "SIG Node meeting resources", "Schedule, notes, and recordings are provided by the official SIG archive.", "https://github.com/kubernetes/community/tree/master/sig-node", "sig-node", "https://github.com/kubernetes/community/blob/master/sigs.yaml"),
        ("event", "Kubernetes community events", "See official community event listings for current dates and registration.", "https://www.kubernetes.dev/events/", "kubernetes-events", "https://www.kubernetes.dev/events/"),
        ("video", "Kubernetes community meeting recordings", "Recordings are linked from SIG archives and official playlists.", "https://www.youtube.com/@KubernetesCommunity", "kubernetes-community", "https://www.youtube.com/@KubernetesCommunity"),
        ("docs", "Kubernetes contributor documentation", "Community guidance and project documentation.", "https://www.kubernetes.dev/docs/", "kubernetes-docs", "https://www.kubernetes.dev/docs/"),
    ]
    for i, (kind, title, summary, url, sid, source) in enumerate(fixtures):
        c.execute("INSERT INTO signals(id,community_id,project_id,kind,title,summary,url,published_at,source,source_id,retrieved_at,demo) VALUES(?,?,?,?,?,?,?,?,?,?,?,1)", (f"demo-{i}", "kubernetes", "kubernetes", kind, title, summary, url, stamp, source, sid, stamp))


def rows(sql: str, params: tuple = ()) -> list[dict]:
    with db() as c:
        return [dict(x) for x in c.execute(sql, params).fetchall()]


def github_get(url: str) -> object:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "AIM-OSI/0.1"}
    if os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
    req = Request(url, headers=headers)
    with urlopen(req, timeout=12) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_youtube_feed(raw: bytes | str, community_id: str, cutoff: datetime | None = None) -> list[dict]:
    """Parse YouTube's public Atom feed and keep only videos published in the last week."""
    cutoff = cutoff or (datetime.now(timezone.utc) - timedelta(days=7))
    root = ET.fromstring(raw)
    atom = "{http://www.w3.org/2005/Atom}"
    yt = "{http://www.youtube.com/xml/schemas/2015}"
    videos = []
    for entry in root.findall(f"{atom}entry"):
        video_id = entry.findtext(f"{yt}videoId")
        published = entry.findtext(f"{atom}published")
        if not video_id or not published:
            continue
        try:
            published_at = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except ValueError:
            continue
        if published_at < cutoff:
            continue
        videos.append({
            "id": f"youtube-{community_id}-{video_id}", "community_id": community_id,
            "kind": "video", "title": entry.findtext(f"{atom}title") or "YouTube video",
            "summary": entry.findtext("{http://search.yahoo.com/mrss/}group/{http://search.yahoo.com/mrss/}description") or "",
            "url": f"https://www.youtube.com/watch?v={video_id}", "published_at": published,
            "source": f"https://www.youtube.com/watch?v={video_id}", "source_id": video_id,
        })
    return videos


def youtube_feed_url(workspace: dict) -> str:
    if workspace.get("youtube_channel"):
        return "https://www.youtube.com/feeds/videos.xml?channel_id=" + workspace["youtube_channel"]
    channel_page = workspace.get("youtube_url")
    if channel_page:
        req = Request(channel_page, headers={"User-Agent": "Mozilla/5.0 (compatible; AIM-OSI/0.1)"})
        with urlopen(req, timeout=12) as response:
            html = response.read(2_000_000).decode("utf-8", "replace")
        match = re.search(r'"externalId"\s*:\s*"(UC[\w-]{20,})"', html) or re.search(r'<meta\s+itemprop="channelId"\s+content="(UC[\w-]{20,})"', html)
        if match:
            return "https://www.youtube.com/feeds/videos.xml?channel_id=" + match.group(1)
    if workspace.get("youtube_user"):
        return "https://www.youtube.com/feeds/videos.xml?user=" + workspace["youtube_user"]
    raise RuntimeError("No official YouTube feed is configured for this workspace")


def parse_kubernetes_sigs(raw: str) -> list[dict]:
    """Parse only top-level SIG entries and their meeting blocks from official sigs.yaml."""
    found: list[dict] = []
    current: dict | None = None
    meeting: dict | None = None
    in_meetings = False
    for line in raw.splitlines():
        directory = re.match(r"^  - dir:\s*([^\s#]+)", line)
        if directory:
            current = {"id": directory.group(1), "meetings": []}
            found.append(current)
            meeting = None
            in_meetings = False
            continue
        if not current:
            continue
        if re.match(r"^    meetings:\s*$", line):
            in_meetings = True
            meeting = None
            continue
        if re.match(r"^    [a-zA-Z_]+:", line) and not line.startswith("    meetings:"):
            in_meetings = False
            meeting = None
        name = re.match(r"^    name:\s*(.*?)\s*$", line)
        if name:
            current["name"] = name.group(1).strip(" '\"")
            continue
        if re.match(r"^      - description:", line) and in_meetings:
            meeting = {}
            current["meetings"].append(meeting)
            match = re.match(r"^      - description:\s*(.*?)\s*$", line)
            if match:
                meeting["description"] = match.group(1).strip(" '\"")
            continue
        if in_meetings and meeting:
            field = re.match(r"^        ([a-z_]+):\s*(.*?)\s*$", line)
            if field:
                meeting[field.group(1)] = field.group(2).strip(" '\"")
    result = []
    for entry in found:
        if not entry.get("name"):
            continue
        meetings = entry.get("meetings", [])
        schedule_parts = []
        for m in meetings:
            when = " ".join(x for x in [m.get("day"), m.get("time"), m.get("tz"), m.get("frequency")] if x)
            schedule_parts.append(f"{m.get('description','Meeting')}: {when}" if when else m.get("description", "Meeting"))
        first = meetings[0] if meetings else {}
        join = next((m for m in meetings if m.get("url")), first)
        archive = next((m for m in meetings if m.get("archive_url")), first)
        calendar = next((m for m in meetings if m.get("calendar_url")), first)
        prefix = "WG" if entry["id"].lower().startswith(("wg-", "working-group-")) else "SIG"
        display_name = entry["name"] if entry["name"].startswith(("SIG ", "WG ")) else f"{prefix} {entry['name']}"
        result.append({
            "id": f"kubernetes-{entry['id']}",
            "name": display_name,
            "description": f"Kubernetes SIG. Official meeting details are linked from the community source.",
            "schedule": "; ".join(schedule_parts) if schedule_parts else "Schedule not provided",
            "timezone": first.get("tz", "Not specified"),
            "meeting_url": join.get("url"),
            "archive_url": archive.get("archive_url", "https://github.com/kubernetes/community/tree/master"),
            "calendar_url": calendar.get("calendar_url"),
        })
    return result


def sync_connector(name: str) -> dict:
    """Connector refreshes are best-effort and each records its own failure."""
    connector, _, community_id = name.partition(":")
    community_id = community_id or "kubernetes"
    workspace = COMMUNITY_CATALOG.get(community_id)
    run_id, started = str(uuid.uuid4()), now_iso()
    with db() as c:
        c.execute("INSERT INTO sync_runs(id,connector,status,started_at) VALUES(?,?,?,?)", (run_id, name, "RUNNING", started))
    count, error, status = 0, None, "SUCCESS"
    try:
        if not workspace:
            raise ValueError("Unknown community workspace")
        if connector == "kubernetes-community" and community_id == "kubernetes":
            url = "https://raw.githubusercontent.com/kubernetes/community/master/sigs.yaml"
            req = Request(url, headers={"User-Agent": "AIM-OSI/0.1"})
            with urlopen(req, timeout=15) as resp:
                raw = resp.read(5_000_000).decode("utf-8", "replace")
            sigs = parse_kubernetes_sigs(raw)
            stamp = now_iso()
            with db() as c:
                c.execute("INSERT INTO communities(id,name,description,website,source_url,retrieved_at,demo) VALUES(?,?,?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET name=excluded.name,description=excluded.description,website=excluded.website,source_url=excluded.source_url,retrieved_at=excluded.retrieved_at,demo=0", (community_id, workspace["name"], workspace["description"], workspace["website"], url, stamp))
                c.execute("DELETE FROM groups WHERE community_id='kubernetes' AND demo=0 AND source_url=?", (url,))
                for sig in sigs:
                    c.execute("INSERT INTO groups(id,community_id,name,description,schedule,timezone,meeting_url,archive_url,calendar_url,source_url,retrieved_at,demo) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)", (sig["id"], community_id, sig["name"], sig["description"], sig["schedule"], sig["timezone"], sig["meeting_url"], sig["archive_url"], sig["calendar_url"], url, stamp))
                    count += 1
        elif connector == "github":
            repo = workspace["repo"]
            repo_api = f"https://api.github.com/repos/{repo}"
            endpoints = [
                ("pr", f"{repo_api}/pulls?state=all&per_page=20&sort=updated&direction=desc"),
                ("issue", f"{repo_api}/issues?state=open&labels=good%20first%20issue&per_page=20&sort=updated&direction=desc"),
                ("release", f"{repo_api}/releases?per_page=10"),
            ]
            stamp = now_iso()
            with db() as c:
                c.execute("INSERT INTO communities(id,name,description,website,source_url,retrieved_at,demo) VALUES(?,?,?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET name=excluded.name,description=excluded.description,website=excluded.website,source_url=excluded.source_url,retrieved_at=excluded.retrieved_at,demo=0", (community_id, workspace["name"], workspace["description"], workspace["website"], workspace["source_url"], stamp))
                c.execute("INSERT INTO projects(id,community_id,name,description,repo_url,source_url,retrieved_at,demo) VALUES(?,?,?,?,?,?,?,0) ON CONFLICT(id) DO UPDATE SET name=excluded.name,description=excluded.description,repo_url=excluded.repo_url,source_url=excluded.source_url,retrieved_at=excluded.retrieved_at,demo=0", (community_id, community_id, workspace["repo_name"], workspace["repo_description"], f"https://github.com/{repo}", repo_api, stamp))
                for kind, url in endpoints:
                    for item in github_get(url):
                        sid = str(item["id"])
                        if kind == "release":
                            title = item.get("name") or item.get("tag_name") or "Release"
                            summary = f"{item.get('tag_name','version unavailable')} · published {item.get('published_at') or 'date unavailable'}"
                        else:
                            title = item.get("title", "Untitled issue")
                            labels = ", ".join(x.get("name", "") for x in item.get("labels", []) if x.get("name"))
                            summary = f"#{item.get('number')} · {item.get('state','unknown')} · updated {item.get('updated_at','date unavailable')}" + (f" · labels: {labels}" if labels else "")
                        actual_kind = "pr" if kind == "issue" and item.get("pull_request") else kind
                        c.execute("INSERT INTO signals(id,community_id,project_id,kind,title,summary,url,published_at,source,source_id,retrieved_at,demo,last_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?) ON CONFLICT(id) DO UPDATE SET community_id=excluded.community_id,project_id=excluded.project_id,title=excluded.title,summary=excluded.summary,url=excluded.url,published_at=excluded.published_at,source=excluded.source,retrieved_at=excluded.retrieved_at,last_updated_at=excluded.last_updated_at", (f"github-{sid}", community_id, community_id, actual_kind, title, summary, item.get("html_url"), item.get("published_at") or item.get("created_at"), url, sid, stamp, item.get("updated_at") or item.get("published_at")))
                        count += 1
        elif connector == "youtube":
            url = youtube_feed_url(workspace)
            req = Request(url, headers={"User-Agent": "AIM-OSI/0.1"})
            with urlopen(req, timeout=12) as resp:
                data = resp.read(1_000_000)
            stamp = now_iso()
            videos = parse_youtube_feed(data, community_id)
            # Shared CNCF channel feeds contain many projects; retain only videos explicitly
            # about the selected project to avoid presenting unrelated community content.
            if workspace.get("youtube_channel") == "UCvqbFHwN-nwalWPjPUKpvTA":
                terms = workspace.get("youtube_terms", [])
                videos = [v for v in videos if any(term.casefold() in v["title"].casefold() for term in terms)]
            with db() as c:
                for item in videos:
                    c.execute("INSERT INTO signals(id,community_id,project_id,kind,title,summary,url,published_at,source,source_id,retrieved_at,demo,last_updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?) ON CONFLICT(id) DO UPDATE SET title=excluded.title,summary=excluded.summary,url=excluded.url,published_at=excluded.published_at,source=excluded.source,retrieved_at=excluded.retrieved_at,last_updated_at=excluded.last_updated_at", (item["id"], community_id, None, "video", item["title"], item["summary"], item["url"], item["published_at"], url, item["source_id"], stamp, item["published_at"]))
                    count += 1
        else:
            raise ValueError("Unknown connector")
    except Exception as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {str(exc)[:240]}"
    finished = now_iso()
    with db() as c:
        c.execute("UPDATE sync_runs SET status=?,finished_at=?,records=?,error=? WHERE id=?", (status, finished, count, error, run_id))
    return {"id": run_id, "connector": name, "status": status, "records": count, "error": error, "started_at": started, "finished_at": finished}


def run_task(task_id: str, connector: str) -> None:
    with db() as c:
        c.execute("UPDATE tasks SET status='RUNNING',started_at=? WHERE id=? AND status='QUEUED'", (now_iso(), task_id))
    result = None
    for attempt in range(3):
        result = sync_connector(connector)
        if result["status"] == "SUCCESS" or attempt == 2:
            break
        with db() as c:
            c.execute("UPDATE tasks SET status='RETRYING',retry_count=? WHERE id=?", (attempt + 1, task_id))
        time.sleep(0.5 * (attempt + 1))
    with db() as c:
        c.execute("UPDATE tasks SET status=?,completed_at=?,records=?,error=?,retry_count=? WHERE id=?", (result["status"], result["finished_at"], result["records"], result["error"], max(0, attempt), task_id))


def queue_sync(connectors: list[str], community_id: str = "kubernetes") -> list[dict]:
    queued = []
    with db() as c:
        for connector in connectors:
            task_connector = f"{connector}:{community_id}"
            task_id = str(uuid.uuid4())
            created = now_iso()
            c.execute("INSERT INTO tasks(id,connector,status,created_at) VALUES(?,?,?,?)", (task_id, task_connector, "QUEUED", created))
            queued.append({"id": task_id, "connector": task_connector, "status": "QUEUED", "created_at": created})
    for task in queued:
        TASK_POOL.submit(run_task, task["id"], task["connector"])
    return queued


def integration_status(community_id: str = "kubernetes") -> list[dict]:
    latest = {r["connector"]: r for r in rows("SELECT s.* FROM sync_runs s JOIN (SELECT connector,MAX(started_at) t FROM sync_runs GROUP BY connector) x ON s.connector=x.connector AND s.started_at=x.t")}
    workspace = COMMUNITY_CATALOG[community_id]
    configs = [
        (f"github:{community_id}", "GitHub", "configured" if os.getenv("GITHUB_TOKEN") else "public", "https://api.github.com"),
        ("kubernetes-community:kubernetes" if community_id == "kubernetes" else "kubernetes-community:not-applicable", "Kubernetes metadata" if community_id == "kubernetes" else "Community metadata", "ready" if community_id == "kubernetes" else "not configured", "https://github.com/kubernetes/community/blob/master/sigs.yaml" if community_id == "kubernetes" else workspace["source_url"]),
        (f"youtube:{community_id}", "YouTube", "official channel feed" if workspace.get("youtube_channel") or workspace.get("youtube_user") else "not configured", workspace.get("youtube_url")),
        (f"calendar:{community_id}", "Public calendars", "links only" if community_id == "kubernetes" else "not configured", "https://github.com/kubernetes/community" if community_id == "kubernetes" else None),
        ("rss", "RSS / feeds", "not configured", None),
        ("summaries", "AI summaries", "factual fallback", None),
    ]
    if community_id == "kubernetes":
        latest.setdefault("github:kubernetes", latest.get("github"))
        latest.setdefault("kubernetes-community:kubernetes", latest.get("kubernetes-community"))
    out=[]
    for key, label, config, source in configs:
        r=latest.get(key)
        state = "healthy" if r and r["status"] == "SUCCESS" else ("error" if r and r["status"] == "FAILED" else "idle")
        run_names = [key]
        if community_id == "kubernetes" and key == "github:kubernetes": run_names.append("github")
        if community_id == "kubernetes" and key == "kubernetes-community:kubernetes": run_names.append("kubernetes-community")
        errors = sum(x["status"] == "FAILED" for x in rows(f"SELECT status FROM sync_runs WHERE connector IN ({','.join('?' for _ in run_names)})", tuple(run_names)))
        out.append({"id":key,"name":label,"status":state,"configuration":config,"last_success":r["finished_at"] if r and r["status"]=="SUCCESS" else None,"last_run":r["finished_at"] if r else None,"error_count":errors,"source_url":source,"demo_mode":DEMO})
    return out


def payload(path: str, query: dict[str, list[str]]) -> tuple[int, object]:
    if path == "/api":
        return 200, {"name":"AIM OSI API","version":"0.1.0","mode":"demo" if DEMO else "live","endpoints":["/api/workspaces","/api/overview?community=<id>","/api/communities","/api/projects","/api/activity","/api/meetings","/api/events","/api/issues","/api/digest","/api/automation","/api/integrations"]}
    if path == "/api/workspaces":
        return 200, list(COMMUNITY_CATALOG.values())
    community_id = (query.get("community") or ["kubernetes"])[0]
    workspace = COMMUNITY_CATALOG.get(community_id)
    if not workspace:
        return 400, {"error":"Unknown community workspace"}
    visible = (community_id, 1 if DEMO else 0)
    if path == "/api/overview":
        communities=rows("SELECT * FROM communities WHERE id=? AND (?=1 OR demo=0) ORDER BY name",visible)
        projects=rows("SELECT * FROM projects WHERE community_id=? AND (?=1 OR demo=0) ORDER BY name",visible)
        if not communities:
            communities=[{"id":workspace["id"],"name":workspace["name"],"description":workspace["description"],"website":workspace["website"],"source_url":workspace["source_url"],"retrieved_at":None,"demo":0,"configured":True}]
        if not projects:
            projects=[{"id":workspace["id"],"community_id":community_id,"name":workspace["repo_name"],"description":workspace["repo_description"],"repo_url":f"https://github.com/{workspace['repo']}","source_url":workspace["source_url"],"retrieved_at":None,"demo":0,"configured":True}]
        return 200, {"mode":"demo" if DEMO else "live","workspace":workspace,"communities":communities,"projects":projects,"meetings":rows("SELECT * FROM groups WHERE community_id=? AND (?=1 OR demo=0) ORDER BY name",visible),"signals":rows("SELECT * FROM signals WHERE community_id=? AND (?=1 OR demo=0) ORDER BY COALESCE(last_updated_at,published_at) DESC LIMIT 100",visible),"weekly_signals":rows("SELECT * FROM signals WHERE community_id=? AND (?=1 OR demo=0) AND COALESCE(last_updated_at,published_at)>=? ORDER BY COALESCE(last_updated_at,published_at) DESC LIMIT 40",visible+((datetime.now(timezone.utc)-timedelta(days=7)).isoformat(timespec="seconds"),)),"integrations":integration_status(community_id),"updated_at":now_iso()}
    if path == "/api/communities": return 200, rows("SELECT * FROM communities WHERE id=? AND (?=1 OR demo=0) ORDER BY name",visible)
    if path == "/api/projects": return 200, rows("SELECT * FROM projects WHERE community_id=? AND (?=1 OR demo=0) ORDER BY name",visible)
    if path == "/api/activity": return 200, rows("SELECT * FROM signals WHERE community_id=? AND (?=1 OR demo=0) ORDER BY published_at DESC LIMIT 100",visible)
    if path == "/api/issues":
        # Filter labels using only source-returned label metadata/title text.
        result = rows("SELECT * FROM signals WHERE community_id=? AND kind='issue' AND (?=1 OR demo=0) ORDER BY COALESCE(last_updated_at,published_at) DESC LIMIT 100",visible)
        label = (query.get("label") or [None])[0]
        if label:
            result = [item for item in result if label.casefold() in (item.get("title", "") + " " + item.get("summary", "")).casefold()]
        return 200, result
    if path == "/api/meetings": return 200, rows("SELECT * FROM groups WHERE community_id=? AND (?=1 OR demo=0) ORDER BY name",visible)
    if path == "/api/events": return 200, rows("SELECT * FROM signals WHERE community_id=? AND kind='event' AND (?=1 OR demo=0) ORDER BY published_at DESC LIMIT 100",visible)
    if path == "/api/digest":
        since=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat(timespec="seconds")
        signals=rows("SELECT * FROM signals WHERE community_id=? AND (?=1 OR demo=0) AND COALESCE(last_updated_at,published_at)>=? ORDER BY COALESCE(last_updated_at,published_at) DESC LIMIT 100",visible+(since,))
        return 200,{"title":f"{workspace['name']} weekly digest","generated_at":now_iso(),"period_start":since,"mode":"demo" if DEMO else "live","summary":f"Factual digest of {workspace['name']} source records published or updated in the last seven days. Interpretation is intentionally not generated without a configured summarization provider.","sections":[{"title":title,"items":[x for x in signals if x["kind"] in kinds]} for title,kinds in [("Project changes",["pr","release"]),("Meetings and recordings",["meeting","video"]),("Issues and contribution opportunities",["issue"]),("Events and documentation",["event","docs"])]],"sources":[{"url":x["source"],"retrieved_at":x["retrieved_at"]} for x in signals if x.get("source")]}
    if path == "/api/automation":
        runs=rows("SELECT * FROM sync_runs WHERE connector LIKE ? ORDER BY started_at DESC LIMIT 30",(f"%:{community_id}",))
        queue=rows("SELECT * FROM tasks WHERE connector LIKE ? ORDER BY created_at DESC LIMIT 30",(f"%:{community_id}",))
        return 200,{"integrations":integration_status(community_id),"runs":runs,"queue":queue}
    if path == "/api/integrations": return 200,integration_status(community_id)
    return 404,{"error":"Not found"}


class Handler(BaseHTTPRequestHandler):
    server_version = "AIMOSI/0.1"
    def log_message(self, fmt, *args):
        # Avoid query strings (which can carry credentials) in standard logs.
        print(f"{self.client_address[0]} {self.command} {urlparse(self.path).path} {args[1] if len(args)>1 else ''}")
    def send_json(self, code: int, value: object):
        data=json.dumps(value,ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(data)
    def add_security_headers(self, mime: str, length: int):
        self.send_header("Content-Type",mime); self.send_header("Content-Length",str(length)); self.send_header("X-Content-Type-Options","nosniff"); self.send_header("X-Frame-Options","DENY"); self.send_header("Referrer-Policy","strict-origin-when-cross-origin"); self.send_header("Content-Security-Policy","default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' https: data:; connect-src 'self'; frame-src https://www.youtube-nocookie.com https://www.youtube.com; frame-ancestors 'none'; base-uri 'self'; form-action 'self' https://github.com")
    def do_GET(self):
        parsed=urlparse(self.path)
        if parsed.path == "/health": return self.send_json(200,{"status":"ok","time":now_iso()})
        if parsed.path == "/api" or parsed.path.startswith("/api/"):
            code,result=payload(parsed.path,parse_qs(parsed.query)); return self.send_json(code,result)
        target=(WEB / ("index.html" if parsed.path=="/" else parsed.path.lstrip("/"))).resolve()
        if not target.is_relative_to(WEB.resolve()) or not target.is_file(): return self.send_json(404,{"error":"Not found"})
        body=target.read_bytes(); mime="text/html; charset=utf-8" if target.suffix==".html" else "text/css; charset=utf-8" if target.suffix==".css" else "text/javascript; charset=utf-8" if target.suffix==".js" else "application/octet-stream"
        self.send_response(200); self.add_security_headers(mime,len(body)); self.end_headers(); self.wfile.write(body)
    def do_POST(self):
        if urlparse(self.path).path != "/api/sync": return self.send_json(404,{"error":"Not found"})
        length=int(self.headers.get("Content-Length","0"))
        if length > 2048: return self.send_json(413,{"error":"Request too large"})
        try: body=json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError: return self.send_json(400,{"error":"Invalid JSON"})
        names=body.get("connectors",["kubernetes-community","github"])
        community_id=body.get("community","kubernetes")
        if community_id not in COMMUNITY_CATALOG: return self.send_json(400,{"error":"Unknown community workspace"})
        if not isinstance(names,list) or len(names)>4 or any(x not in {"github","kubernetes-community","youtube"} for x in names): return self.send_json(400,{"error":"Invalid connector list"})
        if "kubernetes-community" in names and community_id != "kubernetes": return self.send_json(400,{"error":"Kubernetes metadata is only available in its workspace"})
        client = self.client_address[0]
        with LOCK:
            now = time.monotonic()
            if now - SYNC_REQUESTS.get(client, 0) < 10:
                return self.send_json(429,{"error":"Please wait before starting another sync"})
            with db() as c:
                active = c.execute("SELECT count(*) FROM tasks WHERE status IN ('QUEUED','RUNNING','RETRYING')").fetchone()[0]
            if active + len(names) > 20:
                return self.send_json(429,{"error":"Sync task queue is full"})
            SYNC_REQUESTS[client] = now
        result=queue_sync(names,community_id)
        return self.send_json(202,{"tasks":result})


def main():
    init_db()
    server=ThreadingHTTPServer(("0.0.0.0",PORT),Handler)
    print(f"AIM OSI listening on http://localhost:{PORT} (mode={'demo' if DEMO else 'live'})")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()


if __name__ == "__main__": main()
