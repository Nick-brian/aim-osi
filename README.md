# AIM OSI

Open Source Community Intelligence — an extensible Kubernetes-first MVP for answering what happened, what is happening, and what matters next.

## Run locally

Requirements: Python 3.11+ (the app uses only the standard library).

```sh
cd /home/nickbrian/Projects/aim-osi
python3 app.py
```

Open http://localhost:8000. The default `DEMO_MODE=true` clearly labels seeded data as illustrative. Use the workspace selector to choose Kubernetes, Prometheus, etcd, containerd, Argo CD, or OpenTelemetry. Each workspace tracks its listed primary GitHub repository; Kubernetes also loads official SIG meeting metadata. Set `DEMO_MODE=false` to hide demo records. Live integrations require network access; set `GITHUB_TOKEN` to raise GitHub API rate limits. Set both `YOUTUBE_API_KEY` and an official `YOUTUBE_PLAYLIST_ID` to enable playlist imports. GitHub OAuth is intentionally not enabled until callback/session secrets are configured; public data needs no login.

## What is included

- Responsive dashboard, intelligence feed, weekly digest, change summary, community/project, meetings, events, good-first-issues, and automation views.
- JSON API at `/api/*`, health endpoint `/health`, and OpenAPI-style endpoint listing at `/api`.
- Selectable community workspaces backed by a catalog of primary repositories, the official Kubernetes SIG metadata adapter (`kubernetes/community` `sigs.yaml`), public GitHub API adapter, and opt-in YouTube API adapter.
- Provenance on normalized records; connector health and task run history; graceful per-source failure handling.
- SQLite for zero-setup preview with a normalized relational schema and startup migrations. PostgreSQL is the production database target, but runtime wiring is not included in this preview.
- GitHub Actions CI, Dockerfile, security headers, request validation, and tests.

## Configuration

Copy `.env.example` to `.env` and export the values in your shell or use Docker Compose. The server reads `PORT`, `DATABASE_PATH`, `DEMO_MODE`, `GITHUB_TOKEN`, `YOUTUBE_API_KEY`, and `YOUTUBE_PLAYLIST_ID`. Never commit credentials. The zero-dependency local preview uses SQLite.

## API

`GET /api` lists endpoints. `GET /api/workspaces` returns the selectable community catalog. Data routes accept `?community=<workspace-id>` (for example `/api/overview?community=prometheus`); without it, they use Kubernetes. `POST /api/sync` accepts a `community` id and queues a best-effort refresh for its supported connectors.

## Data integrity and limitations

Seeded records are marked `demo`; never treat them as current community facts. In live mode the UI only presents records successfully returned by public sources, with source links and retrieval timestamps. Calendar schedules are links/metadata only; event occurrences are not guessed from recurring text. Summaries are extractive/factual fallback copy, not LLM output. OAuth, private calendars, email delivery, and production PostgreSQL runtime wiring are not enabled in this preview.

## Validation

```sh
python3 -m unittest discover -s tests -v
python3 -m py_compile app.py
```
