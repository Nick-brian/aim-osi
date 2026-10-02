# AIM OSI architecture

## Request and data flow

```text
Official source APIs / structured metadata
                 ↓
      Connector adapters (app.py)
                 ↓
 Normalize + deduplicate by external ID
                 ↓
 SQLite preview store (PostgreSQL-ready relational domains)
                 ↓
        Read-only JSON API (/api/*)
                 ↓
 Responsive browser views (web/)
```

The Kubernetes adapter reads `kubernetes/community` `sigs.yaml` and keeps group schedules, time zones, join URLs, archives, and public calendar links when the source supplies them. The GitHub adapter reads Kubernetes pull requests, explicitly labeled `good first issue` issues, and releases. YouTube is opt-in and requires both an API key and an official playlist ID. No meeting occurrence dates are generated from recurring schedule text.

## Normalized records

- `communities`, `projects`, and `groups` represent the ecosystem hierarchy without Kubernetes-only columns.
- `signals` stores feed and digest items from GitHub/YouTube, including kind, external ID, source URL, item URL, publication/update/retrieval timestamps, and a `demo` marker.
- `sync_runs` stores individual connector outcomes. `tasks` stores queued, running, retried, successful, and failed background work.

Unique external IDs make connector writes safe to repeat. Each source runs independently; a failed adapter updates its own run/task and leaves other records available. Sync requests are capped per client and the active queue has a fixed limit.

## API surface

`GET /api` lists the routes. The main reads are `/api/overview`, `/api/communities`, `/api/projects`, `/api/activity`, `/api/issues?label=good%20first%20issue`, `/api/meetings`, `/api/events`, `/api/digest`, `/api/automation`, and `/api/integrations`. `POST /api/sync` accepts an allow-listed connector list and returns queued task IDs. `/health` is suitable for local health checks.

All public routes return public community information. There are no private-user routes yet. The GitHub sign-in entry point remains disabled until OAuth callback handling, secure session storage, CSRF protection, and secrets are configured.

## Deployment boundary

The local and Docker preview uses SQLite so it starts without external services or package installation. PostgreSQL is the intended hosted database, but an ORM/driver, production migration path, and hosted authentication are follow-up work. Treat this as a preview, not an internet-exposed production deployment.
