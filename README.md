# AIM OSI

Open source community intelligence, with selectable workspaces for Kubernetes, Prometheus, etcd, containerd, Argo CD, and OpenTelemetry.

## Run locally

Requires Python 3.11+; runtime dependencies use the Python standard library.

```sh
python3 app.py
```

Open `http://localhost:8000`. The local preview starts with clearly labeled demo records. Set `DEMO_MODE=false` to show only verified source data. Select a workspace and use **Sync sources** to pull public GitHub activity, Kubernetes SIG metadata, and recent videos from the project’s configured YouTube channel/feed.

## Deploy on Vercel

Import this repository in Vercel with the project root set to the repository root. `vercel.json` publishes the `public/` frontend and routes `/api/*` to the Python function in `api/index.py`. The serverless app runs in live mode and keeps only a temporary SQLite cache in `/tmp`; source data is refreshed on demand. Vercel’s temporary filesystem is not durable storage, so this preview does not claim persistent task history.

Optional GitHub sign-in requires these Vercel environment variables:

- `GITHUB_CLIENT_ID`
- `GITHUB_CLIENT_SECRET`
- `SESSION_SECRET` (long random value)
- `GITHUB_OAUTH_REDIRECT_URI` (for example `https://YOUR_DOMAIN/api/auth/callback`)

Create a GitHub OAuth App with the callback URL above. The app requests only `read:user`, stores a signed, HTTP-only profile session, and does not retain the OAuth access token. Public GitHub data can be read without a user login; `GITHUB_TOKEN` is optional and increases API rate limits.

The YouTube connector reads public official-channel Atom feeds; it requires no YouTube API key. Video results are limited to the previous seven days. A project that has no clearly identified official channel uses the CNCF channel with title/description filtering; the source channel is always linked and the embedded YouTube player remains subject to the video's embed settings.

## Features and routes

- Weekly digest combines the former intelligence feed and “This week” page, including searchable recent activity and playable YouTube embeds.
- Community overview, repositories, good-first issues, meetings, events, integrations, and sync status.
- `/api/workspaces`, `/api/overview?community=<id>`, `/api/digest?community=<id>`, `/api/automation?community=<id>`, `/api/sync`, and `/health`.
- Provenance links and retrieval/publication dates are kept with imported source records.

## Validation

```sh
PYTHONPYCACHEPREFIX=/tmp/aim-osi-pycache python3 -m py_compile app.py api/index.py
python3 -m unittest discover -s tests -v
node --check web/app.js
```

Live GitHub and YouTube access depends on the deployed function being permitted to make outbound HTTPS requests. Configure Vercel environment values in its project settings; secrets should not be committed.
