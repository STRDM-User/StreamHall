<div align="center"><a name="readme-top"></a>

# StreamHall

**Self-hosted live stream hall - public list, player, admin panel, analytics & push notifications**

**English** · [简体中文](./README.zh-CN.md)

[![][license-shield]][license-link]
[![][python-shield]][python-link]
[![][docker-shield]][docker-link]
[![][postgres-shield]][postgres-link]

</div>

---

- [✨ Features](#-features)
- [🚀 Quick Start](#-quick-start)
- [💻 Local Development](#-local-development)
- [⚙️ Configuration](#️-configuration)
- [🐳 Services](#-services)
- [🔌 Stream Push](#-stream-push)
- [📡 API](#-api)
- [🤝 Contributing](#-contributing)
- [📝 License](#-license)

## ✨ Features

- **Public stream list** - Live and archive tabs, password-protected streams, custom site branding, bilingual UI (Chinese / English) with per-language site description
- **Player** - HLS, FLV, MPEG-DASH playback via ArtPlayer; AES-128 key override and DASH ClearKey support; Widevine and FairPlay DRM playback via Shaka Player (multi-DRM configs per source, Android Telegram WebView detection)
- **Admin panel** - Add, edit, reorder, enable/disable streams; manage sources with per-source labels and proxy mode selection
- **Viewer analytics** - Session tracking, unique visitors, peak concurrent viewers, average watch duration, device / browser / OS / geography breakdown, real-time dashboard, CSV export
- **Telegram notifications** - Per-stream push messages on stream start and stop
- **Stream push** - Local file browser with per-file and per-folder RTMP push management; multi-file folder push with independent stream keys; inline push status and detail modal; remote RTMP push config for external encoders; hidden HLS route proxy (`/h/<slug>`) so real stream keys are never exposed publicly
- **VOD / file serving** - Signed `/video/` URLs with HTTP Range support (seek-capable); publish any local video file or folder as an archive stream directly from the file browser
- **HLS proxy modes** - Per-source direct, full proxy, or manifest-only proxy modes for balancing source URL exposure, CORS compatibility, and server bandwidth
- **API key auth** - Generate per-key tokens in the admin panel for programmatic access to all admin and analytics endpoints
- **Mobile responsive** - Admin panel sidebar, source editor, file browser rows, and push directory sidebar all collapse gracefully on narrow screens

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🚀 Quick Start

**1. Download and configure**

```bash
curl -LO https://git.stdm.moe/Stardream/StreamHall/raw/branch/main/docker-compose.yml
curl -LO https://git.stdm.moe/Stardream/StreamHall/raw/branch/main/nginx-hls.conf
```

> [!IMPORTANT]
> Open `docker-compose.yml` and change `SECRET_KEY` and `POSTGRES_PASSWORD` to strong random values before starting.

**2. Start**

```bash
docker compose up -d
```

**3. Find the initial admin password**

```bash
docker logs streamhall
```

Look for:

```
StreamHall initial admin password: <random-password>
```

**4. Access**

| URL | Description |
|---|---|
| `http://HOST:8085/` | Public stream list |
| `http://HOST:8085/admin` | Admin panel |
| `http://HOST:18088/` | SRS HTTP playback |
| `http://HOST:8889/` | Nginx HLS proxy |

> [!TIP]
> After first login, change your password under **Site Settings → Security**. The initial password is printed once and not stored in plaintext.

**Updating to a newer version**

```bash
docker compose pull && docker compose up -d
```

**Resetting a forgotten admin password**

```bash
docker exec streamhall-postgres psql -U streamhall -d streamhall \
  -c "DELETE FROM site_settings WHERE key='admin_password_hash';"
docker restart streamhall
docker logs streamhall
```

Deleting the hash causes StreamHall to generate a new random password on the next startup and print it to the log.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 💻 Local Development

**Prerequisites:** Python 3.12+, PostgreSQL 14+

```bash
# 1. Clone the repo
git clone https://git.stdm.moe/Stardream/StreamHall.git
cd StreamHall

# 2. Install the single runtime dependency
pip install -r requirements.txt

# 3. Export required environment variables
export SECRET_KEY=dev-secret
export DATABASE_URL=postgresql://user:pass@localhost:5432/streamhall

# 4. Start the server
python server.py
```

The app listens on `http://localhost:8080` by default. On first startup it creates the database schema and prints a one-time admin password to stdout.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## ⚙️ Configuration

Set these environment variables in `docker-compose.yml`:

| Variable | Default | Required | Description |
|---|---|---|---|
| `SECRET_KEY` | `REPLACE_ME` | **Yes** | HMAC signing key for sessions and stream routes. Generate with `openssl rand -hex 32` |
| `DATABASE_URL` | see compose file | **Yes** | PostgreSQL connection string |
| `POSTGRES_PASSWORD` | see compose file | **Yes** | PostgreSQL password |
| `TZ` | `UTC` | No | Container timezone, e.g. `Asia/Shanghai` |
| `SRS_HTTP_ORIGIN` | `http://srs:8080` | No | SRS HTTP playback base URL |
| `STREAM_PROBE_TIMEOUT` | `4` | No | Seconds before aborting a stream URL probe |
| `STREAM_MONITOR_INTERVAL` | `10` | No | Seconds between stream liveness checks |
| `TELEGRAM_TIMEOUT` | `6` | No | Seconds before aborting a Telegram API call |
| `RTMP_HOST` | `srs` | No | Hostname of the SRS container used for local push jobs |
| `VIDEOS_DIRS` | *(unset)* | No | Comma-separated list of directories exposed in the file browser. Optionally prefix each path with a label: `label:/app/path`. Multiple entries: `movies:/app/movies,shows:/app/shows` |

> [!WARNING]
> Always change `SECRET_KEY` and `POSTGRES_PASSWORD` before exposing StreamHall to a network. The defaults are intentionally weak placeholders.

**Mounting video directories for Local Push**

*Method 1 — single base directory, multiple sources as subdirectories:*

Mount multiple host paths under one container base path. The file browser shows them as subdirectories.

```yaml
environment:
  VIDEOS_DIRS: "/app/videos"
volumes:
  - ./videos:/app/videos/local
  - /your/media/path:/app/videos/external
```

*Method 2 — multiple labeled top-level directories:*

Each path is listed as a separate labeled root entry in the file browser.

```yaml
environment:
  VIDEOS_DIRS: "/app/videos,external:/app/media/external"
volumes:
  - ./videos:/app/videos
  - /your/media/path:/app/media/external
```

**HLS proxy modes**

Each stream source can choose how external HLS URLs are exposed to viewers:

| Mode | Behavior | Bandwidth impact |
|---|---|---|
| `Auto` | Backward-compatible default; external HLS uses the full proxy | StreamHall carries manifest and segment traffic |
| `Direct` | Player uses the source URL directly | Viewer traffic goes to the source server |
| `Full proxy` | Manifest, segments, maps, and keys are routed through `/proxy/hls/` | StreamHall carries all HLS media traffic |
| `Manifest only` | Only the playlist uses StreamHall; segment/key/map URLs are absolute source URLs | Low StreamHall bandwidth; final media URLs remain visible in browser network tools |

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🐳 Services

The default compose stack starts four containers:

| Container | Image | Port(s) | Purpose |
|---|---|---|---|
| `streamhall` | local build | `8085→8080` | Python app + static frontend |
| `streamhall-postgres` | `postgres:16-alpine` | - | PostgreSQL persistent database |
| `streamhall-srs` | `ossrs/srs:6` | `1935`, `18088→8080` | RTMP publishing + HTTP playback |
| `streamhall-nginx-hls` | `nginx:alpine` | `8889→80` | Clean public HLS route proxy |

Persistent data is stored in `./postgres-data/`.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🔌 Stream Push

SRS handles RTMP publishing on port `1935`. Configure your encoder as follows:

```
RTMP server:  rtmp://HOST:1935/live
Stream key:   your-stream-key
```

The admin panel's **Local Push** section provides a file browser for your media directory. Select any video file to push it via RTMP with a custom stream key, or select a folder to push all contained videos simultaneously with independent stream keys. Push status is shown inline per file; a detail modal gives real-time duration, copy addresses, and stop controls.

The hidden public HLS URL (`/h/<slug>/...`) routes through nginx on port `8889`, keeping the real stream key out of the public URL.

> [!NOTE]
> The RTMP host field accepts an optional port, e.g. `live.example.com:1935`. Do not hard-code `:1935` if you use a non-standard port.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 📡 API

### Authentication

- **Browser session** - cookie set by `POST /api?action=login`
- **API key** - send `Authorization: Bearer <token>` header. Keys are managed in the admin panel under **Site Settings → API Keys**.

### Public Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api?action=public_list` | Stream list for the public homepage |
| `GET` | `/api?action=site_settings` | Site title, description, branding |
| `GET` | `/api?action=get_player_data&id=<public_id>` | Player sources and metadata |
| `POST` | `/api?action=verify_password` | Verify a stream password |
| `GET` | `/video/<token>/<payload>` | Signed VOD endpoint with HTTP Range support |

### Admin Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api?action=login` | Authenticate and start a session |
| `GET` | `/api?action=logout` | End the current session |
| `GET` | `/api?action=list_admin` | Full stream list |
| `POST` | `/api?action=add` | Add a stream |
| `POST` | `/api?action=update` | Update a stream |
| `POST` | `/api?action=delete` | Delete a stream |
| `POST` | `/api?action=set_stream_enabled` | Toggle stream visibility |
| `POST` | `/api?action=reorder_streams` | Reorder within a label group |
| `POST` | `/api?action=update_site_settings` | Save site settings |
| `GET` | `/api?action=telegram_settings` | Read Telegram bot config |
| `POST` | `/api?action=update_telegram_settings` | Save Telegram bot config |
| `POST` | `/api?action=update_admin_password` | Change admin password |
| `GET` | `/api?action=list_api_keys` | List API keys (tokens not returned) |
| `POST` | `/api?action=create_api_key` | Create a key - token returned once |
| `POST` | `/api?action=delete_api_key` | Revoke a key by `id` |
| `GET` | `/api?action=list_pushes` | List active push jobs |
| `POST` | `/api?action=start_push` | Start an RTMP push job for a file |
| `POST` | `/api?action=stop_push` | Stop a push job by stream key |
| `GET` | `/api?action=list_folder_videos` | List playable files in a directory (with signed URLs) |

### Analytics Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api?action=stats_overview` | Aggregated totals |
| `GET` | `/api?action=stats_streams` | Per-stream stats table |
| `GET` | `/api?action=stats_timeseries` | Bucketed view counts |
| `GET` | `/api?action=stats_geo` | Country-level visitor counts |
| `GET` | `/api?action=stats_stream_detail&id=<id>` | Single-stream detail |
| `GET` | `/api?action=stats_sessions_page&id=<id>` | Paginated session list |
| `GET` | `/api?action=stats_export_csv&range=<range>` | Export sessions as CSV (`range`: `today`, `7d`, `30d` (default), `all`) |

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 💡 Acknowledgements

StreamHall was inspired by [AniLive](https://anilive.nekoss.cn/), a live stream listing site that is not open-source. This project is an independent reimplementation built from scratch, sharing no code with the original site.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 🤝 Contributing

Issues and pull requests are welcome. Please open an issue first for major changes so the direction can be discussed.

<div align="right">

[![][back-to-top]](#readme-top)

</div>

## 📝 License

Copyright © 2026 [Stardream](https://git.stdm.moe/Stardream). This project is licensed under the [GNU Affero General Public License v3.0](./LICENSE).

**Permissions**

| | |
|---|---|
| ✅ | Commercial use |
| ✅ | Modification |
| ✅ | Distribution |
| ✅ | Private use |

**Conditions**

| | |
|---|---|
| ⚠️ | Disclose source - modified versions must also be open-source |
| ⚠️ | Same license - derivative works must use AGPL-3.0 |
| ⚠️ | Network use = distribution - if you run a modified version as a network service, you must make the source available to users |
| ⚠️ | State changes - modifications must be documented |

In short: you are free to use, modify, and self-host StreamHall. If you distribute or offer a modified version as a hosted service, you must publish the full source under AGPL-3.0.

---

<!-- LINK GROUP -->
[back-to-top]: https://img.shields.io/badge/-BACK_TO_TOP-black?style=flat-square
[license-shield]: https://img.shields.io/badge/license-AGPL--3.0-white?labelColor=black&style=flat-square
[license-link]: ./LICENSE
[python-shield]: https://img.shields.io/badge/python-3.12-blue?labelColor=black&logo=python&logoColor=white&style=flat-square
[python-link]: https://www.python.org/
[docker-shield]: https://img.shields.io/badge/docker-compose-2496ED?labelColor=black&logo=docker&logoColor=white&style=flat-square
[docker-link]: https://docs.docker.com/compose/
[postgres-shield]: https://img.shields.io/badge/postgres-16-336791?labelColor=black&logo=postgresql&logoColor=white&style=flat-square
[postgres-link]: https://www.postgresql.org/
