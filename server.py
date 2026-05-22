#!/usr/bin/env python3
from __future__ import annotations

import base64
import socket
import csv
import hashlib
import hmac
import io
import json
import mimetypes
import os
import re
import secrets
import subprocess
import tempfile
import threading
import time
import uuid
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None


# StreamHall server - single-file Python HTTP server (no external framework).
# Each request gets its own thread (ThreadingHTTPServer) and its own PostgreSQL
# connection. Two daemon threads run in the background: one monitors stream
# liveness, the other cleans up stale viewer sessions.
# Admin endpoints require either a session cookie or a Bearer/query-param API key.

class AppError(Exception):
    """Carries an i18n error code and an optional English detail string.
    The code maps to err.<code> in the frontend translation tables.
    The detail (e.g. a third-party API description) is passed through as-is."""
    def __init__(self, code: str, detail: str = ""):
        super().__init__(code)
        self.code = code
        self.detail = detail


ROOT = Path(__file__).resolve().parent
PUBLIC_DIR = ROOT / "public"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SECRET_KEY = os.getenv("SECRET_KEY", "change-this-secret").encode("utf-8")
SESSION_COOKIE = "streamhall_session"
SESSION_MAX_AGE = 60 * 60 * 24
PASSWORD_HASH_ITERATIONS = 240000
INITIAL_ADMIN_PASSWORD_BYTES = 18
PUBLIC_ID_BYTES = 9
STREAM_PROBE_TIMEOUT = float(os.getenv("STREAM_PROBE_TIMEOUT", "4"))
TELEGRAM_TIMEOUT = float(os.getenv("TELEGRAM_TIMEOUT", "6"))
STREAM_MONITOR_INTERVAL = max(5, int(os.getenv("STREAM_MONITOR_INTERVAL", "10")))
SRS_HTTP_ORIGIN = os.getenv("SRS_HTTP_ORIGIN", "http://srs:8080").rstrip("/")
OBS_ROUTE_SLUG_LENGTH = max(12, int(os.getenv("OBS_ROUTE_SLUG_LENGTH", "22")))
URL_PATH_SAFE = "/._~!$&'()*+,;=:@"

# Video push
_VIDEOS_DIRS_RAW = os.environ.get("VIDEOS_DIRS", "/app/videos")

def _parse_videos_dirs(raw: str) -> list[dict]:
    result = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item and not item.startswith("/"):
            label, path = item.split(":", 1)
        else:
            label = os.path.basename(item.rstrip("/")) or item
            path = item
        result.append({"label": label, "path": path})
    return result

VIDEOS_DIRS: list[dict] = _parse_videos_dirs(_VIDEOS_DIRS_RAW)
UPLOAD_DIR: str = VIDEOS_DIRS[0]["path"] if VIDEOS_DIRS else "/app/videos"
RTMP_HOST = os.environ.get("RTMP_HOST", "localhost")
VIDEO_EXTS = frozenset({".mp4", ".mkv", ".avi", ".flv", ".ts", ".mov", ".wmv", ".webm", ".m4v"})

active_pushes: dict[str, dict] = {}
_pushes_lock = threading.Lock()
HLS_PROXY_PREFIX = "/proxy/hls"
HLS_URI_RE = re.compile(r'URI="([^"]+)"')  # matches URI="..." attributes in HLS tag lines (e.g. EXT-X-KEY)

DEFAULT_SITE_SETTINGS = {
    "site_title": "StreamHall",
    "site_icon_url": "",
    "site_description": "",
    "site_description_en": "",
    "site_nav_links": '[{"label":"直播列表","url":"#stream-list"}]',
    "site_nav_links_en": '[{"label":"Streams","url":"#stream-list"}]',
    "footer_markdown": "",
    "footer_markdown_en": "",
    "telegram_public_base_url": "",
    "obs_rtmp_host": "",
    "obs_playback_origin": "",
}

DEFAULT_TELEGRAM_SETTINGS = {
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "telegram_public_base_url": "",
    "telegram_live_notify_start": "0",
    "telegram_live_notify_stop": "0",
    "telegram_live_start_template": "【开播提醒】{title}\n观看地址：{url}\n时间：{time}",
    "telegram_live_stop_template": "【关播提醒】{title}\n时间：{time}",
    "telegram_archive_notify_start": "0",
    "telegram_archive_notify_stop": "0",
    "telegram_archive_start_template": "【上架提醒】{title}\n观看地址：{url}\n时间：{time}",
    "telegram_archive_stop_template": "【下架提醒】{title}\n时间：{time}",
}



def now() -> int:
    return int(time.time())


class PostgresCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.rowcount = cursor.rowcount

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()


class PostgresConnection:
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is required. StreamHall uses Postgres only.")
        if psycopg is None:
            raise RuntimeError("psycopg is required. Install requirements.txt before starting StreamHall.")
        # Retry for up to 30 seconds so the app can start before Postgres is
        # ready in the Docker Compose stack (postgres container may still be
        # initialising when this container starts).
        last_exc = None
        for _ in range(30):
            try:
                self.conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
                break
            except Exception as exc:
                last_exc = exc
                time.sleep(1)
        else:
            raise RuntimeError(f"Could not connect to Postgres: {last_exc}")

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self.conn.__exit__(exc_type, exc, tb)

    def execute(self, sql: str, params: tuple | list = ()):
        # Translate SQLite-style ? placeholders to psycopg %s before executing.
        return PostgresCursor(self.conn.execute(sql.replace("?", "%s"), params))


def db():
    return PostgresConnection()


def generate_public_id(conn) -> str:
    while True:
        value = secrets.token_urlsafe(PUBLIC_ID_BYTES)
        row = conn.execute("SELECT 1 FROM streams WHERE public_id = ?", (value,)).fetchone()
        if not row:
            return value


def obs_route_slug(stream_key: str) -> str:
    # Derive a public URL slug from a stream key using HMAC-SHA256.
    # The slug is deterministic (same key -> same slug) but non-reversible,
    # so the real stream key is never exposed in the public HLS URL.
    digest = hmac.new(SECRET_KEY, stream_key.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")[:OBS_ROUTE_SLUG_LENGTH]


def normalize_stream_label(value: object) -> str:
    label = str(value or "LIVE").strip().upper()
    return label if label in ("LIVE", "ARCHIVE") else "LIVE"


def init_db() -> None:
    init_postgres_db()


def init_postgres_db() -> None:
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS streams (
                id BIGSERIAL PRIMARY KEY,
                public_id TEXT NOT NULL DEFAULT '',
                stream_label TEXT NOT NULL DEFAULT 'LIVE',
                event_name TEXT NOT NULL,
                stream_password TEXT NOT NULL DEFAULT '',
                links_json TEXT NOT NULL DEFAULT '[]',
                is_hidden INTEGER NOT NULL DEFAULT 0,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                tg_notify_enabled INTEGER NOT NULL DEFAULT 0,
                admin_pinned INTEGER NOT NULL DEFAULT 0,
                public_pinned INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_public_id ON streams(public_id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS site_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stream_probe_states (
                stream_id INTEGER PRIMARY KEY,
                is_live INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS obs_stream_routes (
                id BIGSERIAL PRIMARY KEY,
                stream_key TEXT NOT NULL UNIQUE,
                public_slug TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id BIGSERIAL PRIMARY KEY,
                token_hash TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS push_jobs (
                job_id TEXT PRIMARY KEY,
                dir_index INTEGER NOT NULL,
                rel_path TEXT NOT NULL DEFAULT '',
                filename TEXT NOT NULL DEFAULT '',
                stream_key TEXT NOT NULL,
                hls_slug TEXT NOT NULL DEFAULT '',
                loop INTEGER NOT NULL DEFAULT 0,
                is_folder INTEGER NOT NULL DEFAULT 0,
                started_at INTEGER NOT NULL
            )
            """
        )
        init_stats_tables(conn)
        for key, value in DEFAULT_SITE_SETTINGS.items():
            conn.execute(
                "INSERT INTO site_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                (key, value),
            )
        for key, value in DEFAULT_TELEGRAM_SETTINGS.items():
            conn.execute(
                "INSERT INTO site_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
                (key, value),
            )
        row = conn.execute("SELECT COUNT(*) AS count FROM streams").fetchone()
        ensure_admin_password(conn)


def init_stats_tables(conn) -> None:
    # ALTER TABLE ... ADD COLUMN IF NOT EXISTS is used for additive schema
    # migrations: new columns are appended without dropping existing data,
    # so upgrades on a live database are safe and idempotent.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS viewer_sessions (
            session_id TEXT PRIMARY KEY,
            visitor_id TEXT NOT NULL,
            stream_id INTEGER NOT NULL,
            public_id TEXT NOT NULL,
            ip_hash TEXT NOT NULL DEFAULT '',
            user_agent TEXT NOT NULL DEFAULT '',
            referer TEXT NOT NULL DEFAULT '',
            device_type TEXT NOT NULL DEFAULT '',
            started_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            ended_at INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            play_state TEXT NOT NULL DEFAULT 'viewing'
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_viewer_sessions_stream ON viewer_sessions(stream_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_viewer_sessions_last_seen ON viewer_sessions(last_seen_at)")
    conn.execute("ALTER TABLE viewer_sessions ADD COLUMN IF NOT EXISTS ip_address TEXT NOT NULL DEFAULT ''")
    conn.execute("ALTER TABLE viewer_sessions ADD COLUMN IF NOT EXISTS os TEXT NOT NULL DEFAULT ''")
    conn.execute("ALTER TABLE viewer_sessions ADD COLUMN IF NOT EXISTS browser TEXT NOT NULL DEFAULT ''")
    conn.execute("ALTER TABLE streams ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
    conn.execute("UPDATE streams SET sort_order = id WHERE sort_order = 0")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS viewer_events (
            id BIGSERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            stream_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_at INTEGER NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{{}}'
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_viewer_events_stream_time ON viewer_events(stream_id, event_at)")



def reset_postgres_sequence(conn, table: str) -> None:
    conn.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE((SELECT MAX(id) FROM {table}), 1), (SELECT COUNT(*) FROM {table}) > 0)"
    )


def b64encode_bytes(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def b64decode_bytes(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def hash_admin_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${b64encode_bytes(salt)}${b64encode_bytes(digest)}"


def verify_admin_password_hash(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations_text, salt_text, digest_text = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        salt = b64decode_bytes(salt_text)
        expected = b64decode_bytes(digest_text)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    except Exception:
        return False
    return hmac.compare_digest(actual, expected)


def get_setting(conn, key: str) -> str:
    row = conn.execute("SELECT value FROM site_settings WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else ""


def set_setting(conn, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO site_settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def generate_initial_admin_password() -> str:
    return secrets.token_urlsafe(INITIAL_ADMIN_PASSWORD_BYTES)


def ensure_admin_password(conn) -> None:
    if get_setting(conn, "admin_password_hash"):
        return
    password = generate_initial_admin_password()
    set_setting(conn, "admin_password_hash", hash_admin_password(password))
    set_setting(conn, "admin_password_changed_at", str(now()))
    print("=" * 72, flush=True)
    print("StreamHall initial admin password:", password, flush=True)
    print("Save this password now. It is shown only once.", flush=True)
    print("=" * 72, flush=True)


def sign(value: str) -> str:
    sig = hmac.new(SECRET_KEY, value.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode("ascii").rstrip("=")


def encode_proxy_target(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")


def decode_proxy_target(value: str) -> str:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def hls_proxy_host_token(host: str) -> str:
    return sign(f"hls-proxy-host:{host.lower()}")


def hls_proxy_path(url: str) -> str:
    host = urlparse(url).netloc
    encoded = encode_proxy_target(url)
    return f"{HLS_PROXY_PREFIX}/{hls_proxy_host_token(host)}/{encoded}"


PLAYABLE_VIDEO_EXTS = frozenset({".mp4", ".mkv", ".mov", ".webm", ".m4v"})


def _dir_has_ext(path: str, exts: frozenset, max_depth: int = 10) -> bool:
    """Return True if path contains any file with a matching extension (recursive, early exit)."""
    if max_depth <= 0:
        return False
    try:
        for name in os.listdir(path):
            if name.startswith((".", "@", "#")):
                continue
            full = os.path.join(path, name)
            if os.path.isfile(full):
                if os.path.splitext(name)[1].lower() in exts:
                    return True
            elif os.path.isdir(full):
                if _dir_has_ext(full, exts, max_depth - 1):
                    return True
    except OSError:
        pass
    return False


def video_proxy_path(dir_index: int, rel_path: str, filename: str) -> str:
    payload = f"{dir_index}:{rel_path}:{filename}"
    token = sign(f"video-proxy:{payload}")
    encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
    return f"/video/{token}/{encoded}"


def is_hls_link(link: dict[str, object]) -> bool:
    raw_url = str(link.get("url") or "")
    link_type = str(link.get("type") or "").strip().lower()
    path = urlparse(raw_url).path.lower()
    return link_type == "m3u8" or path.endswith(".m3u8")


def add_playback_urls(links: list[dict[str, object]]) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for link in links:
        item = dict(link)
        raw_url = str(item.get("url") or "")
        parsed = urlparse(raw_url)
        if is_hls_link(item) and parsed.scheme in ("http", "https"):
            item["playback_url"] = hls_proxy_path(raw_url)
        prepared.append(item)
    return prepared


def make_session() -> str:
    value = f"admin:{now()}"
    token = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{token}.{sign(value)}"


def verify_session(token: str | None) -> bool:
    if not token or "." not in token:
        return False
    payload, given_sig = token.rsplit(".", 1)
    try:
        padded = payload + "=" * (-len(payload) % 4)
        value = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        role, issued = value.split(":", 1)
        issued_at = int(issued)
        if role != "admin" or now() - issued_at > SESSION_MAX_AGE:
            return False
        # Reject sessions issued before the last password change so that
        # rotating the password immediately invalidates all existing sessions.
        if issued_at < admin_session_not_before():
            return False
    except Exception:
        return False
    return hmac.compare_digest(sign(value), given_sig)


def admin_session_not_before() -> int:
    try:
        with db() as conn:
            return int(get_setting(conn, "admin_password_changed_at") or 0)
    except Exception:
        return 0


def normalize_links(raw: object) -> list[dict[str, str]]:
    links = raw if isinstance(raw, list) else []
    normalized = []
    for item in links:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        url = str(item.get("url", "")).strip()
        if not name or not url:
            continue
        link_type = str(item.get("type", "")).strip().lower()
        if link_type not in ("", "m3u8", "flv", "dash"):
            link_type = ""
        normalized.append(
            {
                "name": name,
                "type": link_type,
                "url": url,
                "key": str(item.get("key", "")).strip(),
                "clearkey": str(item.get("clearkey", "")).strip(),
            }
        )
    return normalized


def normalize_site_settings(raw: dict[str, object]) -> dict[str, str]:
    site_title = str(raw.get("siteTitle", raw.get("site_title", ""))).strip()
    site_description = str(raw.get("siteDescription", raw.get("site_description", ""))).strip()
    site_description_en = str(raw.get("siteDescriptionEn", raw.get("site_description_en", ""))).strip()
    site_icon_url = str(raw.get("siteIconUrl", raw.get("site_icon_url", ""))).strip()
    footer_markdown = str(raw.get("footerMarkdown", raw.get("footer_markdown", ""))).strip()
    footer_markdown_en = str(raw.get("footerMarkdownEn", raw.get("footer_markdown_en", ""))).strip()

    def _parse_nav_links(raw_val: object) -> list[dict[str, str]]:
        if isinstance(raw_val, str):
            try:
                raw_val = json.loads(raw_val)
            except json.JSONDecodeError:
                raw_val = []
        result: list[dict[str, str]] = []
        if isinstance(raw_val, list):
            for item in raw_val[:12]:
                if not isinstance(item, dict):
                    continue
                label = str(item.get("label", "")).strip()[:24]
                url = str(item.get("url", "")).strip()[:300]
                if not label or not url:
                    continue
                result.append({"label": label, "url": url})
        return result

    nav_links = _parse_nav_links(raw.get("navLinks", raw.get("site_nav_links", [])))
    nav_links_en = _parse_nav_links(raw.get("navLinksEn", raw.get("site_nav_links_en", [])))

    if not site_title:
        raise AppError("site_title_empty")
    if site_icon_url and not (
        site_icon_url.startswith("/")
        or site_icon_url.startswith("data:image/")
        or urlparse(site_icon_url).scheme in ("http", "https")
    ):
        raise AppError("site_icon_invalid")
    public_base_url = str(raw.get("publicBaseUrl", raw.get("telegram_public_base_url", ""))).strip().rstrip("/")
    return {
        "site_title": site_title[:80],
        "site_description": site_description[:300],
        "site_description_en": site_description_en[:300],
        "site_icon_url": site_icon_url[:500],
        "site_nav_links": json.dumps(nav_links, ensure_ascii=False),
        "site_nav_links_en": json.dumps(nav_links_en, ensure_ascii=False),
        "footer_markdown": footer_markdown[:5000],
        "footer_markdown_en": footer_markdown_en[:5000],
        "telegram_public_base_url": public_base_url[:300],
    }


def bool_setting(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    text = str(value or "").strip().lower()
    return "1" if text in ("1", "true", "yes", "on") else "0"


def _tpl(raw: dict[str, object], camel_key: str, db_key: str) -> str:
    return str(raw.get(camel_key, raw.get(db_key, DEFAULT_TELEGRAM_SETTINGS.get(db_key, "")))).strip()


def normalize_telegram_settings(raw: dict[str, object]) -> dict[str, str]:
    return {
        "telegram_bot_token": str(raw.get("botToken", raw.get("telegram_bot_token", ""))).strip()[:200],
        "telegram_chat_id": str(raw.get("chatId", raw.get("telegram_chat_id", ""))).strip()[:120],
        "telegram_public_base_url": str(
            raw.get("publicBaseUrl", raw.get("telegram_public_base_url", ""))
        ).strip().rstrip("/")[:300],
        "telegram_live_notify_start":    bool_setting(raw.get("liveNotifyStart",    raw.get("telegram_live_notify_start", "0"))),
        "telegram_live_notify_stop":     bool_setting(raw.get("liveNotifyStop",     raw.get("telegram_live_notify_stop", "0"))),
        "telegram_live_start_template":  (_tpl(raw, "liveStartTemplate",    "telegram_live_start_template")  or DEFAULT_TELEGRAM_SETTINGS["telegram_live_start_template"])[:1000],
        "telegram_live_stop_template":   (_tpl(raw, "liveStopTemplate",     "telegram_live_stop_template")   or DEFAULT_TELEGRAM_SETTINGS["telegram_live_stop_template"])[:1000],
        "telegram_archive_notify_start": bool_setting(raw.get("archiveNotifyStart", raw.get("telegram_archive_notify_start", "0"))),
        "telegram_archive_notify_stop":  bool_setting(raw.get("archiveNotifyStop",  raw.get("telegram_archive_notify_stop", "0"))),
        "telegram_archive_start_template": (_tpl(raw, "archiveStartTemplate", "telegram_archive_start_template") or DEFAULT_TELEGRAM_SETTINGS["telegram_archive_start_template"])[:1000],
        "telegram_archive_stop_template":  (_tpl(raw, "archiveStopTemplate",  "telegram_archive_stop_template")  or DEFAULT_TELEGRAM_SETTINGS["telegram_archive_stop_template"])[:1000],
    }


def stream_probe_response(valid: bool, status_code: int | None = None) -> dict[str, object]:
    return {
        "valid": valid,
        "code": "detected" if valid else "no_info",
        "status_code": status_code,
    }


def decode_probe_text(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return ""


def resolve_video_file_path(url_path: str) -> str | None:
    """Decode a /video/<token>/<encoded> path and return the absolute filepath, or None if invalid/missing."""
    parts = url_path.strip("/").split("/", 2)
    if len(parts) != 3:
        return None
    _, token, encoded = parts
    padded = encoded + "=" * (-len(encoded) % 4)
    try:
        payload = base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        return None
    if not hmac.compare_digest(token, sign(f"video-proxy:{payload}")):
        return None
    payload_parts = payload.split(":", 2)
    if len(payload_parts) != 3:
        return None
    dir_index_str, rel_path, filename = payload_parts
    try:
        dir_index = int(dir_index_str)
        assert 0 <= dir_index < len(VIDEOS_DIRS)
    except (ValueError, AssertionError):
        return None
    if rel_path and (".." in rel_path.split("/") or rel_path.startswith("/") or os.path.isabs(rel_path)):
        return None
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        return None
    filepath = (
        os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path, filename)
        if rel_path
        else os.path.join(VIDEOS_DIRS[dir_index]["path"], filename)
    )
    return filepath if os.path.isfile(filepath) else None


def probe_stream_url(raw_url: object, type_hint: object = "") -> dict[str, object]:
    url = str(raw_url or "").strip()
    if not url:
        return stream_probe_response(False)

    if url.startswith("/video/"):
        return stream_probe_response(resolve_video_file_path(url) is not None)

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return stream_probe_response(False)

    hint = str(type_hint or "").strip().lower()
    path = parsed.path.lower()
    is_hls = hint == "m3u8" or path.endswith(".m3u8")
    is_dash = hint == "dash" or path.endswith(".mpd")
    is_flv = hint == "flv" or path.endswith(".flv")
    headers = {
        "User-Agent": "StreamHall/1.0",
        "Accept": "*/*",
    }
    if not is_hls and not is_dash:
        headers["Range"] = "bytes=0-4095"

    try:
        with urlopen(Request(url, headers=headers), timeout=STREAM_PROBE_TIMEOUT) as resp:
            status_code = int(getattr(resp, "status", resp.getcode()) or 0)
            content_type = resp.headers.get("Content-Type", "").lower()
            read_limit = 65536 if is_hls or is_dash or "mpegurl" in content_type or "xml" in content_type else 4096
            data = resp.read(read_limit)
    except HTTPError as exc:
        return stream_probe_response(False, exc.code)
    except (TimeoutError, URLError, OSError):
        return stream_probe_response(False)

    if status_code >= 400:
        return stream_probe_response(False, status_code)

    if is_hls or "mpegurl" in content_type or "m3u8" in content_type:
        text = decode_probe_text(data)
        has_playlist = "#EXTM3U" in text
        has_live_media = any(
            marker in text
            for marker in ("#EXTINF", "#EXT-X-STREAM-INF", "#EXT-X-MEDIA-SEQUENCE", "#EXT-X-PART")
        )
        return stream_probe_response(has_playlist and has_live_media, status_code)

    if is_dash or "dash+xml" in content_type:
        return stream_probe_response(b"<MPD" in data[:2048], status_code)

    if is_flv or "flv" in content_type:
        return stream_probe_response(data.startswith(b"FLV") or len(data) > 0, status_code)

    has_video_type = content_type.startswith("video/") or content_type.startswith("audio/")
    is_binary_stream = "application/octet-stream" in content_type
    return stream_probe_response(len(data) > 0 and (has_video_type or is_binary_stream), status_code)


def site_settings() -> dict[str, str]:
    settings = dict(DEFAULT_SITE_SETTINGS)
    with db() as conn:
        rows = conn.execute("SELECT key, value FROM site_settings").fetchall()
    for row in rows:
        if row["key"] in settings:
            settings[row["key"]] = row["value"]
    return settings


def telegram_settings() -> dict[str, str]:
    settings = dict(DEFAULT_TELEGRAM_SETTINGS)
    with db() as conn:
        rows = conn.execute("SELECT key, value FROM site_settings").fetchall()
    for row in rows:
        if row["key"] in settings:
            settings[row["key"]] = row["value"]
    return settings


class SafeTemplateValues(dict):
    # Return the original {key} literal for any unknown placeholder instead of
    # raising KeyError, so templates with unrecognised variables render safely.
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def render_message_template(template: str, values: dict[str, object]) -> str:
    try:
        return template.format_map(SafeTemplateValues({key: str(value) for key, value in values.items()}))
    except ValueError:
        return template


def send_telegram_message(settings: dict[str, str], text: str) -> None:
    token = settings.get("telegram_bot_token", "").strip()
    chat_id = settings.get("telegram_chat_id", "").strip()
    if not token or not chat_id:
        raise AppError("tg_config_missing")
    payload = urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "false",
        }
    ).encode("utf-8")
    req = Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": "StreamHall/1.0",
        },
    )
    try:
        with urlopen(req, timeout=TELEGRAM_TIMEOUT) as resp:
            raw = resp.read(65536)
    except HTTPError as exc:
        raw = exc.read(65536)
        try:
            data = json.loads(raw.decode("utf-8"))
            description = str(data.get("description") or f"TG API HTTP {exc.code}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            description = f"TG API HTTP {exc.code}"
        raise AppError("tg_api_error", detail=description) from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AppError("tg_api_invalid") from exc
    if not data.get("ok"):
        description = str(data.get("description") or "")
        raise AppError("tg_api_error", detail=description)


def find_stream(conn, stream_ref: object) -> dict[str, object] | None:
    ref = str(stream_ref or "").strip()
    if not ref:
        return None
    return conn.execute("SELECT * FROM streams WHERE public_id = ?", (ref,)).fetchone()


def verify_admin_password(password: str) -> bool:
    with db() as conn:
        stored_hash = get_setting(conn, "admin_password_hash")
    return bool(stored_hash) and verify_admin_password_hash(password, stored_hash)


def client_ip_hash(headers: object) -> str:
    # Hash the client IP with HMAC-SHA256 so unique visitors can be counted
    # without storing the raw IP address in the database.
    raw = headers.get("CF-Connecting-IP") or headers.get("X-Forwarded-For") or headers.get("X-Real-IP") or ""
    ip = str(raw).split(",", 1)[0].strip()
    if not ip:
        return ""
    return hmac.new(SECRET_KEY, ip.encode("utf-8"), hashlib.sha256).hexdigest()


def detect_device_type(user_agent: str) -> str:
    ua = user_agent.lower()
    if any(item in ua for item in ("mobile", "iphone", "android")):
        return "mobile"
    if any(item in ua for item in ("ipad", "tablet")):
        return "tablet"
    return "desktop"


def client_ip(headers: object) -> str:
    raw = headers.get("CF-Connecting-IP") or headers.get("X-Forwarded-For") or headers.get("X-Real-IP") or ""
    return str(raw).split(",", 1)[0].strip()


def detect_os(user_agent: str) -> str:
    ua = user_agent.lower()
    if "windows" in ua:
        return "Windows"
    if "iphone" in ua or "ipad" in ua:
        return "iOS"
    if "android" in ua:
        return "Android"
    if "mac os" in ua or "macintosh" in ua:
        return "macOS"
    if "linux" in ua:
        return "Linux"
    return "Other"


def detect_browser(user_agent: str) -> str:
    ua = user_agent.lower()
    if "edg/" in ua or "edge/" in ua:
        return "Edge"
    if "firefox/" in ua or "fxios/" in ua:
        return "Firefox"
    if "opr/" in ua or "opera/" in ua:
        return "Opera"
    if "chrome/" in ua or "crios/" in ua or "chromium/" in ua:
        return "Chrome"
    if "safari/" in ua:
        return "Safari"
    return "Other"


_geo_cache: dict[str, dict] = {}
_geo_cache_time: dict[str, float] = {}
_GEO_TTL = 21600  # 6-hour in-process cache to stay within ip-api.com free-tier rate limits


def batch_geoip(ips: list[str]) -> dict[str, dict]:
    # Resolve IPs to country/region/city via ip-api.com batch endpoint (max 100 per call).
    # Results are cached in memory for _GEO_TTL seconds. If the lookup fails, the
    # affected IPs silently get an empty geo dict so stats still render.
    result: dict[str, dict] = {}
    to_fetch: list[str] = []
    t = time.time()
    for ip in ips:
        if not ip:
            continue
        if ip in _geo_cache and t - _geo_cache_time.get(ip, 0) < _GEO_TTL:
            result[ip] = _geo_cache[ip]
        else:
            to_fetch.append(ip)
    if to_fetch:
        try:
            payload = json.dumps([
                {"query": ip, "fields": "status,countryCode,country,regionName,city"}
                for ip in to_fetch[:100]
            ]).encode("utf-8")
            req = Request(
                "http://ip-api.com/batch",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(req, timeout=4) as resp:
                rows = json.loads(resp.read())
            t2 = time.time()
            for ip, row in zip(to_fetch, rows):
                geo: dict[str, str] = {}
                if isinstance(row, dict) and row.get("status") == "success":
                    geo = {
                        "countryCode": str(row.get("countryCode", "") or ""),
                        "country":     str(row.get("country", "") or ""),
                        "region":      str(row.get("regionName", "") or ""),
                        "city":        str(row.get("city", "") or ""),
                    }
                _geo_cache[ip] = geo
                _geo_cache_time[ip] = t2
                result[ip] = geo
        except Exception:
            pass
    for ip in ips:
        if ip and ip not in result:
            result[ip] = {}
    return result


def stream_public(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": row["public_id"],
        "stream_label": normalize_stream_label(row["stream_label"]),
        "event_name": row["event_name"],
        "has_password": 1 if row["stream_password"] else 0,
    }


def stream_admin(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": row["id"],
        "public_id": row["public_id"],
        "stream_label": normalize_stream_label(row["stream_label"]),
        "event_name": row["event_name"],
        "stream_password": row["stream_password"],
        "links_json": row["links_json"],
        "is_hidden": row["is_hidden"],
        "is_enabled": row["is_enabled"],
        "tg_notify_enabled": row["tg_notify_enabled"],
        "sort_order": row["sort_order"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def obs_route_public(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": row["id"],
        "stream_key": row["stream_key"],
        "public_slug": row["public_slug"],
        "created_at": row["created_at"],
    }


def player_data(row: dict[str, object], settings: dict[str, str] | None = None) -> dict[str, object]:
    try:
        links = json.loads(row["links_json"] or "[]")
    except json.JSONDecodeError:
        links = []
    site = settings or site_settings()
    return {
        "eventName": row["event_name"],
        "siteTitle": site["site_title"],
        "siteIconUrl": site.get("site_icon_url", ""),
        "links": add_playback_urls(normalize_links(links)),
    }


def probe_stream_links(row: dict[str, object]) -> dict[str, object]:
    try:
        links = normalize_links(json.loads(row["links_json"] or "[]"))
    except json.JSONDecodeError:
        links = []
    for index, link in enumerate(links):
        result = probe_stream_url(link["url"], link.get("type", ""))
        if result["valid"]:
            return {
                **result,
                "index": index,
                "url": link["url"],
                "link_name": link["name"],
            }
    return stream_probe_response(False)


def player_url_for_row(
    row: dict[str, object],
    headers: object | None = None,
    tg_settings: dict[str, str] | None = None,
) -> str:
    path = f"/player.html?id={quote(str(row['public_id']))}"
    if headers is not None:
        host = headers.get("X-Forwarded-Host") or headers.get("Host") or ""
        proto = headers.get("X-Forwarded-Proto") or "http"
        if host:
            return f"{proto}://{host}{path}"
    settings = tg_settings or telegram_settings()
    base_url = settings.get("telegram_public_base_url", "").strip().rstrip("/")
    return f"{base_url}{path}" if base_url else path


def notification_context_for_row(
    row: dict[str, object],
    probe_result: dict[str, object],
    status: str,
    headers: object | None = None,
    tg_settings: dict[str, str] | None = None,
) -> dict[str, object]:
    site = site_settings()
    settings = tg_settings or telegram_settings()
    return {
        "title": row["event_name"],
        "site_title": site["site_title"],
        "url": player_url_for_row(row, headers, settings),
        "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now())),
        "status": status,
        "stream_id": row["id"],
        "public_id": row["public_id"],
        "link_name": probe_result.get("link_name", ""),
        "source_url": probe_result.get("url", ""),
    }


def maybe_notify_stream_transition(
    row: dict[str, object],
    is_live: bool,
    probe_result: dict[str, object],
    headers: object | None = None,
) -> None:
    stream_id = int(row["id"])
    with db() as conn:
        previous = conn.execute(
            "SELECT is_live FROM stream_probe_states WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        previous_live = None if previous is None else bool(previous["is_live"])
        conn.execute(
            """
            INSERT INTO stream_probe_states (stream_id, is_live, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(stream_id) DO UPDATE SET
                is_live = excluded.is_live,
                updated_at = excluded.updated_at
            """,
            (stream_id, 1 if is_live else 0, now()),
        )
    if previous_live == is_live:
        return
    if previous_live is None and not is_live:
        return
    if int(row["tg_notify_enabled"] or 0) != 1:
        return

    settings = telegram_settings()
    label = normalize_stream_label(row.get("stream_label", "LIVE")).lower()
    if is_live:
        notify_key   = f"telegram_{label}_notify_start"
        template_key = f"telegram_{label}_start_template"
        status = "start"
    else:
        notify_key   = f"telegram_{label}_notify_stop"
        template_key = f"telegram_{label}_stop_template"
        status = "stop"
    if settings.get(notify_key, "0") != "1":
        return
    template = settings.get(template_key, "")

    text = render_message_template(
        template,
        notification_context_for_row(row, probe_result, status, headers, settings),
    )
    try:
        send_telegram_message(settings, text)
    except Exception as exc:
        print(f"Telegram notification failed for stream {stream_id}: {exc}")


def notify_current_live_if_needed(stream_id: int, headers: object | None = None) -> None:
    try:
        with db() as conn:
            row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        if not row:
            return
        if int(row["is_enabled"] or 0) != 1 or int(row["tg_notify_enabled"] or 0) != 1:
            return
        result = probe_stream_links(row)
        if not result.get("valid"):
            return
        settings = telegram_settings()
        label = normalize_stream_label(row.get("stream_label", "LIVE")).lower()
        if settings.get(f"telegram_{label}_notify_start", "0") != "1":
            return
        text = render_message_template(
            settings.get(f"telegram_{label}_start_template", ""),
            notification_context_for_row(row, result, "start", headers, settings),
        )
        send_telegram_message(settings, text)
    except Exception as exc:
        print(f"Telegram current live notification failed for stream {stream_id}: {exc}")


def check_stream_row_live(
    row: dict[str, object],
    headers: object | None = None,
    notify: bool = True,
) -> dict[str, object]:
    if int(row["is_enabled"] or 0) != 1:
        result = stream_probe_response(False)
        result["code"] = "closed"
        if notify:
            maybe_notify_stream_transition(row, False, result, headers)
        return result
    result = probe_stream_links(row)
    if notify:
        maybe_notify_stream_transition(row, bool(result.get("valid")), result, headers)
    return result


def rewrite_hls_manifest(manifest: str, slug: str, stream_key: str) -> str:
    # Rewrite an SRS-generated HLS manifest so all segment URLs point to the
    # public /h/<slug>/ proxy path. This hides the real stream key from clients
    # while the server can still reverse-map slug -> key when proxying segments.
    rewritten: list[str] = []
    base = f"/h/{quote(slug, safe='')}/"
    for line in manifest.splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            rewritten.append(line)
            continue
        if text.startswith(("http://", "https://")):
            rewritten.append(line)
            continue
        parsed = urlparse(text)
        segment = parsed.path.lstrip("/")
        if segment.startswith("live/"):
            segment = segment.split("/", 1)[1]
        if segment.startswith(stream_key):
            segment = f"{slug}{segment[len(stream_key):]}"
        query = f"?{parsed.query}" if parsed.query else ""
        rewritten.append(f"{base}{quote(segment, safe=URL_PATH_SAFE)}{query}")
    return "\n".join(rewritten) + ("\n" if manifest.endswith("\n") else "")


def rewrite_external_hls_manifest(manifest: str, base_url: str) -> str:
    # Rewrite all URLs in an external HLS manifest (segment lines and URI="..."
    # attributes such as EXT-X-KEY) to route through the signed /proxy/hls/
    # endpoint, enabling cross-origin playback and key override in the player.
    def proxied_uri(value: str) -> str:
        if not value or value.startswith("data:"):
            return value
        return hls_proxy_path(urljoin(base_url, value))

    rewritten: list[str] = []
    for line in manifest.splitlines():
        text = line.strip()
        if not text:
            rewritten.append(line)
            continue
        if text.startswith("#"):
            rewritten.append(HLS_URI_RE.sub(lambda match: f'URI="{proxied_uri(match.group(1))}"', line))
            continue
        rewritten.append(proxied_uri(text))
    return "\n".join(rewritten) + ("\n" if manifest.endswith("\n") else "")


def monitor_streams_loop() -> None:
    while True:
        try:
            with db() as conn:
                rows = conn.execute("SELECT * FROM streams ORDER BY id DESC").fetchall()
            for row in rows:
                check_stream_row_live(row)
        except Exception as exc:
            print(f"Stream monitor failed: {exc}")
        time.sleep(STREAM_MONITOR_INTERVAL)


class StreamHallHandler(BaseHTTPRequestHandler):
    server_version = "StreamHall/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.address_string()} - {fmt % args}")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.rstrip("/") == "/api":
            self.handle_api(parsed)
            return
        if parsed.path.startswith("/h/"):
            self.proxy_obs_route(parsed.path, parsed.query, send_body=True)
            return
        if parsed.path.startswith(f"{HLS_PROXY_PREFIX}/"):
            self.proxy_hls_route(parsed.path, send_body=True)
            return
        if parsed.path.startswith("/video/"):
            self.serve_video_file(parsed.path, send_body=True)
            return
        self.serve_static(parsed.path)

    def do_HEAD(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.rstrip("/") == "/api":
            self.send_error(HTTPStatus.METHOD_NOT_ALLOWED)
            return
        if parsed.path.startswith("/h/"):
            self.proxy_obs_route(parsed.path, parsed.query, send_body=False)
            return
        if parsed.path.startswith(f"{HLS_PROXY_PREFIX}/"):
            self.proxy_hls_route(parsed.path, send_body=False)
            return
        if parsed.path.startswith("/video/"):
            self.serve_video_file(parsed.path, send_body=False)
            return
        self.serve_static(parsed.path, send_body=False)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.rstrip("/") == "/api":
            self.handle_api(parsed)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def is_admin(self) -> bool:
        cookie = SimpleCookie(self.headers.get("Cookie"))
        morsel = cookie.get(SESSION_COOKIE)
        if verify_session(morsel.value if morsel else None):
            return True
        return self._verify_api_key()

    def _verify_api_key(self) -> bool:
        token = None
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:].strip()
        if not token:
            qs = parse_qs(urlparse(self.path).query)
            token = qs.get("api_key", [""])[0]
        if not token:
            return False
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        try:
            with db() as conn:
                row = conn.execute(
                    "SELECT id FROM api_keys WHERE token_hash = ?", (token_hash,)
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE api_keys SET last_used_at = ? WHERE id = ?",
                        (now(), row["id"]),
                    )
                    return True
        except Exception:
            pass
        return False

    def read_json(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if not length:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def send_json(
        self,
        payload: dict[str, object],
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Content-Type-Options", "nosniff")
        for key, value in (extra_headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def error_json(self, code: str, status: HTTPStatus = HTTPStatus.BAD_REQUEST, *, detail: str = "") -> None:
        payload: dict[str, object] = {"status": "error", "code": code}
        if detail:
            payload["detail"] = detail
        self.send_json(payload, status)

    def handle_api(self, parsed) -> None:
        action = parse_qs(parsed.query).get("action", [""])[0]
        try:
            if action == "site_settings":
                self.api_site_settings()
            elif action == "public_list":
                self.api_public_list()
            elif action == "get_player_data":
                self.api_get_player_data(parse_qs(parsed.query))
            elif action == "verify_password":
                self.api_verify_password()
            elif action == "check_player_stream":
                self.api_check_player_stream()
            elif action == "viewer_start":
                self.api_viewer_start()
            elif action == "viewer_heartbeat":
                self.api_viewer_heartbeat()
            elif action == "viewer_end":
                self.api_viewer_end()
            elif action == "login":
                self.api_login()
            elif action == "logout":
                self.api_logout()
            elif action == "session":
                self.send_json({"status": "success", "logged_in": self.is_admin()})
            elif action == "list_admin":
                self.require_admin()
                self.api_list_admin()
            elif action == "stream_stats_summary":
                self.require_admin()
                self.api_stream_stats_summary()
            elif action == "add":
                self.require_admin()
                self.api_add()
            elif action == "update":
                self.require_admin()
                self.api_update()
            elif action == "delete":
                self.require_admin()
                self.api_delete()
            elif action == "set_stream_enabled":
                self.require_admin()
                self.api_set_stream_enabled()
            elif action == "set_stream_tg_notify":
                self.require_admin()
                self.api_set_stream_tg_notify()
            elif action == "reorder_streams":
                self.require_admin()
                self.api_reorder_streams()
            elif action == "update_site_settings":
                self.require_admin()
                self.api_update_site_settings()
            elif action == "update_admin_password":
                self.require_admin()
                self.api_update_admin_password()
            elif action == "telegram_settings":
                self.require_admin()
                self.api_telegram_settings()
            elif action == "update_telegram_settings":
                self.require_admin()
                self.api_update_telegram_settings()
            elif action == "test_telegram":
                self.require_admin()
                self.api_test_telegram()
            elif action == "check_stream_url":
                self.require_admin()
                self.api_check_stream_url()
            elif action == "check_stream":
                self.require_admin()
                self.api_check_stream()
            elif action == "get_obs_config":
                self.require_admin()
                self.api_get_obs_config()
            elif action == "save_obs_config":
                self.require_admin()
                self.api_save_obs_config()
            elif action == "list_obs_routes":
                self.require_admin()
                self.api_list_obs_routes()
            elif action == "add_obs_route":
                self.require_admin()
                self.api_add_obs_route()
            elif action == "delete_obs_route":
                self.require_admin()
                self.api_delete_obs_route()
            elif action == "stats_overview":
                self.require_admin()
                self.api_stats_overview()
            elif action == "stats_streams":
                self.require_admin()
                self.api_stats_streams()
            elif action == "stats_timeseries":
                self.require_admin()
                self.api_stats_timeseries()
            elif action == "stats_export_csv":
                self.require_admin()
                self.api_stats_export_csv()
            elif action == "stats_stream_detail":
                self.require_admin()
                self.api_stats_stream_detail()
            elif action == "stats_dashboard_realtime":
                self.require_admin()
                self.api_stats_dashboard_realtime()
            elif action == "stats_stream_realtime":
                self.require_admin()
                self.api_stats_stream_realtime()
            elif action == "stats_geo":
                self.require_admin()
                self.api_stats_geo()
            elif action == "stats_sessions_page":
                self.require_admin()
                self.api_stats_sessions_page()
            elif action == "list_api_keys":
                self.require_admin()
                self.api_list_api_keys()
            elif action == "create_api_key":
                self.require_admin()
                self.api_create_api_key()
            elif action == "delete_api_key":
                self.require_admin()
                self.api_delete_api_key()
            elif action == "list_videos":
                self.require_admin()
                self.api_list_videos()
            elif action == "list_folder_videos":
                self.require_admin()
                self.api_list_folder_videos()
            elif action == "upload_video":
                self.require_admin()
                self.api_upload_video()
            elif action == "delete_video":
                self.require_admin()
                self.api_delete_video()
            elif action == "start_push":
                self.require_admin()
                self.api_start_push()
            elif action == "stop_push":
                self.require_admin()
                self.api_stop_push()
            elif action == "list_pushes":
                self.require_admin()
                self.api_list_pushes()
            else:
                self.error_json("invalid_action")
        except PermissionError:
            self.error_json("auth_required", HTTPStatus.UNAUTHORIZED)
        except AppError as exc:
            self.error_json(exc.code, detail=exc.detail)
        except ValueError as exc:
            self.error_json(str(exc))
        except Exception as exc:
            self.error_json("server_error", HTTPStatus.INTERNAL_SERVER_ERROR, detail=str(exc))

    def require_admin(self) -> None:
        if not self.is_admin():
            raise PermissionError()

    def player_url(self, row: dict[str, object]) -> str:
        return player_url_for_row(row, self.headers)

    def request_origin(self) -> str:
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or ""
        proto = self.headers.get("X-Forwarded-Proto") or "http"
        return f"{proto}://{host}" if host else ""

    def notification_context(self, row: dict[str, object], probe_result: dict[str, object], status: str) -> dict[str, object]:
        return notification_context_for_row(row, probe_result, status, self.headers)

    def maybe_notify_stream_transition(self, row: dict[str, object], is_live: bool, probe_result: dict[str, object]) -> None:
        maybe_notify_stream_transition(row, is_live, probe_result, self.headers)

    def check_stream_row(self, row: dict[str, object], notify: bool = True) -> dict[str, object]:
        return check_stream_row_live(row, self.headers, notify)

    def api_site_settings(self) -> None:
        settings = site_settings()
        origin = self.request_origin()
        # Auto-populate telegram_public_base_url from the request origin the
        # first time the frontend loads, so Telegram notification links resolve
        # to the correct host without requiring manual configuration.
        if origin and not settings.get("telegram_public_base_url"):
            settings["telegram_public_base_url"] = origin
            with db() as conn:
                conn.execute(
                    """
                    INSERT INTO site_settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    ("telegram_public_base_url", origin),
                )
        self.send_json({"status": "success", "data": settings})

    def api_public_list(self) -> None:
        with db() as conn:
            rows = conn.execute(
                "SELECT id, public_id, stream_label, event_name, stream_password FROM streams "
                "WHERE is_hidden = 0 AND is_enabled = 1 "
                "ORDER BY CASE stream_label WHEN 'LIVE' THEN 0 WHEN 'ARCHIVE' THEN 1 ELSE 2 END ASC, "
                "sort_order ASC, id DESC"
            ).fetchall()
        self.send_json({"status": "success", "data": [stream_public(row) for row in rows]})

    def api_get_player_data(self, query: dict[str, list[str]]) -> None:
        stream_ref = query.get("id", [""])[0]
        with db() as conn:
            row = find_stream(conn, stream_ref)
        if not row:
            raise AppError("stream_not_found")
        if int(row["is_enabled"] or 0) != 1:
            raise AppError("stream_disabled")
        settings = site_settings()
        if row["stream_password"]:
            self.send_json(
                {
                    "status": "success",
                    "data": {
                        "requires_password": True,
                        "eventName": row["event_name"],
                        "siteTitle": settings["site_title"],
                        "siteIconUrl": settings.get("site_icon_url", ""),
                    },
                }
            )
            return
        self.send_json({"status": "success", "data": player_data(row, settings)})

    def api_verify_password(self) -> None:
        body = self.read_json()
        password = str(body.get("password", ""))
        with db() as conn:
            row = find_stream(conn, body.get("id", ""))
        if not row:
            raise AppError("stream_not_found")
        if int(row["is_enabled"] or 0) != 1:
            raise AppError("stream_disabled")
        if row["stream_password"] and not hmac.compare_digest(row["stream_password"], password):
            raise AppError("auth_incorrect_password")
        self.send_json({"status": "success", "data": player_data(row)})

    def api_check_player_stream(self) -> None:
        body = self.read_json()
        password = str(body.get("password", ""))
        with db() as conn:
            row = find_stream(conn, body.get("id", ""))
        if not row:
            raise AppError("stream_not_found")
        if row["stream_password"] and not hmac.compare_digest(row["stream_password"], password):
            raise AppError("auth_incorrect_password")
        self.send_json({"status": "success", "data": self.check_stream_row(row)})

    def api_viewer_start(self) -> None:
        body = self.read_json()
        visitor_id = str(body.get("visitorId", "")).strip()[:120] or secrets.token_urlsafe(18)
        stream_ref = body.get("id", "")
        referer = str(body.get("referer", self.headers.get("Referer", ""))).strip()[:500]
        with db() as conn:
            row = find_stream(conn, stream_ref)
            if not row or int(row["is_enabled"] or 0) != 1:
                raise AppError("stream_not_found_or_disabled")
            session_id = secrets.token_urlsafe(18)
            timestamp = now()
            conn.execute(
                """
                INSERT INTO viewer_sessions
                    (session_id, visitor_id, stream_id, public_id, ip_hash, user_agent, referer,
                     device_type, ip_address, browser, os, started_at, last_seen_at, is_active, play_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    session_id,
                    visitor_id,
                    row["id"],
                    row["public_id"],
                    client_ip_hash(self.headers),
                    self.headers.get("User-Agent", "")[:800],
                    referer,
                    detect_device_type(self.headers.get("User-Agent", "")),
                    client_ip(self.headers),
                    detect_browser(self.headers.get("User-Agent", "")),
                    detect_os(self.headers.get("User-Agent", "")),
                    timestamp,
                    timestamp,
                    str(body.get("state", "viewing"))[:40],
                ),
            )
            conn.execute(
                "INSERT INTO viewer_events (session_id, stream_id, event_type, event_at, metadata) VALUES (?, ?, ?, ?, ?)",
                (session_id, row["id"], "start", timestamp, "{}"),
            )
        self.send_json({"status": "success", "data": {"sessionId": session_id, "visitorId": visitor_id}})

    def api_viewer_heartbeat(self) -> None:
        body = self.read_json()
        session_id = str(body.get("sessionId", "")).strip()
        if not session_id:
            raise AppError("missing_session_id")
        timestamp = now()
        with db() as conn:
            cur = conn.execute(
                "UPDATE viewer_sessions SET last_seen_at = ?, is_active = 1, play_state = ? WHERE session_id = ?",
                (timestamp, str(body.get("state", "viewing"))[:40], session_id),
            )
        if cur.rowcount == 0:
            raise AppError("viewer_session_not_found")
        self.send_json({"status": "success"})

    def api_viewer_end(self) -> None:
        body = self.read_json()
        session_id = str(body.get("sessionId", "")).strip()
        if not session_id:
            self.send_json({"status": "success"})
            return
        timestamp = now()
        with db() as conn:
            row = conn.execute("SELECT stream_id FROM viewer_sessions WHERE session_id = ?", (session_id,)).fetchone()
            conn.execute(
                "UPDATE viewer_sessions SET last_seen_at = ?, ended_at = ?, is_active = 0, play_state = ? WHERE session_id = ?",
                (timestamp, timestamp, str(body.get("state", "ended"))[:40], session_id),
            )
            if row:
                conn.execute(
                    "INSERT INTO viewer_events (session_id, stream_id, event_type, event_at, metadata) VALUES (?, ?, ?, ?, ?)",
                    (session_id, row["stream_id"], "end", timestamp, "{}"),
                )
        self.send_json({"status": "success"})

    def api_login(self) -> None:
        body = self.read_json()
        password = str(body.get("password", "")).strip()
        if not verify_admin_password(password):
            raise AppError("auth_incorrect_password")
        token = make_session()
        cookie = (
            f"{SESSION_COOKIE}={token}; Path=/; Max-Age={SESSION_MAX_AGE}; "
            "HttpOnly; SameSite=Lax"
        )
        self.send_json({"status": "success"}, extra_headers={"Set-Cookie": cookie})

    def api_logout(self) -> None:
        cookie = f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
        self.send_json({"status": "success"}, extra_headers={"Set-Cookie": cookie})

    def api_update_admin_password(self) -> None:
        body = self.read_json()
        current_password = str(body.get("currentPassword", "")).strip()
        new_password = str(body.get("newPassword", "")).strip()
        confirm_password = str(body.get("confirmPassword", "")).strip()
        if not verify_admin_password(current_password):
            raise AppError("auth_current_pw_wrong")
        if len(new_password) < 10:
            raise AppError("auth_pw_too_short")
        if new_password != confirm_password:
            raise AppError("auth_pw_mismatch")
        with db() as conn:
            set_setting(conn, "admin_password_hash", hash_admin_password(new_password))
            set_setting(conn, "admin_password_changed_at", str(now()))
        cookie = f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"
        self.send_json({"status": "success"}, extra_headers={"Set-Cookie": cookie})

    def api_list_admin(self) -> None:
        with db() as conn:
            rows = conn.execute(
                "SELECT * FROM streams ORDER BY "
                "CASE stream_label WHEN 'LIVE' THEN 0 WHEN 'ARCHIVE' THEN 1 ELSE 2 END ASC, "
                "sort_order ASC, id DESC"
            ).fetchall()
        self.send_json({"status": "success", "data": [stream_admin(row) for row in rows]})

    def api_stream_stats_summary(self) -> None:
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime((local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)))
        with db() as conn:
            rows = conn.execute(
                """
                SELECT
                    stream_id,
                    SUM(CASE WHEN is_active = 1 AND last_seen_at >= ? THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN started_at >= ? THEN 1 ELSE 0 END) AS today_views,
                    COUNT(*) AS total_views,
                    MAX(last_seen_at) AS last_seen_at
                FROM viewer_sessions
                GROUP BY stream_id
                """,
                (online_after, today_start),
            ).fetchall()
        data = {
            str(row["stream_id"]): {
                "online": int(row["online"] or 0),
                "today_views": int(row["today_views"] or 0),
                "total_views": int(row["total_views"] or 0),
                "last_seen_at": int(row["last_seen_at"] or 0),
            }
            for row in rows
        }
        self.send_json({"status": "success", "data": data})

    def api_add(self) -> None:
        body = self.read_json()
        event_name = str(body.get("eventName", "")).strip()
        if not event_name:
            raise AppError("stream_name_empty")
        links = normalize_links(body.get("links", []))
        stream_label = normalize_stream_label(body.get("streamLabel"))
        with db() as conn:
            max_order = conn.execute(
                "SELECT COALESCE(MAX(sort_order), 0) AS v FROM streams WHERE stream_label = ?",
                (stream_label,),
            ).fetchone()["v"]
            params = (
                generate_public_id(conn),
                stream_label,
                event_name,
                str(body.get("streamPassword", "")).strip(),
                json.dumps(links, ensure_ascii=False),
                1 if body.get("isHidden") else 0,
                int(max_order) + 1,
                now(),
                now(),
            )
            row = conn.execute(
                """
                INSERT INTO streams
                    (public_id, stream_label, event_name, stream_password, links_json, is_hidden, sort_order, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                params,
            ).fetchone()
            stream_id = row["id"]
        self.send_json({"status": "success", "id": stream_id})

    def api_update(self) -> None:
        body = self.read_json()
        stream_id = int(body.get("id", 0) or 0)
        event_name = str(body.get("eventName", "")).strip()
        if not stream_id:
            raise AppError("missing_stream_id")
        if not event_name:
            raise AppError("stream_name_empty")
        links = normalize_links(body.get("links", []))
        stream_label = normalize_stream_label(body.get("streamLabel"))
        with db() as conn:
            cur = conn.execute(
                """
                UPDATE streams
                SET stream_label = ?, event_name = ?, stream_password = ?, links_json = ?, is_hidden = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    stream_label,
                    event_name,
                    str(body.get("streamPassword", "")).strip(),
                    json.dumps(links, ensure_ascii=False),
                    1 if body.get("isHidden") else 0,
                    now(),
                    stream_id,
                ),
            )
        if cur.rowcount == 0:
            raise AppError("stream_not_found")
        self.send_json({"status": "success"})

    def api_delete(self) -> None:
        body = self.read_json()
        stream_id = int(body.get("id", 0) or 0)
        if not stream_id:
            raise AppError("missing_stream_id")
        with db() as conn:
            conn.execute("DELETE FROM streams WHERE id = ?", (stream_id,))
            conn.execute("DELETE FROM stream_probe_states WHERE stream_id = ?", (stream_id,))
        self.send_json({"status": "success"})

    def api_set_stream_enabled(self) -> None:
        body = self.read_json()
        stream_id = int(body.get("id", 0) or 0)
        enabled = 1 if body.get("enabled") else 0
        if not stream_id:
            raise AppError("missing_stream_id")
        with db() as conn:
            cur = conn.execute(
                "UPDATE streams SET is_enabled = ?, updated_at = ? WHERE id = ?",
                (enabled, now(), stream_id),
            )
            row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        if cur.rowcount == 0 or not row:
            raise AppError("stream_not_found")
        result = None if enabled else stream_probe_response(False)
        if not enabled:
            result["code"] = "closed"
            self.maybe_notify_stream_transition(row, False, result)
        self.send_json({"status": "success", "enabled": enabled, "data": result})

    def api_set_stream_tg_notify(self) -> None:
        body = self.read_json()
        stream_id = int(body.get("id", 0) or 0)
        enabled = 1 if body.get("enabled") else 0
        if not stream_id:
            raise AppError("missing_stream_id")
        with db() as conn:
            cur = conn.execute(
                "UPDATE streams SET tg_notify_enabled = ?, updated_at = ? WHERE id = ?",
                (enabled, now(), stream_id),
            )
            row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        if cur.rowcount == 0:
            raise AppError("stream_not_found")
        if enabled and row:
            headers = dict(self.headers)
            threading.Thread(
                target=notify_current_live_if_needed,
                args=(stream_id, headers),
                daemon=True,
            ).start()
        self.send_json({"status": "success", "enabled": enabled})

    def api_reorder_streams(self) -> None:
        body = self.read_json()
        ids = [int(x) for x in body.get("ids", [])]
        label = str(body.get("label", "")).strip()
        if not ids or not label:
            raise AppError("obs_ids_required")
        with db() as conn:
            for order, stream_id in enumerate(ids):
                conn.execute(
                    "UPDATE streams SET sort_order = ?, updated_at = ? WHERE id = ? AND stream_label = ?",
                    (order, now(), stream_id, label),
                )
        self.send_json({"status": "success"})

    def api_update_site_settings(self) -> None:
        settings = normalize_site_settings(self.read_json())
        with db() as conn:
            for key, value in settings.items():
                conn.execute(
                    """
                    INSERT INTO site_settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, value),
                )
        self.send_json({"status": "success", "data": settings})

    def api_telegram_settings(self) -> None:
        settings = telegram_settings()
        origin = self.request_origin()
        if origin and not settings.get("telegram_public_base_url"):
            settings["telegram_public_base_url"] = origin
            with db() as conn:
                conn.execute(
                    """
                    INSERT INTO site_settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    ("telegram_public_base_url", origin),
                )
        self.send_json({"status": "success", "data": settings})

    def api_update_telegram_settings(self) -> None:
        settings = normalize_telegram_settings(self.read_json())
        with db() as conn:
            for key, value in settings.items():
                conn.execute(
                    """
                    INSERT INTO site_settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, value),
                )
        self.send_json({"status": "success", "data": settings})

    def api_test_telegram(self) -> None:
        settings = normalize_telegram_settings(self.read_json())
        if not settings["telegram_bot_token"] or not settings["telegram_chat_id"]:
            raise AppError("tg_config_missing")
        send_telegram_message(settings, "StreamHall Telegram test message")
        self.send_json({"status": "success", "message": "Test message sent"})

    def api_check_stream_url(self) -> None:
        body = self.read_json()
        result = probe_stream_url(body.get("url", ""), body.get("type", ""))
        self.send_json({"status": "success", "data": result})

    def api_check_stream(self) -> None:
        body = self.read_json()
        stream_id = int(body.get("id", 0) or 0)
        if not stream_id:
            raise AppError("missing_stream_id")
        with db() as conn:
            row = conn.execute("SELECT * FROM streams WHERE id = ?", (stream_id,)).fetchone()
        if not row:
            raise AppError("stream_not_found")
        self.send_json({"status": "success", "data": self.check_stream_row(row)})

    def api_get_obs_config(self) -> None:
        with db() as conn:
            rows = conn.execute(
                "SELECT key, value FROM site_settings WHERE key IN (?, ?)",
                ("obs_rtmp_host", "obs_playback_origin"),
            ).fetchall()
        data: dict[str, str] = {"obs_rtmp_host": "", "obs_playback_origin": ""}
        for row in rows:
            data[row["key"]] = row["value"]
        self.send_json({"status": "ok", "obs_rtmp_host": data["obs_rtmp_host"], "obs_playback_origin": data["obs_playback_origin"]})

    def api_save_obs_config(self) -> None:
        body = self.read_json()
        rtmp_host = str(body.get("obs_rtmp_host") or "").strip()
        playback_origin = str(body.get("obs_playback_origin") or "").strip()
        with db() as conn:
            for key, value in [("obs_rtmp_host", rtmp_host), ("obs_playback_origin", playback_origin)]:
                conn.execute(
                    "INSERT INTO site_settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )
        self.send_json({"status": "ok"})

    def api_list_obs_routes(self) -> None:
        with db() as conn:
            rows = conn.execute("SELECT * FROM obs_stream_routes ORDER BY id DESC").fetchall()
        self.send_json({"status": "success", "data": [obs_route_public(row) for row in rows]})

    def api_add_obs_route(self) -> None:
        body = self.read_json()
        stream_key = str(body.get("streamKey", "")).strip()
        if not stream_key:
            raise AppError("stream_key_empty")
        if len(stream_key) > 180:
            raise AppError("stream_key_too_long")
        slug = obs_route_slug(stream_key)
        with db() as conn:
            conn.execute(
                """
                INSERT INTO obs_stream_routes (stream_key, public_slug, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(stream_key) DO UPDATE SET public_slug = excluded.public_slug
                """,
                (stream_key, slug, now()),
            )
            row = conn.execute("SELECT * FROM obs_stream_routes WHERE stream_key = ?", (stream_key,)).fetchone()
        self.send_json({"status": "success", "data": obs_route_public(row)})

    def api_delete_obs_route(self) -> None:
        body = self.read_json()
        route_id = int(body.get("id", 0) or 0)
        if not route_id:
            raise AppError("missing_route_id")
        with db() as conn:
            cur = conn.execute("DELETE FROM obs_stream_routes WHERE id = ?", (route_id,))
        if cur.rowcount == 0:
            raise AppError("route_not_found")
        self.send_json({"status": "success"})

    def api_stats_overview(self) -> None:
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime((local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)))
        with db() as conn:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN is_active = 1 AND last_seen_at >= ? THEN 1 ELSE 0 END) AS total_online,
                    SUM(CASE WHEN started_at >= ? THEN 1 ELSE 0 END)                     AS today_views,
                    COUNT(*)                                                               AS total_views,
                    COUNT(DISTINCT visitor_id)                                             AS unique_visitors,
                    COUNT(DISTINCT stream_id)                                              AS streams_with_views
                FROM viewer_sessions
                """,
                (online_after, today_start),
            ).fetchone()
            device_rows = conn.execute(
                "SELECT device_type, COUNT(*) AS cnt FROM viewer_sessions GROUP BY device_type"
            ).fetchall()
        devices = {r["device_type"]: int(r["cnt"]) for r in device_rows}
        self.send_json({"status": "success", "data": {
            "total_online":       int(row["total_online"] or 0),
            "today_views":        int(row["today_views"] or 0),
            "total_views":        int(row["total_views"] or 0),
            "unique_visitors":    int(row["unique_visitors"] or 0),
            "streams_with_views": int(row["streams_with_views"] or 0),
            "devices":            devices,
        }})

    def api_stats_streams(self) -> None:
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime((local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)))
        with db() as conn:
            rows = conn.execute(
                """
                SELECT
                    vs.stream_id,
                    s.event_name,
                    SUM(CASE WHEN vs.is_active = 1 AND vs.last_seen_at >= ? THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN vs.started_at >= ? THEN 1 ELSE 0 END)                        AS today_views,
                    COUNT(*)                                                                     AS total_views,
                    COUNT(DISTINCT vs.visitor_id)                                                AS unique_visitors,
                    SUM(CASE WHEN vs.device_type = 'mobile'  THEN 1 ELSE 0 END)                 AS mobile,
                    SUM(CASE WHEN vs.device_type = 'tablet'  THEN 1 ELSE 0 END)                 AS tablet,
                    SUM(CASE WHEN vs.device_type = 'desktop' THEN 1 ELSE 0 END)                 AS desktop,
                    AVG(CASE WHEN vs.ended_at > 0 THEN vs.ended_at - vs.started_at END)         AS avg_duration,
                    MAX(vs.last_seen_at)                                                         AS last_seen_at
                FROM viewer_sessions vs
                LEFT JOIN streams s ON vs.stream_id = s.id
                GROUP BY vs.stream_id, s.event_name
                ORDER BY total_views DESC
                """,
                (online_after, today_start),
            ).fetchall()
        self.send_json({"status": "success", "data": [
            {
                "stream_id":       int(r["stream_id"]),
                "event_name":      str(r["event_name"] or ""),
                "online":          int(r["online"] or 0),
                "today_views":     int(r["today_views"] or 0),
                "total_views":     int(r["total_views"] or 0),
                "unique_visitors": int(r["unique_visitors"] or 0),
                "mobile":          int(r["mobile"] or 0),
                "tablet":          int(r["tablet"] or 0),
                "desktop":         int(r["desktop"] or 0),
                "avg_duration":    round(float(r["avg_duration"]), 1) if r["avg_duration"] else None,
                "last_seen_at":    int(r["last_seen_at"] or 0),
            }
            for r in rows
        ]})

    def api_stats_timeseries(self) -> None:
        range_param = parse_qs(urlparse(self.path).query).get("range", ["today"])[0]
        local_now = time.localtime(now())
        today_start = int(time.mktime((local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)))
        if range_param == "7d":
            bucket_secs = 86400
            ref_start = today_start - 6 * 86400
            num_buckets = 7
        elif range_param == "30d":
            bucket_secs = 86400
            ref_start = today_start - 29 * 86400
            num_buckets = 30
        else:
            range_param = "today"
            bucket_secs = 3600
            ref_start = today_start
            num_buckets = 24
        with db() as conn:
            rows = conn.execute(
                """
                SELECT (started_at - ?) / ? AS bucket, COUNT(*) AS views
                FROM viewer_sessions
                WHERE started_at >= ?
                GROUP BY bucket
                """,
                (ref_start, bucket_secs, ref_start),
            ).fetchall()
        buckets = [0] * num_buckets
        for r in rows:
            idx = int(r["bucket"])
            if 0 <= idx < num_buckets:
                buckets[idx] = int(r["views"])
        self.send_json({"status": "success", "data": {
            "range": range_param,
            "buckets": buckets,
            "ref_start": ref_start,
            "bucket_secs": bucket_secs,
        }})

    def api_stats_export_csv(self) -> None:
        with db() as conn:
            rows = conn.execute(
                """
                SELECT vs.session_id, vs.visitor_id, s.event_name,
                       vs.device_type, vs.referer,
                       vs.started_at, vs.ended_at, vs.last_seen_at,
                       vs.is_active, vs.play_state
                FROM viewer_sessions vs
                LEFT JOIN streams s ON vs.stream_id = s.id
                ORDER BY vs.started_at DESC
                """
            ).fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["session_id", "visitor_id", "event_name", "device_type", "referer",
                         "started_at", "ended_at", "last_seen_at", "is_active", "play_state"])
        for r in rows:
            writer.writerow([
                r["session_id"], r["visitor_id"], r["event_name"] or "",
                r["device_type"], r["referer"],
                r["started_at"], r["ended_at"], r["last_seen_at"],
                r["is_active"], r["play_state"],
            ])
        body = buf.getvalue().encode("utf-8-sig")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", 'attachment; filename="viewer_stats.csv"')
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def api_stats_stream_detail(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        stream_id = int(qs.get("id", [0])[0] or 0)
        range_param = qs.get("range", ["7d"])[0]
        if not stream_id:
            raise AppError("missing_stream_id")
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime((local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)))
        if range_param == "today":
            bucket_secs, ref_start, num_buckets = 3600, today_start, 24
        elif range_param == "30d":
            bucket_secs, ref_start, num_buckets = 86400, today_start - 29 * 86400, 30
        else:
            range_param = "7d"
            bucket_secs, ref_start, num_buckets = 86400, today_start - 6 * 86400, 7
        with db() as conn:
            summary = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN is_active = 1 AND last_seen_at >= ? THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN started_at >= ? THEN 1 ELSE 0 END)                     AS today_views,
                    COUNT(*)                                                               AS total_views,
                    COUNT(DISTINCT visitor_id)                                             AS unique_visitors,
                    SUM(CASE WHEN device_type = 'mobile'  THEN 1 ELSE 0 END)              AS mobile,
                    SUM(CASE WHEN device_type = 'tablet'  THEN 1 ELSE 0 END)              AS tablet,
                    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END)              AS desktop,
                    AVG(CASE WHEN ended_at > 0 THEN ended_at - started_at END)            AS avg_duration
                FROM viewer_sessions WHERE stream_id = ?
                """,
                (online_after, today_start, stream_id),
            ).fetchone()
            ts_rows = conn.execute(
                """
                SELECT (started_at - ?) / ? AS bucket, COUNT(*) AS views
                FROM viewer_sessions
                WHERE stream_id = ? AND started_at >= ?
                GROUP BY bucket
                """,
                (ref_start, bucket_secs, stream_id, ref_start),
            ).fetchall()
            recent = conn.execute(
                """
                SELECT started_at, ended_at, last_seen_at, device_type, play_state, is_active, ip_address, browser
                FROM viewer_sessions
                WHERE stream_id = ?
                ORDER BY started_at DESC LIMIT 20
                """,
                (stream_id,),
            ).fetchall()
        buckets = [0] * num_buckets
        for r in ts_rows:
            idx = int(r["bucket"])
            if 0 <= idx < num_buckets:
                buckets[idx] = int(r["views"])
        ips = [str(r["ip_address"] or "") for r in recent]
        geo_map = batch_geoip(ips)
        self.send_json({"status": "success", "data": {
            "summary": {
                "online":          int(summary["online"] or 0),
                "today_views":     int(summary["today_views"] or 0),
                "total_views":     int(summary["total_views"] or 0),
                "unique_visitors": int(summary["unique_visitors"] or 0),
                "mobile":          int(summary["mobile"] or 0),
                "tablet":          int(summary["tablet"] or 0),
                "desktop":         int(summary["desktop"] or 0),
                "avg_duration":    round(float(summary["avg_duration"]), 1) if summary["avg_duration"] else None,
            },
            "timeseries": {
                "range": range_param, "buckets": buckets,
                "ref_start": ref_start, "bucket_secs": bucket_secs,
            },
            "recent_sessions": [
                {
                    "started_at":    int(r["started_at"]),
                    "ended_at":      int(r["ended_at"] or 0),
                    "last_seen_at":  int(r["last_seen_at"] or 0),
                    "device_type":   str(r["device_type"] or ""),
                    "play_state":    str(r["play_state"] or ""),
                    "is_active":     int(r["is_active"]),
                    "ip_address":    str(r["ip_address"] or ""),
                    "browser":       str(r["browser"] or ""),
                    "geo":           geo_map.get(str(r["ip_address"] or ""), {}),
                }
                for r in recent
            ],
        }})

    def _stream_live_snapshot(self, stream_id: int) -> dict:
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime(
            (local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)
        ))
        with db() as conn:
            summary = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN is_active=1 AND last_seen_at >= ? THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN started_at >= ?                   THEN 1 ELSE 0 END) AS today_views,
                    COUNT(*)                                                             AS total_views,
                    COUNT(DISTINCT visitor_id)                                           AS unique_visitors,
                    SUM(CASE WHEN device_type='mobile'  THEN 1 ELSE 0 END)              AS mobile,
                    SUM(CASE WHEN device_type='tablet'  THEN 1 ELSE 0 END)              AS tablet,
                    SUM(CASE WHEN device_type='desktop' THEN 1 ELSE 0 END)              AS desktop,
                    AVG(CASE WHEN ended_at > 0 THEN ended_at - started_at END)          AS avg_duration
                FROM viewer_sessions WHERE stream_id = ?
                """,
                (online_after, today_start, stream_id),
            ).fetchone()
            browser_rows = conn.execute(
                "SELECT browser, COUNT(*) AS cnt FROM viewer_sessions WHERE stream_id = ? AND browser != '' GROUP BY browser ORDER BY cnt DESC LIMIT 6",
                (stream_id,),
            ).fetchall()
            os_rows = conn.execute(
                "SELECT os, COUNT(*) AS cnt FROM viewer_sessions WHERE stream_id = ? AND os != '' GROUP BY os ORDER BY cnt DESC LIMIT 6",
                (stream_id,),
            ).fetchall()
            recent = conn.execute(
                """
                SELECT started_at, ended_at, last_seen_at, device_type, play_state,
                       is_active, ip_address, browser
                FROM viewer_sessions WHERE stream_id = ?
                ORDER BY started_at DESC LIMIT 20
                """,
                (stream_id,),
            ).fetchall()
        ips = [str(r["ip_address"] or "") for r in recent]
        geo_map = batch_geoip(ips)
        return {
            "summary": {
                "online":          int(summary["online"] or 0),
                "today_views":     int(summary["today_views"] or 0),
                "total_views":     int(summary["total_views"] or 0),
                "unique_visitors": int(summary["unique_visitors"] or 0),
                "mobile":          int(summary["mobile"] or 0),
                "tablet":          int(summary["tablet"] or 0),
                "desktop":         int(summary["desktop"] or 0),
                "avg_duration":    round(float(summary["avg_duration"]), 1) if summary["avg_duration"] else None,
                "browsers":        [{"name": r["browser"], "cnt": int(r["cnt"])} for r in browser_rows],
                "oses":            [{"name": r["os"],      "cnt": int(r["cnt"])} for r in os_rows],
            },
            "recent_sessions": [
                {
                    "started_at":   int(r["started_at"]),
                    "ended_at":     int(r["ended_at"] or 0),
                    "last_seen_at": int(r["last_seen_at"] or 0),
                    "device_type":  str(r["device_type"] or ""),
                    "play_state":   str(r["play_state"] or ""),
                    "is_active":    int(r["is_active"]),
                    "ip_address":   str(r["ip_address"] or ""),
                    "browser":      str(r["browser"] or ""),
                    "geo":          geo_map.get(str(r["ip_address"] or ""), {}),
                }
                for r in recent
            ],
        }

    def _dashboard_snapshot(self) -> dict:
        online_after = now() - 45
        local_now = time.localtime(now())
        today_start = int(time.mktime(
            (local_now.tm_year, local_now.tm_mon, local_now.tm_mday, 0, 0, 0, 0, 0, -1)
        ))
        with db() as conn:
            ov = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN is_active = 1 AND last_seen_at >= ? THEN 1 ELSE 0 END) AS total_online,
                    SUM(CASE WHEN started_at >= ?                     THEN 1 ELSE 0 END) AS today_views,
                    COUNT(*)                                                               AS total_views,
                    COUNT(DISTINCT visitor_id)                                             AS unique_visitors,
                    COUNT(DISTINCT stream_id)                                              AS streams_with_views
                FROM viewer_sessions
                """,
                (online_after, today_start),
            ).fetchone()
            device_rows = conn.execute(
                "SELECT device_type, COUNT(*) AS cnt FROM viewer_sessions GROUP BY device_type"
            ).fetchall()
            browser_rows = conn.execute(
                "SELECT browser, COUNT(*) AS cnt FROM viewer_sessions WHERE browser != '' GROUP BY browser ORDER BY cnt DESC LIMIT 6"
            ).fetchall()
            os_rows = conn.execute(
                "SELECT os, COUNT(*) AS cnt FROM viewer_sessions WHERE os != '' GROUP BY os ORDER BY cnt DESC LIMIT 6"
            ).fetchall()
            stream_rows = conn.execute(
                """
                SELECT
                    vs.stream_id,
                    s.event_name,
                    SUM(CASE WHEN vs.is_active = 1 AND vs.last_seen_at >= ? THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN vs.started_at >= ?                        THEN 1 ELSE 0 END) AS today_views,
                    COUNT(*)                                                                     AS total_views,
                    COUNT(DISTINCT vs.visitor_id)                                                AS unique_visitors,
                    SUM(CASE WHEN vs.device_type = 'mobile'  THEN 1 ELSE 0 END)                 AS mobile,
                    SUM(CASE WHEN vs.device_type = 'tablet'  THEN 1 ELSE 0 END)                 AS tablet,
                    SUM(CASE WHEN vs.device_type = 'desktop' THEN 1 ELSE 0 END)                 AS desktop,
                    AVG(CASE WHEN vs.ended_at > 0 THEN vs.ended_at - vs.started_at END)         AS avg_duration,
                    MAX(vs.last_seen_at)                                                         AS last_seen_at
                FROM viewer_sessions vs
                LEFT JOIN streams s ON vs.stream_id = s.id
                GROUP BY vs.stream_id, s.event_name
                ORDER BY total_views DESC
                """,
                (online_after, today_start),
            ).fetchall()
        devices  = {r["device_type"]: int(r["cnt"]) for r in device_rows}
        browsers = [{"name": r["browser"], "cnt": int(r["cnt"])} for r in browser_rows]
        oses     = [{"name": r["os"],      "cnt": int(r["cnt"])} for r in os_rows]
        return {
            "overview": {
                "total_online":       int(ov["total_online"] or 0),
                "today_views":        int(ov["today_views"] or 0),
                "total_views":        int(ov["total_views"] or 0),
                "unique_visitors":    int(ov["unique_visitors"] or 0),
                "streams_with_views": int(ov["streams_with_views"] or 0),
                "devices":            devices,
                "browsers":           browsers,
                "oses":               oses,
            },
            "streams": [
                {
                    "stream_id":       int(r["stream_id"]),
                    "event_name":      str(r["event_name"] or ""),
                    "online":          int(r["online"] or 0),
                    "today_views":     int(r["today_views"] or 0),
                    "total_views":     int(r["total_views"] or 0),
                    "unique_visitors": int(r["unique_visitors"] or 0),
                    "mobile":          int(r["mobile"] or 0),
                    "tablet":          int(r["tablet"] or 0),
                    "desktop":         int(r["desktop"] or 0),
                    "avg_duration":    round(float(r["avg_duration"]), 1) if r["avg_duration"] else None,
                    "last_seen_at":    int(r["last_seen_at"] or 0),
                }
                for r in stream_rows
            ],
        }

    def api_stats_geo(self) -> None:
        qs        = parse_qs(urlparse(self.path).query)
        stream_id = qs.get("id", [None])[0]
        range_    = qs.get("range", ["30d"])[0]
        since_map = {"today": 86400, "7d": 7 * 86400, "30d": 30 * 86400}
        since     = (now() - since_map[range_]) if range_ in since_map else 0
        with db() as conn:
            cond   = "WHERE ip_address != ''"
            params: list = []
            if since:
                cond += " AND started_at >= ?"
                params.append(since)
            if stream_id:
                cond += " AND stream_id = ?"
                params.append(int(stream_id))
            rows = conn.execute(
                f"SELECT DISTINCT ip_address FROM viewer_sessions {cond}", params
            ).fetchall()
        ips     = [r["ip_address"] for r in rows]
        geo_map = batch_geoip(ips)
        country_counts: dict[str, dict] = {}
        for ip in ips:
            g    = geo_map.get(ip, {})
            code = g.get("countryCode", "")
            if not code:
                continue
            if code not in country_counts:
                country_counts[code] = {"code": code, "name": g.get("country", code), "count": 0}
            country_counts[code]["count"] += 1
        countries = sorted(country_counts.values(), key=lambda x: x["count"], reverse=True)
        self.send_json({"status": "ok", "data": {"countries": countries}})

    def api_list_api_keys(self) -> None:
        with db() as conn:
            rows = conn.execute(
                "SELECT id, label, created_at, last_used_at FROM api_keys ORDER BY created_at DESC"
            ).fetchall()
        self.send_json({"status": "success", "data": [dict(r) for r in rows]})

    def api_create_api_key(self) -> None:
        body = self.read_json()
        label = str(body.get("label", "")).strip()[:80]
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        ts = now()
        with db() as conn:
            conn.execute(
                "INSERT INTO api_keys (token_hash, label, created_at, last_used_at) VALUES (?, ?, ?, 0)",
                (token_hash, label, ts),
            )
            row = conn.execute(
                "SELECT id, label, created_at, last_used_at FROM api_keys WHERE token_hash = ?",
                (token_hash,),
            ).fetchone()
        result = dict(row)
        result["token"] = token
        self.send_json({"status": "success", "data": result})

    def api_delete_api_key(self) -> None:
        body = self.read_json()
        key_id = int(body.get("id", 0))
        if not key_id:
            raise AppError("missing_id")
        with db() as conn:
            conn.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        self.send_json({"status": "success"})

    def api_stats_sessions_page(self) -> None:
        qs        = parse_qs(urlparse(self.path).query)
        stream_id = int(qs.get("id", [0])[0] or 0)
        offset    = max(0, int(qs.get("offset", [0])[0] or 0))
        limit     = min(max(1, int(qs.get("limit", [50])[0] or 50)), 200)
        order_col = qs.get("order_by",  ["last_seen_at"])[0]
        order_dir = qs.get("order_dir", ["desc"])[0].upper()
        SORT_COLS = {
            "last_seen_at": "last_seen_at",
            "started_at":   "started_at",
            "duration":     "CASE WHEN ended_at > 0 THEN ended_at - started_at ELSE last_seen_at - started_at END",
            "device_type":  "device_type",
            "browser":      "browser",
            "ip_address":   "ip_address",
        }
        if order_dir not in ("ASC", "DESC"):
            order_dir = "DESC"
        col_expr = SORT_COLS.get(order_col, "last_seen_at")
        with db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) AS cnt FROM viewer_sessions WHERE stream_id = ?", (stream_id,)
            ).fetchone()["cnt"]
            rows = conn.execute(
                "SELECT started_at, ended_at, last_seen_at, device_type, "
                "play_state, is_active, ip_address, browser "
                "FROM viewer_sessions WHERE stream_id = ? "
                f"ORDER BY {col_expr} {order_dir} LIMIT ? OFFSET ?",
                (stream_id, limit, offset),
            ).fetchall()
        geo_map  = batch_geoip([r["ip_address"] for r in rows if r["ip_address"]])
        sessions = [{**dict(r), "geo": geo_map.get(r["ip_address"], {})} for r in rows]
        self.send_json({"status": "ok", "data": {
            "sessions": sessions, "total": total, "offset": offset, "limit": limit,
        }})

    def api_stats_dashboard_realtime(self) -> None:
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
        except OSError:
            return
        while True:
            try:
                msg = ("data: " + json.dumps(self._dashboard_snapshot(), ensure_ascii=False) + "\n\n").encode("utf-8")
                self.wfile.write(msg)
                self.wfile.flush()
            except OSError:
                break
            time.sleep(1)

    def api_stats_stream_realtime(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        stream_id = int(qs.get("id", [0])[0] or 0)
        if not stream_id:
            raise AppError("missing_stream_id")
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
        except OSError:
            return
        while True:
            try:
                snapshot = self._stream_live_snapshot(stream_id)
                msg = ("data: " + json.dumps(snapshot, ensure_ascii=False) + "\n\n").encode("utf-8")
                self.wfile.write(msg)
                self.wfile.flush()
            except OSError:
                break
            time.sleep(1)

    def proxy_obs_route(self, request_path: str, request_query: str = "", send_body: bool = True) -> None:
        parts = request_path.strip("/").split("/")
        if len(parts) < 2 or parts[0] != "h":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        slug = parts[1]
        tail = "/".join(parts[2:])
        flv_slug = slug[:-4] if slug.endswith(".flv") else ""
        lookup_slug = flv_slug or slug
        with db() as conn:
            row = conn.execute(
                "SELECT * FROM obs_stream_routes WHERE public_slug = ?", (lookup_slug,)
            ).fetchone()
        if not row:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        stream_key = row["stream_key"]
        if flv_slug:
            upstream_path = f"/live/{quote(stream_key, safe='')}.flv"
            content_type = "video/x-flv"
            rewrite_manifest = False
        elif tail in ("", "index.m3u8"):
            upstream_path = f"/live/{quote(stream_key, safe='')}.m3u8"
            content_type = "application/vnd.apple.mpegurl"
            rewrite_manifest = True
        else:
            if tail.startswith(lookup_slug):
                tail = f"{stream_key}{tail[len(lookup_slug):]}"
            upstream_path = f"/live/{quote(tail, safe=URL_PATH_SAFE)}"
            content_type = mimetypes.guess_type(tail)[0] or "application/octet-stream"
            rewrite_manifest = tail.split("?", 1)[0].endswith(".m3u8")

        query = f"?{request_query}" if request_query else ""
        upstream_url = f"{SRS_HTTP_ORIGIN}{upstream_path}{query}"
        try:
            req = Request(upstream_url, headers={"User-Agent": "StreamHall/1.0"})
            opener = urlopen(req, timeout=STREAM_PROBE_TIMEOUT) if rewrite_manifest else urlopen(req)
            with opener as resp:
                if rewrite_manifest:
                    body = resp.read(1024 * 1024)
                    text = decode_probe_text(body)
                    content = rewrite_hls_manifest(text, lookup_slug, stream_key).encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", f"{content_type}; charset=utf-8")
                    self.send_header("Content-Length", str(len(content)))
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    if send_body:
                        self.wfile.write(content)
                    return

                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not send_body:
                    return
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except HTTPError as exc:
            self.send_error(HTTPStatus(exc.code) if exc.code in HTTPStatus._value2member_map_ else HTTPStatus.BAD_GATEWAY)
        except (URLError, TimeoutError, OSError):
            self.send_error(HTTPStatus.BAD_GATEWAY)

    def proxy_hls_route(self, request_path: str, send_body: bool = True) -> None:
        # URL format: /proxy/hls/<token>/<base64-encoded-url>
        # The token is an HMAC of the target hostname; it ensures that only URLs
        # generated by hls_proxy_path() can be proxied, preventing open-proxy abuse.
        parts = request_path.strip("/").split("/")
        if len(parts) != 4 or parts[0] != "proxy" or parts[1] != "hls":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        token, encoded_url = parts[2], parts[3]
        try:
            target_url = decode_proxy_target(encoded_url)
        except (ValueError, UnicodeDecodeError):
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
        parsed = urlparse(target_url)
        if parsed.scheme not in ("http", "https"):
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
        if not hmac.compare_digest(token, hls_proxy_host_token(parsed.netloc)):
            self.send_error(HTTPStatus.FORBIDDEN)
            return

        try:
            req = Request(target_url, headers={"User-Agent": "StreamHall/1.0"})
            with urlopen(req, timeout=STREAM_PROBE_TIMEOUT if parsed.path.lower().endswith(".m3u8") else 20) as resp:
                content_type = resp.headers.get("Content-Type") or mimetypes.guess_type(parsed.path)[0] or "application/octet-stream"
                is_manifest = parsed.path.lower().endswith(".m3u8") or "mpegurl" in content_type.lower()
                if is_manifest:
                    body = resp.read(2 * 1024 * 1024)
                    content = rewrite_external_hls_manifest(decode_probe_text(body), target_url).encode("utf-8")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/vnd.apple.mpegurl; charset=utf-8")
                    self.send_header("Content-Length", str(len(content)))
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    if send_body:
                        self.wfile.write(content)
                    return

                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not send_body:
                    return
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except HTTPError as exc:
            self.send_error(HTTPStatus(exc.code) if exc.code in HTTPStatus._value2member_map_ else HTTPStatus.BAD_GATEWAY)
        except (URLError, TimeoutError, OSError):
            self.send_error(HTTPStatus.BAD_GATEWAY)

    def api_list_videos(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        dir_index_str = (qs.get("dir_index", [None])[0] or "").strip()
        if not dir_index_str:
            roots = [{"index": i, "label": d["label"]} for i, d in enumerate(VIDEOS_DIRS)]
            self.send_json({"status": "ok", "roots": roots})
            return
        try:
            dir_index = int(dir_index_str)
        except ValueError:
            raise AppError("invalid_filename")
        if dir_index < 0 or dir_index >= len(VIDEOS_DIRS):
            raise AppError("invalid_filename")
        rel_path = (qs.get("rel_path", [""])[0] or "").strip()
        if rel_path:
            if ".." in rel_path.split("/") or rel_path.startswith("/") or os.path.isabs(rel_path):
                raise AppError("invalid_rel_path")
        base = VIDEOS_DIRS[dir_index]["path"]
        target = os.path.join(base, rel_path) if rel_path else base
        entries: list[dict] = []
        if os.path.isdir(target):
            try:
                for name in os.listdir(target):
                    if name.startswith((".", "@", "#")):
                        continue
                    full = os.path.join(target, name)
                    if os.path.isdir(full):
                        entries.append({
                            "name": name,
                            "type": "dir",
                            "has_playable": _dir_has_ext(full, PLAYABLE_VIDEO_EXTS),
                            "has_video": _dir_has_ext(full, VIDEO_EXTS),
                        })
                    elif os.path.isfile(full) and os.path.splitext(name)[1].lower() in VIDEO_EXTS:
                        try:
                            size = os.path.getsize(full)
                        except OSError:
                            size = 0
                        entry_dict: dict = {"name": name, "type": "file", "size": size}
                        if os.path.splitext(name)[1].lower() in PLAYABLE_VIDEO_EXTS:
                            entry_dict["video_url"] = video_proxy_path(dir_index, rel_path, name)
                        entries.append(entry_dict)
            except OSError:
                pass
        entries.sort(key=lambda e: (0 if e["type"] == "dir" else 1, e["name"].lower()))
        self.send_json({
            "status": "ok",
            "dir_index": dir_index,
            "rel_path": rel_path,
            "label": VIDEOS_DIRS[dir_index]["label"],
            "entries": entries,
        })

    def api_list_folder_videos(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        dir_index_str = (qs.get("dir_index", [None])[0] or "").strip()
        rel_path = (qs.get("rel_path", [""])[0] or "").strip()
        mode = (qs.get("mode", [""])[0] or "").strip()
        try:
            dir_index = int(dir_index_str)
        except (ValueError, TypeError):
            raise AppError("invalid_filename")
        if dir_index < 0 or dir_index >= len(VIDEOS_DIRS):
            raise AppError("invalid_filename")
        if rel_path and (".." in rel_path.split("/") or rel_path.startswith("/") or os.path.isabs(rel_path)):
            raise AppError("invalid_rel_path")
        base = VIDEOS_DIRS[dir_index]["path"]
        target = os.path.join(base, rel_path) if rel_path else base
        if not os.path.isdir(target):
            raise AppError("file_not_found")
        use_exts = VIDEO_EXTS if mode == "push" else PLAYABLE_VIDEO_EXTS
        files: list[dict] = []
        try:
            for dirpath, dirnames, filenames in os.walk(target):
                dirnames[:] = sorted(d for d in dirnames if not d.startswith((".", "@", "#")))
                for name in sorted(filenames):
                    if name.startswith((".", "@", "#")):
                        continue
                    if os.path.splitext(name)[1].lower() not in use_exts:
                        continue
                    full = os.path.join(dirpath, name)
                    file_dir_rel = os.path.relpath(dirpath, base).replace("\\", "/")
                    if file_dir_rel == ".":
                        file_dir_rel = ""
                    display_name = os.path.relpath(full, target).replace("\\", "/")
                    try:
                        size = os.path.getsize(full)
                    except OSError:
                        size = 0
                    entry: dict = {"name": name, "size": size, "file_dir_rel": file_dir_rel, "display_name": display_name}
                    if mode != "push":
                        entry["video_url"] = video_proxy_path(dir_index, file_dir_rel, name)
                    files.append(entry)
        except OSError:
            pass
        self.send_json({"status": "ok", "files": files})

    def api_upload_video(self) -> None:
        qs = parse_qs(urlparse(self.path).query)
        filename = (qs.get("filename", [""])[0] or "").strip()
        if not filename or "/" in filename or "\\" in filename or ".." in filename:
            raise AppError("invalid_filename")
        if os.path.splitext(filename)[1].lower() not in VIDEO_EXTS:
            raise AppError("invalid_file_type")
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        dest = os.path.join(UPLOAD_DIR, filename)
        with open(dest, "wb") as f:
            remaining = content_length
            while remaining > 0:
                chunk = self.rfile.read(min(remaining, 65536))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)
        self.send_json({"status": "ok", "filename": filename})

    def api_delete_video(self) -> None:
        body = self.read_json()
        dir_index = int(body.get("dir_index", -1))
        filename = (body.get("filename") or "").strip()
        rel_path = (body.get("rel_path") or "").strip()
        if dir_index < 0 or dir_index >= len(VIDEOS_DIRS):
            raise AppError("invalid_filename")
        if not filename or "/" in filename or "\\" in filename or ".." in filename:
            raise AppError("invalid_filename")
        if rel_path and (".." in rel_path.split("/") or rel_path.startswith("/") or os.path.isabs(rel_path)):
            raise AppError("invalid_rel_path")
        path = os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path, filename) if rel_path else os.path.join(VIDEOS_DIRS[dir_index]["path"], filename)
        if not os.path.isfile(path):
            raise AppError("file_not_found")
        os.remove(path)
        self.send_json({"status": "ok"})

    def api_start_push(self) -> None:
        body = self.read_json()
        dir_index = int(body.get("dir_index", -1))
        filename = (body.get("filename") or "").strip()
        stream_key = (body.get("stream_key") or "").strip()
        loop = bool(body.get("loop", False))
        rel_path = (body.get("rel_path") or "").strip()
        if dir_index < 0 or dir_index >= len(VIDEOS_DIRS):
            raise AppError("invalid_filename")
        is_folder = not filename
        if not is_folder and ("/" in filename or "\\" in filename or ".." in filename):
            raise AppError("invalid_filename")
        if rel_path and (".." in rel_path.split("/") or rel_path.startswith("/") or os.path.isabs(rel_path)):
            raise AppError("invalid_rel_path")
        if not stream_key:
            raise AppError("stream_key_required")
        playlist_path: str | None = None
        if is_folder:
            folder = os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path) if rel_path else VIDEOS_DIRS[dir_index]["path"]
            if not os.path.isdir(folder):
                raise AppError("file_not_found")
            video_files = sorted([
                f for f in os.listdir(folder)
                if not f.startswith((".", "@", "#"))
                and os.path.isfile(os.path.join(folder, f))
                and os.path.splitext(f)[1].lower() in VIDEO_EXTS
            ])
            if not video_files:
                raise AppError("file_not_found")
            fd, playlist_path = tempfile.mkstemp(suffix=".txt", prefix="sh_concat_")
            with os.fdopen(fd, "w") as pf:
                for vf in video_files:
                    pf.write(f"file '{os.path.join(folder, vf)}'\n")
            display_name = rel_path.split("/")[-1] if rel_path else VIDEOS_DIRS[dir_index]["label"]
            cmd = ["ffmpeg", "-re"]
            if loop:
                cmd += ["-stream_loop", "-1"]
            cmd += ["-f", "concat", "-safe", "0", "-i", playlist_path, "-c", "copy", "-f", "flv",
                    f"rtmp://{RTMP_HOST}:1935/live/{stream_key}"]
        else:
            filepath = os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path, filename) if rel_path else os.path.join(VIDEOS_DIRS[dir_index]["path"], filename)
            if not os.path.isfile(filepath):
                raise AppError("file_not_found")
            display_name = filename
            cmd = ["ffmpeg", "-re"]
            if loop:
                cmd += ["-stream_loop", "-1"]
            cmd += ["-i", filepath, "-c", "copy", "-f", "flv",
                    f"rtmp://{RTMP_HOST}:1935/live/{stream_key}"]
        with _pushes_lock:
            for job in active_pushes.values():
                if job["stream_key"] == stream_key:
                    if playlist_path:
                        try:
                            os.unlink(playlist_path)
                        except OSError:
                            pass
                    raise AppError("push_already_running")
        slug = obs_route_slug(stream_key)
        with db() as conn:
            res = conn.execute(
                "INSERT INTO obs_stream_routes (stream_key, public_slug, created_at) VALUES (?, ?, ?) ON CONFLICT (stream_key) DO NOTHING",
                (stream_key, slug, now()),
            )
            push_created_route = res.rowcount == 1
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        job_id = uuid.uuid4().hex[:8]
        started = now()
        with _pushes_lock:
            active_pushes[job_id] = {
                "proc": proc,
                "filename": display_name,
                "stream_key": stream_key,
                "hls_slug": slug,
                "push_created_route": push_created_route,
                "loop": loop,
                "started_at": started,
                "dir_index": dir_index,
                "push_rel_path": rel_path,
                "is_folder": is_folder,
                "user_stopped": False,
            }
        try:
            with db() as conn:
                conn.execute(
                    "INSERT INTO push_jobs (job_id, dir_index, rel_path, filename, stream_key, hls_slug, loop, is_folder, started_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (job_id, dir_index, rel_path, display_name, stream_key, slug, int(loop), int(is_folder), started),
                )
        except Exception:
            pass
        def _monitor(jid: str, stream_key: str = stream_key, created: bool = push_created_route, pfile: str | None = playlist_path, lp: bool = loop) -> None:
            rc = proc.wait()
            with _pushes_lock:
                job_meta = active_pushes.pop(jid, {})
            user_stopped = job_meta.get("user_stopped", False)
            if pfile:
                try:
                    os.unlink(pfile)
                except OSError:
                    pass
            if user_stopped or (rc == 0 and not lp):
                try:
                    with db() as conn:
                        conn.execute("DELETE FROM push_jobs WHERE job_id = ?", (jid,))
                        if created:
                            conn.execute("DELETE FROM obs_stream_routes WHERE stream_key = ?", (stream_key,))
                except Exception:
                    pass
        threading.Thread(target=_monitor, args=(job_id,), daemon=True).start()
        self.send_json({"status": "ok", "job_id": job_id, "hls_slug": slug})

    def api_stop_push(self) -> None:
        body = self.read_json()
        job_id = (body.get("job_id") or "").strip()
        with _pushes_lock:
            job = active_pushes.get(job_id)
        if not job:
            raise AppError("push_not_found")
        job["user_stopped"] = True
        job["proc"].terminate()
        self.send_json({"status": "ok"})

    def api_list_pushes(self) -> None:
        result = []
        with _pushes_lock:
            for job_id, job in list(active_pushes.items()):
                result.append({
                    "job_id": job_id,
                    "filename": job["filename"],
                    "stream_key": job["stream_key"],
                    "hls_slug": job.get("hls_slug", ""),
                    "loop": job["loop"],
                    "elapsed": now() - job["started_at"],
                    "running": job["proc"].poll() is None,
                    "dir_index": job.get("dir_index", -1),
                    "push_rel_path": job.get("push_rel_path", ""),
                    "is_folder": job.get("is_folder", False),
                })
        self.send_json({"status": "ok", "pushes": result})

    def serve_video_file(self, path_str: str, send_body: bool = True) -> None:
        filepath = resolve_video_file_path(path_str)
        if filepath is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        range_header = self.headers.get("Range", "").strip()
        if range_header:
            m = re.match(r"bytes=(\d+)-(\d*)", range_header)
            if not m:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else file_size - 1
            if start > end or start >= file_size:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            end = min(end, file_size - 1)
            length = end - start + 1
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Content-Length", str(length))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if send_body:
                with open(filepath, "rb") as f:
                    f.seek(start)
                    remaining = length
                    while remaining > 0:
                        chunk = f.read(min(65536, remaining))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        remaining -= len(chunk)
        else:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(file_size))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if send_body:
                with open(filepath, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)

    def serve_static(self, request_path: str, send_body: bool = True) -> None:
        routes = {
            "/": "index.html",
            "/index.html": "index.html",
            "/player.html": "player.html",
            "/admin": "admin.html",
            "/admin.html": "admin.html",
        }
        rel = routes.get(request_path, request_path.lstrip("/"))
        target = (PUBLIC_DIR / rel).resolve()
        try:
            target.relative_to(PUBLIC_DIR.resolve())
        except ValueError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content = target.read_bytes()
        mime = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        if target.suffix == ".js":
            mime = "text/javascript"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{mime}; charset=utf-8" if mime.startswith("text/") else mime)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache" if target.suffix == ".html" else "public, max-age=86400")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if send_body:
            self.wfile.write(content)


_SESSION_STALE_SECS = 75  # 5 missed 15s heartbeats → consider ended


def cleanup_stale_sessions_loop() -> None:
    while True:
        time.sleep(120)
        try:
            cutoff = now() - _SESSION_STALE_SECS
            with db() as conn:
                conn.execute(
                    """
                    UPDATE viewer_sessions
                    SET ended_at = last_seen_at, is_active = 0, play_state = 'ended'
                    WHERE is_active = 1 AND last_seen_at < ?
                    """,
                    (cutoff,),
                )
        except Exception:
            pass


def _wait_for_rtmp(host: str, port: int = 1935, timeout: float = 60.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False


def resume_push_jobs() -> None:
    if not _wait_for_rtmp(RTMP_HOST):
        return
    try:
        with db() as conn:
            rows = conn.execute("SELECT * FROM push_jobs").fetchall()
    except Exception:
        return
    for row in rows:
        job_id = row["job_id"]
        try:
            dir_index = int(row["dir_index"])
            if dir_index < 0 or dir_index >= len(VIDEOS_DIRS):
                raise ValueError("invalid dir_index")
            rel_path = row["rel_path"] or ""
            filename = row["filename"] or ""
            stream_key = row["stream_key"]
            hls_slug = row["hls_slug"] or obs_route_slug(stream_key)
            loop = bool(row["loop"])
            is_folder = bool(row["is_folder"])
            playlist_path: str | None = None
            if is_folder:
                folder = os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path) if rel_path else VIDEOS_DIRS[dir_index]["path"]
                if not os.path.isdir(folder):
                    raise FileNotFoundError(folder)
                video_files = sorted([
                    f for f in os.listdir(folder)
                    if not f.startswith((".", "@", "#"))
                    and os.path.isfile(os.path.join(folder, f))
                    and os.path.splitext(f)[1].lower() in VIDEO_EXTS
                ])
                if not video_files:
                    raise FileNotFoundError(folder)
                fd, playlist_path = tempfile.mkstemp(suffix=".txt", prefix="sh_concat_")
                with os.fdopen(fd, "w") as pf:
                    for vf in video_files:
                        pf.write(f"file '{os.path.join(folder, vf)}'\n")
                display_name = rel_path.split("/")[-1] if rel_path else VIDEOS_DIRS[dir_index]["label"]
                cmd = ["ffmpeg", "-re"]
                if loop:
                    cmd += ["-stream_loop", "-1"]
                cmd += ["-f", "concat", "-safe", "0", "-i", playlist_path, "-c", "copy", "-f", "flv",
                        f"rtmp://{RTMP_HOST}:1935/live/{stream_key}"]
            else:
                filepath = (
                    os.path.join(VIDEOS_DIRS[dir_index]["path"], rel_path, filename)
                    if rel_path
                    else os.path.join(VIDEOS_DIRS[dir_index]["path"], filename)
                )
                if not os.path.isfile(filepath):
                    raise FileNotFoundError(filepath)
                display_name = filename
                cmd = ["ffmpeg", "-re"]
                if loop:
                    cmd += ["-stream_loop", "-1"]
                cmd += ["-i", filepath, "-c", "copy", "-f", "flv",
                        f"rtmp://{RTMP_HOST}:1935/live/{stream_key}"]
            with db() as conn:
                conn.execute(
                    "INSERT INTO obs_stream_routes (stream_key, public_slug, created_at) VALUES (?, ?, ?) ON CONFLICT (stream_key) DO NOTHING",
                    (stream_key, hls_slug, now()),
                )
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with _pushes_lock:
                active_pushes[job_id] = {
                    "proc": proc,
                    "filename": display_name,
                    "stream_key": stream_key,
                    "hls_slug": hls_slug,
                    "push_created_route": True,
                    "loop": loop,
                    "started_at": now(),
                    "dir_index": dir_index,
                    "push_rel_path": rel_path,
                    "is_folder": is_folder,
                    "user_stopped": False,
                }
            def _monitor(jid: str = job_id, sk: str = stream_key, pfile: str | None = playlist_path, lp: bool = loop) -> None:
                rc = proc.wait()
                with _pushes_lock:
                    job_meta = active_pushes.pop(jid, {})
                user_stopped = job_meta.get("user_stopped", False)
                if pfile:
                    try:
                        os.unlink(pfile)
                    except OSError:
                        pass
                if user_stopped or (rc == 0 and not lp):
                    try:
                        with db() as conn:
                            conn.execute("DELETE FROM push_jobs WHERE job_id = ?", (jid,))
                            conn.execute("DELETE FROM obs_stream_routes WHERE stream_key = ?", (sk,))
                    except Exception:
                        pass
            threading.Thread(target=_monitor, daemon=True).start()
        except Exception:
            try:
                with db() as conn:
                    conn.execute("DELETE FROM push_jobs WHERE job_id = ?", (job_id,))
            except Exception:
                pass


def main() -> None:
    init_db()
    threading.Thread(target=resume_push_jobs, daemon=True).start()
    threading.Thread(target=monitor_streams_loop, daemon=True).start()
    threading.Thread(target=cleanup_stale_sessions_loop, daemon=True).start()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    httpd = ThreadingHTTPServer((host, port), StreamHallHandler)
    print(f"StreamHall listening on http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
