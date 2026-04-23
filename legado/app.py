import uuid
import csv
import gzip
import hashlib
import json
import tempfile
import mimetypes
import os
import re
import sqlite3
import threading
import time
import shutil
from contextlib import asynccontextmanager
from datetime import datetime
from email.utils import formatdate
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import Headers

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT_ENV = (
    os.getenv("APP_DATA_DIR", "").strip()
    or os.getenv("RENDER_DISK_PATH", "").strip()
)
DATA_DIR = Path(DATA_ROOT_ENV) / "data" if DATA_ROOT_ENV else BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "dinamica.db"

SIENGE_USERNAME = os.getenv("SIENGE_USERNAME", "").strip()
SIENGE_PASSWORD = os.getenv("SIENGE_PASSWORD", "").strip()
SIENGE_INSTANCE = os.getenv("SIENGE_INSTANCE", "").strip().split(".")[0]
SIENGE_BASE_URL = f"https://api.sienge.com.br/{SIENGE_INSTANCE}"
DETECTED_PREFIX = "/public/api/v1"
PORT = int(os.getenv("PORT", "8000"))


def has_sienge_credentials() -> bool:
    return bool(SIENGE_USERNAME and SIENGE_PASSWORD and SIENGE_INSTANCE)

@asynccontextmanager
async def lifespan(_: FastAPI) -> Any:
    init_db()
    seed_default_admin_user()
    precompress_dist_assets(BASE_DIR / "dist")

    auto_sync_on_boot = os.getenv("AUTO_SYNC_ON_BOOT", "false") == "true"
    is_production = os.getenv("NODE_ENV") == "production"

    if auto_sync_on_boot or is_production:
        thread = threading.Thread(target=_run_boot_sync, daemon=True)
        thread.start()

    interval_thread = threading.Thread(target=_run_interval_sync, daemon=True)
    interval_thread.start()

    yield


app = FastAPI(title="Dinamica API", version="1.0.0", lifespan=lifespan)

cors_origins_env = os.getenv("CORS_ALLOW_ORIGINS", "*")
allowed_origins = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_db_lock = threading.Lock()
_sync_lock = threading.Lock()
_is_syncing = False
_etag_cache_lock = threading.Lock()
_etag_cache: dict[tuple[str, int, int], str] = {}
_file_lock = threading.RLock()
BACKUP_DIR = DATA_DIR / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

COMPRESSIBLE_EXTENSIONS = {
    ".html",
    ".css",
    ".js",
    ".mjs",
    ".json",
    ".svg",
    ".txt",
    ".xml",
    ".wasm",
}


def _is_compressible_static_file(file_path: Path) -> bool:
    if file_path.suffix.lower() not in COMPRESSIBLE_EXTENSIONS:
        return False
    lower_name = file_path.name.lower()
    return not (lower_name.endswith(".gz") or lower_name.endswith(".br"))


def _should_regenerate_compressed(source_path: Path, compressed_path: Path) -> bool:
    if not compressed_path.exists():
        return True
    return compressed_path.stat().st_mtime < source_path.stat().st_mtime


def precompress_dist_assets(dist_dir: Path) -> None:
    if not dist_dir.exists():
        return

    try:
        import brotli  # type: ignore
    except Exception:
        brotli = None

    for source_path in dist_dir.rglob("*"):
        if not source_path.is_file() or not _is_compressible_static_file(source_path):
            continue

        gzip_path = Path(str(source_path) + ".gz")
        if _should_regenerate_compressed(source_path, gzip_path):
            with source_path.open("rb") as src_file:
                data = src_file.read()
            with gzip_path.open("wb") as raw_gzip_file:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw_gzip_file,
                    compresslevel=9,
                    mtime=0,
                ) as gz_file:
                    gz_file.write(data)

        if brotli is not None:
            br_path = Path(str(source_path) + ".br")
            if _should_regenerate_compressed(source_path, br_path):
                with source_path.open("rb") as src_file:
                    data = src_file.read()
                br_path.write_bytes(brotli.compress(data, quality=11))


def _is_immutable_asset(path: str) -> bool:
    normalized = (path or "").lower().replace("\\", "/")
    normalized = normalized.lstrip("/")
    return normalized.startswith("assets/") or "/assets/" in normalized


def _set_static_cache_headers(path: str, response: Any) -> None:
    normalized = (path or "").lower()
    filename = Path(normalized).name

    if normalized in {"", "/"} or normalized.endswith(".html") or "." not in filename:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return

    if _is_immutable_asset(path):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return

    response.headers["Cache-Control"] = "public, max-age=3600"


def _strong_etag_for_file(file_path: Path, content_encoding: str) -> str:
    stat = file_path.stat()
    cache_key = (str(file_path), stat.st_mtime_ns, stat.st_size)
    with _etag_cache_lock:
        cached = _etag_cache.get(cache_key)
    if cached:
        return cached

    digest = hashlib.sha256()
    digest.update(content_encoding.encode("ascii", errors="ignore"))
    with file_path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)

    etag = f'"{digest.hexdigest()}"'
    with _etag_cache_lock:
        _etag_cache[cache_key] = etag
    return etag


def _normalize_etag_token(etag: str) -> str:
    token = (etag or "").strip()
    if token.startswith("W/"):
        token = token[2:].strip()
    return token


def _client_etag_matches(scope: dict[str, Any], etag: str) -> bool:
    if_none_match = Headers(scope=scope).get("if-none-match", "")
    if not if_none_match:
        return False

    normalized_server_etag = _normalize_etag_token(etag)
    for candidate in if_none_match.split(","):
        normalized_candidate = _normalize_etag_token(candidate)
        if normalized_candidate == "*" or normalized_candidate == normalized_server_etag:
            return True
    return False


class OptimizedStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope: dict[str, Any]) -> Any:
        if scope.get("method") in {"GET", "HEAD"}:
            static_path = path
            if static_path in {"", ".", "./"} or static_path.endswith("/"):
                static_path = f"{static_path}index.html" if static_path else "index.html"
                if static_path in {".index.html", "./index.html"}:
                    static_path = "index.html"

            full_path, _ = self.lookup_path(static_path)
            if full_path and os.path.isfile(full_path):
                source_path = Path(full_path)
                accept_encoding = Headers(scope=scope).get("accept-encoding", "")

                selected_encoding = ""
                encoded_path = source_path
                if _is_compressible_static_file(source_path):
                    if "br" in accept_encoding and Path(str(source_path) + ".br").exists():
                        selected_encoding = "br"
                        encoded_path = Path(str(source_path) + ".br")
                    elif "gzip" in accept_encoding and Path(str(source_path) + ".gz").exists():
                        selected_encoding = "gzip"
                        encoded_path = Path(str(source_path) + ".gz")

                if selected_encoding:
                    media_type, _ = mimetypes.guess_type(str(source_path))
                    strong_etag = _strong_etag_for_file(encoded_path, selected_encoding)
                    if _client_etag_matches(scope, strong_etag):
                        response = Response(status_code=304)
                        response.headers["Content-Encoding"] = selected_encoding
                        response.headers["Vary"] = "Accept-Encoding"
                        response.headers["ETag"] = strong_etag
                        response.headers["Last-Modified"] = formatdate(
                            encoded_path.stat().st_mtime,
                            usegmt=True,
                        )
                        _set_static_cache_headers(static_path, response)
                        return response

                    response = FileResponse(
                        path=str(encoded_path),
                        media_type=media_type,
                        method=scope.get("method", "GET"),
                    )
                    response.headers["Content-Encoding"] = selected_encoding
                    response.headers["Vary"] = "Accept-Encoding"
                    response.headers["ETag"] = strong_etag
                    response.headers["Last-Modified"] = formatdate(
                        encoded_path.stat().st_mtime,
                        usegmt=True,
                    )
                    _set_static_cache_headers(static_path, response)
                    return response

        response = await super().get_response(path, scope)
        if getattr(response, "status_code", 200) == 200:
            _set_static_cache_headers(path, response)
        return response


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def init_db() -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS dataset_cache (
          key TEXT PRIMARY KEY,
          payload TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS building_meta (
          building_id TEXT PRIMARY KEY,
          engineer TEXT DEFAULT '',
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_locations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT UNIQUE,
          name TEXT NOT NULL,
          address TEXT DEFAULT '',
          latitude REAL,
          longitude REAL,
          type TEXT NOT NULL DEFAULT 'custom',
          source TEXT NOT NULL DEFAULT 'manual',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT UNIQUE,
          name TEXT NOT NULL,
          role TEXT DEFAULT '',
          active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          notes TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS obras_kanban (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          building_id TEXT NOT NULL,
          title TEXT NOT NULL,
          description TEXT DEFAULT '',
          status TEXT NOT NULL DEFAULT 'todo',
          progress_pct INTEGER DEFAULT 0,
          drive_link TEXT DEFAULT '',
          created_by TEXT DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS kanban_sprints (
          id TEXT PRIMARY KEY,
          building_id TEXT NOT NULL,
          name TEXT NOT NULL,
          start_date TEXT DEFAULT '',
          end_date TEXT DEFAULT '',
          color TEXT DEFAULT '#f97316',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS kanban_cards (
          id TEXT PRIMARY KEY,
          sprint_id TEXT NOT NULL,
          building_id TEXT NOT NULL,
          title TEXT NOT NULL,
          description TEXT DEFAULT '',
          status TEXT NOT NULL DEFAULT 'planned',
          priority TEXT NOT NULL DEFAULT 'medium',
          responsible TEXT DEFAULT '',
          due_date TEXT DEFAULT '',
          tags TEXT DEFAULT '[]',
          attachments TEXT DEFAULT '[]',
          created_by TEXT DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY (sprint_id) REFERENCES kanban_sprints(id) ON DELETE CASCADE
        );
        """
    )

    def column_exists(table: str, column: str) -> bool:
        rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
        return any((row[1] == column for row in rows))

    if not column_exists("app_users", "email"):
        cur.execute("ALTER TABLE app_users ADD COLUMN email TEXT DEFAULT ''")
    if not column_exists("app_users", "password_hash"):
        cur.execute("ALTER TABLE app_users ADD COLUMN password_hash TEXT DEFAULT ''")
    if not column_exists("app_users", "department"):
        cur.execute("ALTER TABLE app_users ADD COLUMN department TEXT DEFAULT ''")

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_app_users_email ON app_users(email) WHERE email <> ''"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_kanban_sprints_building_id ON kanban_sprints(building_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_kanban_cards_sprint_id ON kanban_cards(sprint_id)"
    )
    conn.commit()
    conn.close()


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def seed_default_admin_user() -> None:
    conn = get_db()
    cur = conn.cursor()
    now = now_iso()
    cur.execute(
        """
        INSERT INTO app_users (username, email, password_hash, name, role, department, active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
          email = excluded.email,
          password_hash = excluded.password_hash,
          name = excluded.name,
          role = excluded.role,
          department = excluded.department,
          active = excluded.active,
          updated_at = excluded.updated_at
        """,
        (
            "dev@admin.com",
            "dev@admin.com",
            hash_password("admin"),
            "Administrador Dev",
            "developer",
            "Tecnologia",
            1,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()


def save_dataset_cache(key: str, payload: Any) -> None:
    with _db_lock:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dataset_cache (key, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              payload = excluded.payload,
              updated_at = excluded.updated_at
            """,
            (key, json.dumps(payload, ensure_ascii=False), now_iso()),
        )
        conn.commit()
        conn.close()


def read_dataset_cache(key: str) -> Any:
    with _db_lock:
        conn = get_db()
        cur = conn.cursor()
        row = cur.execute("SELECT payload FROM dataset_cache WHERE key = ?", (key,)).fetchone()
        conn.close()
    if not row or not row[0]:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def read_cached_payload(cache_key: str, filename: str | None = None, default: Any = None) -> Any:
    payload = read_dataset_cache(cache_key)
    if payload is not None:
        return payload
    if filename:
        payload = read_from_file(filename)
        if payload is not None:
            return payload
    return default


def write_cached_payload(cache_key: str, payload: Any, filename: str | None = None) -> None:
    save_dataset_cache(cache_key, payload)
    if filename:
        save_to_file(filename, payload)


def save_building_meta_to_db(building_id: str, engineer: str) -> None:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO building_meta (building_id, engineer, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(building_id) DO UPDATE SET
          engineer = excluded.engineer,
          updated_at = excluded.updated_at
        """,
        (building_id, engineer, now_iso()),
    )
    conn.commit()
    conn.close()


def read_building_meta_from_db() -> dict[str, dict[str, str]]:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute("SELECT building_id, engineer FROM building_meta").fetchall()
    conn.close()
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        result[str(row[0])] = {"engineer": row[1] or ""}
    return result


def save_to_file(filename: str, data: Any) -> None:
    file_path = DATA_DIR / filename
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with _file_lock:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=file_path.parent,
            delete=False,
        ) as tmp_file:
            if isinstance(data, str):
                tmp_file.write(data)
            else:
                json.dump(data, tmp_file, ensure_ascii=False, indent=2)
            temp_name = tmp_file.name
        os.replace(temp_name, file_path)


def read_from_file(filename: str) -> Any:
    file_path = DATA_DIR / filename
    if not file_path.exists():
        return None
    try:
        with _file_lock:
            content = file_path.read_text(encoding="utf-8")
        if filename.endswith(".csv"):
            return content
        return json.loads(content)
    except Exception:
        return None


def read_obras_meta() -> dict[str, Any]:
    file_meta = read_from_file("obras_meta.json") or {}
    db_meta = read_building_meta_from_db()
    return {**file_meta, **db_meta}


def save_obras_meta(meta: dict[str, Any]) -> None:
    save_to_file("obras_meta.json", meta)
    for building_id, item in meta.items():
        engineer = str((item or {}).get("engineer", ""))
        save_building_meta_to_db(str(building_id), engineer)


def create_resilient_backup() -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_path = BACKUP_DIR / timestamp
    backup_path.mkdir(parents=True, exist_ok=True)

    files_to_copy = [
        "dinamica.db",
        "obras.json",
        "obras_by_code.json",
        "usuarios.json",
        "credores.json",
        "empresas.json",
        "clientes.json",
        "pedidos.json",
        "financeiro.json",
        "receber.json",
        "itens_pedidos.json",
        "cotacoes_pedidos.json",
        "solicitantes-cache.json",
        "consolidado.csv",
    ]

    with _file_lock:
        for name in files_to_copy:
            src = DATA_DIR / name
            if src.exists():
                shutil.copy2(src, backup_path / name)

    existing_backups = sorted(
        [p for p in BACKUP_DIR.iterdir() if p.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    for stale in existing_backups[5:]:
        shutil.rmtree(stale, ignore_errors=True)


def to_array(payload: Any) -> list[Any]:
    if isinstance(payload, dict) and isinstance(payload.get("results"), list):
        return payload["results"]
    if isinstance(payload, list):
        return payload
    return []


def fix_server_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    replacements = {
        "CONSTRU��O": "CONSTRUCAO",
        "MANUTEN��O": "MANUTENCAO",
        "ESPA�O": "ESPACO",
        "VIV�NCIA": "VIVENCIA",
        "EDUCA��O": "EDUCACAO",
        "CI�NCIA": "CIENCIA",
        "PAVIMENTA��O": "PAVIMENTACAO",
        "REGULARIZA��O": "REGULARIZACAO",
        "REQUALIFICA��O": "REQUALIFICACAO",
        "DUPLICA��O": "DUPLICACAO",
        "AMPLIA��O": "AMPLIACAO",
        "SERVI�OS": "SERVICOS",
        "GEST�O": "GESTAO",
        "SUBESTA��O": "SUBESTACAO",
        "A�UDE": "ACUDE",
        "S�O": "SAO",
        "JO�O": "JOAO",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def normalize_person_name(value: Any) -> str:
    if isinstance(value, str):
        raw = value
    elif isinstance(value, dict):
        raw = value.get("name") or value.get("nome") or value.get("username") or value.get("userName") or ""
    else:
        raw = ""
    text = fix_server_text(raw).strip()
    text = re.sub(r"^comprador\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^usu[aá]rio\s+", "", text, flags=re.IGNORECASE)
    return text.strip()


def normalize_building_name(raw: Any, fallback_id: str | None = None) -> str:
    if isinstance(raw, dict):
        resolved = (
            raw.get("name")
            or raw.get("nome")
            or raw.get("tradeName")
            or raw.get("description")
            or raw.get("enterpriseName")
            or raw.get("nomeObra")
            or raw.get("fantasyName")
            or ""
        )
    else:
        resolved = ""
    clean = fix_server_text(str(resolved).strip())
    if clean:
        return clean
    if fallback_id:
        return f"Obra {fallback_id}"
    return "Obra sem nome"


def normalize_creditor_name(raw: Any, fallback_id: str | None = None) -> str:
    if isinstance(raw, dict):
        resolved = (
            raw.get("name")
            or raw.get("nome")
            or raw.get("nomeFantasia")
            or raw.get("supplierName")
            or raw.get("creditorName")
            or raw.get("nomeFornecedor")
            or raw.get("fornecedor")
            or ""
        )
    else:
        resolved = ""
    clean = fix_server_text(str(resolved).strip())
    if clean:
        return clean
    if fallback_id:
        return f"Credor {fallback_id}"
    return "Credor sem nome"


def resolve_building_name_from_caches(building_id: str, fallback: str | None = None) -> str:
    pedidos_cache = read_dataset_cache("pedidos") or {}
    pedidos_list = to_array(pedidos_cache)
    matched = next(
        (
            item
            for item in pedidos_list
            if str(item.get("codigoVisivelObra") or item.get("idObra") or item.get("buildingId") or "")
            == building_id
        ),
        None,
    )
    name = normalize_building_name(matched or {}, building_id)
    if name not in {f"Obra {building_id}", "Obra sem nome"}:
        return name
    return fallback or f"Obra {building_id}"


def resolve_creditor_name_from_caches(creditor_id: str, fallback: str | None = None) -> str:
    pedidos_cache = read_dataset_cache("pedidos") or {}
    financeiro_cache = read_dataset_cache("financeiro") or {}
    pedidos_list = to_array(pedidos_cache)
    financeiro_list = to_array(financeiro_cache)

    pedido = next(
        (
            item
            for item in pedidos_list
            if str(item.get("codigoFornecedor") or item.get("idCredor") or item.get("supplierId") or "")
            == creditor_id
        ),
        None,
    )
    financeiro = next(
        (
            item
            for item in financeiro_list
            if str(item.get("creditorId") or item.get("idCredor") or item.get("codigoFornecedor") or item.get("debtorId") or "")
            == creditor_id
        ),
        None,
    )

    candidate = normalize_creditor_name(pedido or {}, creditor_id)
    if candidate not in {f"Credor {creditor_id}", "Credor sem nome"}:
        return candidate

    financial_candidate = normalize_creditor_name(financeiro or {}, creditor_id)
    if financial_candidate not in {f"Credor {creditor_id}", "Credor sem nome"}:
        return financial_candidate

    return fallback or f"Credor {creditor_id}"


def to_coordinate(value: Any) -> float | None:
    try:
        numeric = float(value)
        return numeric
    except Exception:
        return None


def geocode_address(address: str) -> dict[str, float] | None:
    response = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"format": "jsonv2", "limit": 1, "q": address},
        headers={"User-Agent": "DinamicaDashboard/1.0"},
        timeout=20,
    )
    response.raise_for_status()
    data = response.json() if isinstance(response.json(), list) else []
    if not data:
        return None
    latitude = to_coordinate(data[0].get("lat"))
    longitude = to_coordinate(data[0].get("lon"))
    if latitude is None or longitude is None:
        return None
    return {"latitude": latitude, "longitude": longitude}


def resolve_route_point(point: dict[str, Any]) -> dict[str, float] | None:
    latitude = to_coordinate(point.get("latitude"))
    longitude = to_coordinate(point.get("longitude"))
    if latitude is not None and longitude is not None:
        return {"latitude": latitude, "longitude": longitude}
    address = str(point.get("address") or "").strip()
    if not address:
        return None
    return geocode_address(address)


def extract_distance_km_from_google_html(html: str) -> float | None:
    patterns = [
        r'"distance":"\s*([0-9]+(?:[.,][0-9]+)?)\s*km"',
        r'"distanceText":"\s*([0-9]+(?:[.,][0-9]+)?)\s*km"',
        r'aria-label="([0-9]+(?:[.,][0-9]+)?)\s*km"',
        r'>([0-9]+(?:[.,][0-9]+)?)\s*km<',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if not match:
            continue
        numeric = float(match.group(1).replace(",", "."))
        if numeric > 0:
            return numeric
    return None


def get_google_maps_public_distance(origin: dict[str, Any], destination: dict[str, Any]) -> dict[str, Any] | None:
    try:
        response = requests.get(
            "https://www.google.com/maps/dir/",
            params={
                "api": 1,
                "origin": origin.get("address") or f"{origin.get('latitude')},{origin.get('longitude')}",
                "destination": destination.get("address")
                or f"{destination.get('latitude')},{destination.get('longitude')}",
                "travelmode": "driving",
            },
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            },
            timeout=20,
        )
        distance_km = extract_distance_km_from_google_html(response.text or "")
        if distance_km is not None:
            return {"distanceKm": distance_km, "provider": "Google Maps"}
    except Exception:
        return None
    return None


def calculate_route_distance(origin: dict[str, Any], destination: dict[str, Any]) -> dict[str, Any]:
    google_maps_api_key = str(os.getenv("GOOGLE_MAPS_API_KEY", "")).strip()

    if google_maps_api_key:
        try:
            response = requests.get(
                "https://maps.googleapis.com/maps/api/directions/json",
                params={
                    "origin": origin.get("address") or f"{origin.get('latitude')},{origin.get('longitude')}",
                    "destination": destination.get("address")
                    or f"{destination.get('latitude')},{destination.get('longitude')}",
                    "mode": "driving",
                    "language": "pt-BR",
                    "key": google_maps_api_key,
                },
                timeout=20,
            )
            data = response.json()
            distance_m = (
                ((data.get("routes") or [{}])[0].get("legs") or [{}])[0].get("distance") or {}
            ).get("value")
            if isinstance(distance_m, (int, float)):
                return {"distanceKm": distance_m / 1000.0, "provider": "Google Maps"}
        except Exception:
            pass

    google_public = get_google_maps_public_distance(origin, destination)
    if google_public:
        return google_public

    origin_coords = resolve_route_point(origin)
    destination_coords = resolve_route_point(destination)
    if not origin_coords or not destination_coords:
        return {"distanceKm": None, "provider": ""}

    try:
        response = requests.get(
            f"https://router.project-osrm.org/route/v1/driving/{origin_coords['longitude']},{origin_coords['latitude']};{destination_coords['longitude']},{destination_coords['latitude']}",
            params={"overview": "false"},
            timeout=20,
        )
        data = response.json()
        distance_m = ((data.get("routes") or [{}])[0]).get("distance")
        if isinstance(distance_m, (int, float)):
            return {"distanceKm": distance_m / 1000.0, "provider": "OSRM"}
    except Exception:
        pass

    return {"distanceKm": None, "provider": ""}


def get_latest_sync_info() -> dict[str, Any] | None:
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT started_at, finished_at, status, notes
        FROM sync_runs
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "started_at": row[0],
        "finished_at": row[1],
        "status": row[2],
        "notes": row[3],
    }


def parse_datetime_any(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text or text == "---":
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except Exception:
        pass

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def normalize_order_for_alert(order: dict[str, Any]) -> dict[str, Any]:
    date_raw = order.get("dataEmissao") or order.get("data") or order.get("date") or ""
    date_parsed = parse_datetime_any(date_raw)
    date_numeric = int(date_parsed.timestamp() * 1000) if date_parsed else 0

    return {
        "id": order.get("id") or order.get("numero") or 0,
        "buildingId": order.get("idObra") or order.get("codigoVisivelObra") or order.get("buildingId") or 0,
        "totalAmount": float(order.get("valorTotal") or order.get("totalAmount") or 0),
        "date": date_raw,
        "dateNumeric": date_numeric,
    }


def list_today_orders_for_alerts(live_fetch: bool = True, limit: int = 200) -> list[dict[str, Any]]:
    orders: list[Any] = []

    if live_fetch:
        try:
            response = sienge_get(
                f"{DETECTED_PREFIX}/purchase-orders",
                {"limit": max(20, min(int(limit), 500)), "offset": 0},
            )
            if response.status_code < 400:
                payload = response.json()
                if isinstance(payload, dict) and isinstance(payload.get("results"), list):
                    orders = payload.get("results") or []
        except Exception:
            orders = []

    if not orders:
        cached_payload = read_dataset_cache("pedidos") or read_from_file("pedidos.json") or {}
        orders = to_array(cached_payload)

    normalized_orders = [normalize_order_for_alert(order) for order in orders if isinstance(order, dict)]
    today = datetime.now().date()
    today_orders = [
        order
        for order in normalized_orders
        if (parse_datetime_any(order.get("date")) or datetime.min).date() == today
    ]
    today_orders.sort(key=lambda item: int(item.get("dateNumeric") or 0))
    return today_orders


def sienge_get(endpoint: str, params: dict[str, Any] | None = None) -> requests.Response:
    if not has_sienge_credentials():
        raise HTTPException(
            status_code=503,
            detail="Credenciais do Sienge nao configuradas. Defina SIENGE_USERNAME, SIENGE_PASSWORD e SIENGE_INSTANCE.",
        )
    url = f"{SIENGE_BASE_URL}{endpoint}"
    return requests.get(
        url,
        params=params,
        auth=(SIENGE_USERNAME, SIENGE_PASSWORD),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        timeout=60,
    )


def sienge_healthcheck() -> dict[str, Any]:
    if not has_sienge_credentials():
        return {
            "ok": False,
            "statusCode": 503,
            "detail": "Credenciais do Sienge nao configuradas.",
        }
    try:
        response = requests.get(
            f"{SIENGE_BASE_URL}{DETECTED_PREFIX}/companies",
            params={"limit": 1, "offset": 0},
            auth=(SIENGE_USERNAME, SIENGE_PASSWORD),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=15,
        )

        detail = ""
        try:
            payload = response.json() if response.content else {}
            if isinstance(payload, dict):
                detail = str(payload.get("message") or payload.get("error") or "")
        except Exception:
            detail = response.text[:200]

        return {
            "ok": response.status_code < 400,
            "statusCode": response.status_code,
            "detail": detail,
        }
    except Exception as exc:
        return {
            "ok": False,
            "statusCode": None,
            "detail": str(exc),
        }


def fetch_all(endpoint: str, base_params: dict[str, Any] | None = None) -> dict[str, Any]:
    params = base_params or {}
    all_results: list[Any] = []
    offset = 0
    limit = 200

    while True:
        current_params = dict(params)
        current_params["limit"] = limit
        current_params["offset"] = offset
        response = sienge_get(endpoint, current_params)
        if response.status_code >= 400:
            if offset == 0:
                response.raise_for_status()
            break

        payload = response.json()
        results = payload.get("results") if isinstance(payload, dict) else payload
        if not isinstance(results, list) or len(results) == 0:
            break

        all_results.extend(results)
        offset += len(results)
        meta = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
        count = meta.get("count") if isinstance(meta, dict) else None
        if len(results) < limit or (isinstance(count, int) and offset >= count):
            break

    return {"data": {"results": all_results}}


def sync_all_data() -> bool:
    global _is_syncing

    with _sync_lock:
        if _is_syncing:
            return True
        _is_syncing = True

    sync_start = now_iso()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sync_runs (started_at, status, notes) VALUES (?, ?, ?)",
        (sync_start, "running", "Sincronizacao Sienge completa"),
    )
    sync_run_id = cur.lastrowid
    conn.commit()
    conn.close()

    try:
        start_date_str = "1900-01-01"
        end_date_str = "2030-12-31"

        def safe_fetch(callable_fn, fallback=None):
            try:
                return callable_fn()
            except Exception:
                return fallback

        obras_res = safe_fetch(lambda: fetch_all(f"{DETECTED_PREFIX}/enterprises"), None)
        usuarios_res = safe_fetch(lambda: fetch_all(f"{DETECTED_PREFIX}/users"), None)
        credores_res = safe_fetch(lambda: fetch_all(f"{DETECTED_PREFIX}/creditors"), None)
        pedidos_res = fetch_all(f"{DETECTED_PREFIX}/purchase-orders")
        po_rest_res = fetch_all("/public/api/v1/purchase-orders")
        financeiro_res = fetch_all(
            f"{DETECTED_PREFIX}/bills", {"startDate": start_date_str, "endDate": end_date_str}
        )
        receber_res = fetch_all(
            f"{DETECTED_PREFIX}/accounts-statements",
            {"startDate": start_date_str, "endDate": end_date_str},
        )
        empresas_res = safe_fetch(lambda: fetch_all(f"{DETECTED_PREFIX}/companies"), None)

        clientes_response = safe_fetch(lambda: sienge_get(f"{DETECTED_PREFIX}/clientes"), None)
        clientes_payload = (
            clientes_response.json()
            if clientes_response is not None and clientes_response.status_code < 400
            else []
        )

        pedidos = (pedidos_res or {}).get("data", {}).get("results", [])
        po_rest = (po_rest_res or {}).get("data", {}).get("results", [])
        financeiro = (financeiro_res or {}).get("data", {}).get("results", [])
        receber = (receber_res or {}).get("data", {}).get("results", [])

        obras = (obras_res or {}).get("data", {}).get("results", []) if obras_res else []
        usuarios = (usuarios_res or {}).get("data", {}).get("results", []) if usuarios_res else []
        credores = (credores_res or {}).get("data", {}).get("results", []) if credores_res else []
        empresas = (empresas_res or {}).get("data", {}).get("results", []) if empresas_res else []
        clientes = clientes_payload.get("results", clientes_payload) if isinstance(clientes_payload, dict) else clientes_payload

        solicitantes_cache = read_cached_payload("solicitantes_cache", "solicitantes-cache.json", {}) or {}
        po_map = {po.get("id"): po for po in po_rest if isinstance(po, dict) and po.get("id") is not None}
        missing_reqs: set[str] = set()

        for p in pedidos:
            order_id = p.get("numero") or p.get("id")
            po_obj = po_map.get(order_id)
            if not po_obj:
                continue
            p["createdBy"] = (
                normalize_person_name(po_obj.get("createdBy"))
                or normalize_person_name(po_obj.get("buyerName"))
                or normalize_person_name(p.get("nomeComprador"))
                or str(p.get("codigoComprador") or "").strip()
            )
            note = fix_server_text(po_obj.get("internalNotes") or po_obj.get("notes") or "")
            req_match = re.search(r"SOLICITA[CÇ][AÃ]O\s+(\d+)", note, flags=re.IGNORECASE) or re.search(
                r"REQ(?:UISION)?\s+(\d+)", note, flags=re.IGNORECASE
            )
            if req_match:
                req_id = req_match.group(1)
                p["reqIdOrigin"] = req_id
                if not solicitantes_cache.get(req_id):
                    missing_reqs.add(req_id)

        if missing_reqs:
            req_array = list(missing_reqs)
            for i in range(0, len(req_array), 15):
                batch = req_array[i : i + 15]
                for req_id in batch:
                    try:
                        res = sienge_get(f"/public/api/v1/purchase-requests/{req_id}")
                        if res.status_code < 400:
                            payload = res.json()
                            requester_user = payload.get("requesterUser") if isinstance(payload, dict) else None
                            requester_name = normalize_person_name(requester_user)
                            if requester_name:
                                solicitantes_cache[req_id] = requester_name
                    except Exception:
                        continue
            write_cached_payload("solicitantes_cache", solicitantes_cache, "solicitantes-cache.json")

        for p in pedidos:
            req_origin = p.get("reqIdOrigin")
            if req_origin and solicitantes_cache.get(req_origin):
                p["solicitante"] = solicitantes_cache[req_origin]
                p["requesterId"] = solicitantes_cache[req_origin]
            else:
                fallback_requester = normalize_person_name(p.get("createdBy"))
                p["solicitante"] = fallback_requester
                p["requesterId"] = fallback_requester

        if len(obras) == 0 and isinstance(pedidos, list):
            grouped: dict[str, Any] = {}
            for p in pedidos:
                bid = p.get("codigoVisivelObra") or p.get("idObra")
                if not bid:
                    continue
                bid_str = str(bid)
                grouped[bid_str] = {
                    "id": bid,
                    "code": bid_str,
                    "nome": normalize_building_name(p, bid_str),
                    "name": normalize_building_name(p, bid_str),
                }
            obras = list(grouped.values())

        if len(usuarios) == 0 and isinstance(pedidos, list):
            grouped_u: dict[str, Any] = {}
            for p in pedidos:
                uid = p.get("codigoComprador") or p.get("idComprador")
                if not uid:
                    continue
                uid_str = str(uid)
                grouped_u[uid_str] = {"id": uid_str, "nome": p.get("nomeComprador") or uid_str}
            usuarios = list(grouped_u.values())

        if len(credores) == 0 and isinstance(pedidos, list):
            grouped_c: dict[str, Any] = {}
            for p in pedidos:
                cid = p.get("codigoFornecedor") or p.get("idCredor")
                if not cid:
                    continue
                cid_str = str(cid)
                grouped_c[cid_str] = {
                    "id": cid,
                    "nome": normalize_creditor_name(p, cid_str),
                    "name": normalize_creditor_name(p, cid_str),
                }
            credores = list(grouped_c.values())

        obras_by_code = read_cached_payload("obras_by_code", "obras_by_code.json", {}) or {}
        unique_building_ids = sorted(
            {
                str(p.get("codigoVisivelObra") or p.get("idObra") or p.get("buildingId"))
                for p in pedidos
                if p.get("codigoVisivelObra") or p.get("idObra") or p.get("buildingId")
            }
        )

        for building_id in unique_building_ids:
            existing = obras_by_code.get(building_id) or {}
            if existing.get("name") or existing.get("nome"):
                continue
            try:
                response = sienge_get(f"{DETECTED_PREFIX}/enterprises/{building_id}")
                response.raise_for_status()
                data = response.json() if isinstance(response.json(), dict) else {}
                name = (
                    data.get("name")
                    or data.get("nome")
                    or data.get("tradeName")
                    or data.get("description")
                    or f"Obra {building_id}"
                )
                address = data.get("address") or data.get("adress") or data.get("endereco") or ""
                obras_by_code[building_id] = {
                    "id": data.get("id") or int(building_id),
                    "code": building_id,
                    "name": name,
                    "nome": name,
                    "address": address,
                    "endereco": address,
                    "companyId": data.get("companyId") or data.get("idCompany"),
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                }
            except Exception:
                order = next(
                    (
                        p
                        for p in pedidos
                        if str(p.get("codigoVisivelObra") or p.get("idObra") or p.get("buildingId"))
                        == building_id
                    ),
                    {},
                )
                name = order.get("nomeObra") or f"Obra {building_id}"
                address = order.get("enderecoObra") or ""
                obras_by_code[building_id] = {
                    "id": int(building_id),
                    "code": building_id,
                    "name": name,
                    "nome": name,
                    "address": address,
                    "endereco": address,
                }

        if obras_by_code:
            write_cached_payload("obras_by_code", obras_by_code, "obras_by_code.json")

        write_cached_payload("obras", obras, "obras.json")
        write_cached_payload("usuarios", usuarios, "usuarios.json")
        write_cached_payload("credores", credores, "credores.json")
        write_cached_payload("empresas", empresas, "empresas.json")
        write_cached_payload("clientes", clientes, "clientes.json")

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
              name = excluded.name,
              address = excluded.address,
              latitude = excluded.latitude,
              longitude = excluded.longitude,
              type = excluded.type,
              source = excluded.source,
              updated_at = excluded.updated_at
            """,
            (
                "hq",
                "Sede",
                "Dinamica Empreendimentos e Solucoes LTDA, Fortaleza, CE, Brasil",
                -3.7319,
                -38.5267,
                "hq",
                "system",
                sync_start,
                sync_start,
            ),
        )

        merged_obras = list(obras) + list(obras_by_code.values())
        for obra in merged_obras:
            code = str(obra.get("code") or obra.get("codigoVisivel") or obra.get("id") or "").strip()
            if not code:
                continue
            name = normalize_building_name(obra, code)
            cur.execute(
                """
                INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                  name = excluded.name,
                  address = excluded.address,
                  latitude = excluded.latitude,
                  longitude = excluded.longitude,
                  type = excluded.type,
                  source = excluded.source,
                  updated_at = excluded.updated_at
                """,
                (
                    f"building:{code}",
                    name,
                    str(obra.get("address") or obra.get("endereco") or name),
                    obra.get("latitude"),
                    obra.get("longitude"),
                    "building",
                    "sienge",
                    sync_start,
                    now_iso(),
                ),
            )

        conn.commit()
        conn.close()

        items_map = read_cached_payload("itens_pedidos", "itens_pedidos.json", {}) or {}
        if isinstance(pedidos, list):
            for order in pedidos[:50]:
                order_id = order.get("id") or order.get("numero")
                if not order_id or items_map.get(str(order_id)):
                    continue
                try:
                    response = sienge_get(f"/public/api/v1/purchase-orders/{order_id}/items")
                    if response.status_code < 400:
                        items_payload = response.json()
                        items_map[str(order_id)] = (
                            items_payload.get("results", items_payload)
                            if isinstance(items_payload, dict)
                            else items_payload
                        )
                except Exception:
                    continue

        write_cached_payload("itens_pedidos", items_map, "itens_pedidos.json")

        write_cached_payload("pedidos", pedidos_res.get("data", {}), "pedidos.json")
        write_cached_payload("financeiro", financeiro_res.get("data", {}), "financeiro.json")
        write_cached_payload("receber", receber_res.get("data", {}), "receber.json")

        csv_headers = [
            "Tipo",
            "ID",
            "Obra",
            "Empresa",
            "Fornecedor/Cliente/Descricao",
            "Comprador",
            "Data",
            "Valor",
            "Status",
            "Condicao Pagamento/Prazos",
            "Item/Insumo",
            "Qtd",
            "Un",
            "Vlr Unit",
        ]
        rows: list[list[Any]] = []

        for o in pedidos:
            id_obra = o.get("idObra") or o.get("codigoVisivelObra")
            obra_obj = next((b for b in obras if str(b.get("id")) == str(id_obra)), {})
            obra_nome = obra_obj.get("nome") or id_obra or "Nao Informado"
            empresa_nome = next(
                (
                    e.get("name")
                    for e in empresas
                    if str(e.get("id")) == str(obra_obj.get("idCompany"))
                ),
                "Dinamica",
            )
            id_credor = o.get("idCredor") or o.get("codigoFornecedor")
            fornecedor = next(
                (
                    c.get("nome")
                    for c in credores
                    if str(c.get("id")) == str(id_credor)
                ),
                id_credor or "Nao Informado",
            )
            id_user = o.get("idComprador") or o.get("codigoComprador")
            user = next(
                (
                    u.get("nome")
                    for u in usuarios
                    if str(u.get("id")) == str(id_user)
                ),
                id_user or "Nao Informado",
            )

            order_id = o.get("id") or o.get("numero")
            date = o.get("dataEmissao") or o.get("data") or "---"
            valor = o.get("valorTotal") or 0
            status = o.get("situacao") or "N/A"
            condicao = o.get("condicaoPagamentoDescricao") or "N/A"
            prazo = o.get("dataEntrega") or o.get("prazoEntrega") or "---"

            rows.append(
                [
                    "Pedido",
                    order_id,
                    obra_nome,
                    empresa_nome,
                    fornecedor,
                    user,
                    date,
                    valor,
                    status,
                    f"{condicao} / Prazo: {prazo}",
                    "---",
                    "---",
                    "---",
                    "---",
                ]
            )

            items = items_map.get(str(order_id), [])
            if isinstance(items, list):
                for item in items:
                    desc = item.get("descricao") or item.get("itemNome") or "Item"
                    qtd = item.get("quantidade") or 0
                    un = item.get("unidadeMedidaSigla") or "UN"
                    vlr_u = item.get("valorUnitario") or 0
                    vlr_t = item.get("valorTotal") or 0
                    rows.append(
                        [
                            "Item",
                            order_id,
                            obra_nome,
                            empresa_nome,
                            "---",
                            "---",
                            date,
                            vlr_t,
                            status,
                            "---",
                            desc,
                            qtd,
                            un,
                            vlr_u,
                        ]
                    )

        for f in financeiro:
            id_obra = f.get("idObra") or f.get("codigoVisivelObra")
            obra_obj = next((b for b in obras if str(b.get("id")) == str(id_obra)), {})
            obra_nome = obra_obj.get("nome") or id_obra or "Nao Informado"
            empresa_nome = next(
                (
                    e.get("name")
                    for e in empresas
                    if str(e.get("id")) == str(obra_obj.get("idCompany"))
                ),
                "Dinamica",
            )
            desc = f.get("descricao") or f.get("historico") or f.get("tipoDocumento") or "Titulo a Pagar"
            rows.append(
                [
                    "A Pagar",
                    f.get("id") or f.get("codigoTitulo"),
                    obra_nome,
                    empresa_nome,
                    desc,
                    "---",
                    f.get("dataVencimento") or f.get("dataEmissao") or f.get("issueDate"),
                    f.get("valor") or f.get("valorSaldo"),
                    f.get("situacao") or "ABERTO",
                    "---",
                    "---",
                    "---",
                    "---",
                    "---",
                ]
            )

        for r in receber:
            obra_obj = next((b for b in obras if str(b.get("id")) == str(r.get("idObra"))), {})
            obra_nome = obra_obj.get("nome") or r.get("idObra") or "Nao Informado"
            empresa_nome = next(
                (
                    e.get("name")
                    for e in empresas
                    if str(e.get("id")) == str(obra_obj.get("idCompany"))
                ),
                "Dinamica",
            )
            desc = r.get("descricao") or r.get("historico") or "Titulo a Receber"
            rows.append(
                [
                    "A Receber",
                    r.get("id") or r.get("numero") or r.get("codigoTitulo"),
                    obra_nome,
                    empresa_nome,
                    desc,
                    "---",
                    r.get("dataVencimento") or r.get("dataEmissao"),
                    r.get("valor") or r.get("valorSaldo"),
                    r.get("situacao") or "ABERTO",
                    "---",
                    "---",
                    "---",
                    "---",
                    "---",
                ]
            )

        csv_path = DATA_DIR / "consolidado.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(csv_headers)
            writer.writerows(rows)

        save_dataset_cache("consolidado_csv", csv_path.read_text(encoding="utf-8-sig"))
        create_resilient_backup()

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sync_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
            (now_iso(), "success", f"Sincronizacao completa desde {start_date_str}", sync_run_id),
        )
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sync_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
            (now_iso(), "error", str(exc), sync_run_id),
        )
        conn.commit()
        conn.close()
        return False
    finally:
        with _sync_lock:
            _is_syncing = False


@app.post("/api/sienge/sync")
def api_sync() -> Any:
    success = sync_all_data()
    if not success:
        raise HTTPException(status_code=500, detail="Falha na sincronizacao")
    return {"message": "Sincronizacao concluida com sucesso", "timestamp": now_iso()}


@app.get("/api/sienge/download-csv")
def api_download_csv() -> Any:
    file_path = DATA_DIR / "consolidado.csv"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo CSV ainda nao gerado. Aguarde a sincronizacao.")
    return FileResponse(path=file_path, filename="sienge_consolidado.csv", media_type="text/csv")


@app.get("/api/sienge/test")
def api_test() -> Any:
    pedidos = to_array(read_cached_payload("pedidos", "pedidos.json"))
    financeiro = to_array(read_cached_payload("financeiro", "financeiro.json"))
    receber = to_array(read_cached_payload("receber", "receber.json"))
    obras = to_array(read_cached_payload("obras", "obras.json"))
    credores = to_array(read_cached_payload("credores", "credores.json"))
    usuarios = to_array(read_cached_payload("usuarios", "usuarios.json"))
    live = sienge_healthcheck()
    return {
        "ok": live["ok"] or len(pedidos) > 0 or len(financeiro) > 0 or len(receber) > 0,
        "baseURL": f"{SIENGE_BASE_URL}{DETECTED_PREFIX}",
        "live": live,
        "cache": {
            "pedidos": len(pedidos),
            "financeiro": len(financeiro),
            "receber": len(receber),
            "obras": len(obras),
            "credores": len(credores),
            "usuarios": len(usuarios),
        },
        "latestSync": get_latest_sync_info(),
    }


@app.get("/api/sienge/alerts/recent")
def api_recent_purchase_alerts(request: Request) -> Any:
    live_fetch = request.query_params.get("live", "true").lower() != "false"
    limit_param = request.query_params.get("limit", "200")
    try:
        limit = max(20, min(int(limit_param), 500))
    except Exception:
        limit = 200

    today_orders = list_today_orders_for_alerts(live_fetch=live_fetch, limit=limit)
    day_key = datetime.now().strftime("%Y-%m-%d")

    state = read_dataset_cache("alerts_state")
    if not isinstance(state, dict):
        state = {}

    if state.get("day") != day_key:
        state = {"day": day_key, "seenIds": []}

    seen_ids = state.get("seenIds") if isinstance(state.get("seenIds"), list) else []
    seen_set = {str(item) for item in seen_ids}

    next_alert = next((order for order in today_orders if str(order.get("id")) not in seen_set), None)
    if not next_alert:
        return {
            "hasAlert": False,
            "alert": None,
            "todayCount": len(today_orders),
            "latestSync": get_latest_sync_info(),
        }

    seen_set.add(str(next_alert.get("id")))
    updated_seen = list(seen_set)
    if len(updated_seen) > 500:
        updated_seen = updated_seen[-500:]

    state = {"day": day_key, "seenIds": updated_seen}
    save_dataset_cache("alerts_state", state)

    return {
        "hasAlert": True,
        "alert": next_alert,
        "todayCount": len(today_orders),
        "latestSync": get_latest_sync_info(),
    }


@app.get("/api/sienge/bootstrap")
def api_bootstrap() -> Any:
    try:
        obras_payload = read_cached_payload("obras", "obras.json")
        obras_by_code_payload = read_cached_payload("obras_by_code", "obras_by_code.json")
        usuarios_payload = read_cached_payload("usuarios", "usuarios.json")
        credores_payload = read_cached_payload("credores", "credores.json")
        companies_payload = read_cached_payload("empresas", "empresas.json")
        pedidos_payload = read_cached_payload("pedidos", "pedidos.json")
        financeiro_payload = read_cached_payload("financeiro", "financeiro.json")
        receber_payload = read_cached_payload("receber", "receber.json")
        itens_payload = read_cached_payload("itens_pedidos", "itens_pedidos.json", {}) or {}
        solicitantes_cache = read_cached_payload("solicitantes_cache", "solicitantes-cache.json", {}) or {}
        meta = read_obras_meta()

        obras_base = to_array(obras_payload)
        obras_by_code = list((obras_by_code_payload or {}).values()) if isinstance(obras_by_code_payload, dict) else []
        usuarios = to_array(usuarios_payload)
        credores = to_array(credores_payload)
        companies = to_array(companies_payload)
        pedidos = to_array(pedidos_payload)
        financeiro = to_array(financeiro_payload)
        receber = to_array(receber_payload)

        building_name_hints: dict[str, str] = {}
        creditor_name_hints: dict[str, str] = {}

        for pedido in pedidos:
            building_id = str(pedido.get("codigoVisivelObra") or pedido.get("idObra") or pedido.get("buildingId") or "")
            creditor_id = str(pedido.get("codigoFornecedor") or pedido.get("idCredor") or pedido.get("supplierId") or "")
            building_name = normalize_building_name(pedido, building_id)
            creditor_name = normalize_creditor_name(pedido, creditor_id)
            if building_id and building_name not in {f"Obra {building_id}", "Obra sem nome"}:
                building_name_hints[building_id] = building_name
            if creditor_id and creditor_name not in {f"Credor {creditor_id}", "Credor sem nome"}:
                creditor_name_hints[creditor_id] = creditor_name

        for item in financeiro:
            creditor_id = str(item.get("creditorId") or item.get("idCredor") or item.get("codigoFornecedor") or item.get("debtorId") or "")
            creditor_name = normalize_creditor_name(item, creditor_id)
            if creditor_id and creditor_name not in {f"Credor {creditor_id}", "Credor sem nome"}:
                creditor_name_hints[creditor_id] = creditor_name

        building_map: dict[str, Any] = {}
        for obra in [*obras_base, *obras_by_code]:
            bid = str(obra.get("id") or obra.get("code") or obra.get("codigoVisivel") or "")
            if not bid:
                continue
            normalized_name = normalize_building_name(obra, bid)
            fallback_name = (
                normalized_name
                if normalized_name not in {f"Obra {bid}", "Obra sem nome"}
                else building_name_hints.get(bid) or f"Obra {bid}"
            )
            building_map[bid] = {
                "id": int(obra.get("id") or bid),
                "code": str(obra.get("code") or obra.get("codigoVisivel") or bid),
                "name": fallback_name,
                "nome": fallback_name,
                "address": obra.get("address") or obra.get("endereco") or obra.get("adress") or "",
                "endereco": obra.get("endereco") or obra.get("address") or obra.get("adress") or "",
                "latitude": obra.get("latitude"),
                "longitude": obra.get("longitude"),
                "companyId": obra.get("companyId") or obra.get("idCompany"),
                "engineer": ((meta.get(bid) or {}).get("engineer") or obra.get("engineer") or obra.get("responsavelTecnico") or ""),
            }

        for pedido in pedidos:
            bid = str(pedido.get("codigoVisivelObra") or pedido.get("idObra") or pedido.get("buildingId") or "")
            if not bid or bid in building_map:
                continue
            hinted_name = building_name_hints.get(bid) or normalize_building_name(pedido, bid)
            building_map[bid] = {
                "id": int(bid),
                "code": bid,
                "name": hinted_name,
                "nome": hinted_name,
                "address": pedido.get("enderecoObra") or "",
                "endereco": pedido.get("enderecoObra") or "",
                "latitude": None,
                "longitude": None,
                "companyId": None,
                "engineer": (meta.get(bid) or {}).get("engineer") or "",
            }

        user_map: dict[str, str] = {}
        for user in usuarios:
            uid = str(user.get("id") or user.get("userId") or user.get("username") or "")
            if uid:
                user_map[uid] = normalize_person_name(user)

        creditor_map: dict[str, str] = {}
        for credor in credores:
            cid = str(credor.get("id") or credor.get("creditorId") or "")
            if not cid:
                continue
            normalized = normalize_creditor_name(credor, cid)
            creditor_map[cid] = (
                normalized if normalized not in {f"Credor {cid}", "Credor sem nome"} else creditor_name_hints.get(cid) or f"Credor {cid}"
            )

        normalized_orders: list[dict[str, Any]] = []
        for pedido in pedidos:
            building_id = str(pedido.get("codigoVisivelObra") or pedido.get("idObra") or pedido.get("buildingId") or "")
            supplier_id = str(pedido.get("codigoFornecedor") or pedido.get("idCredor") or pedido.get("supplierId") or "")
            buyer_id = str(pedido.get("idComprador") or pedido.get("codigoComprador") or pedido.get("buyerId") or "")
            note = fix_server_text(pedido.get("internalNotes") or pedido.get("notes") or "")
            request_match = re.search(r"SOLICITA[CÇ][AÃ]O\s+(\d+)", note, flags=re.IGNORECASE) or re.search(
                r"REQ(?:UISION)?\s+(\d+)", note, flags=re.IGNORECASE
            )
            requester_from_cache = solicitantes_cache.get(request_match.group(1), "") if request_match else ""
            raw_requester = str(
                requester_from_cache
                or pedido.get("solicitante")
                or pedido.get("requesterId")
                or pedido.get("requesterUser")
                or pedido.get("createdBy")
                or ""
            ).strip()
            requester_name = normalize_person_name(user_map.get(raw_requester) or raw_requester)
            buyer_name = normalize_person_name(pedido.get("nomeComprador") or pedido.get("buyerName") or user_map.get(buyer_id) or buyer_id)
            building = building_map.get(building_id, {})
            company_id = building.get("companyId")
            normalized_orders.append(
                {
                    "id": pedido.get("id") or pedido.get("numero") or 0,
                    "buildingId": int(building_id) if building_id.isdigit() else 0,
                    "companyId": int(company_id) if company_id is not None else None,
                    "buyerId": buyer_id,
                    "supplierId": int(supplier_id) if supplier_id.isdigit() else 0,
                    "date": pedido.get("data") or pedido.get("dataEmissao") or pedido.get("date") or "",
                    "totalAmount": float(pedido.get("totalAmount") or pedido.get("valorTotal") or 0),
                    "status": pedido.get("status") or pedido.get("situacao") or "N/A",
                    "paymentCondition": pedido.get("condicaoPagamento") or pedido.get("paymentMethod") or "A Prazo",
                    "deliveryDate": pedido.get("dataEntrega") or pedido.get("prazoEntrega") or "",
                    "internalNotes": pedido.get("internalNotes") or pedido.get("observacao") or "",
                    "nomeObra": building.get("name") or building_name_hints.get(building_id) or normalize_building_name(pedido, building_id),
                    "nomeFornecedor": creditor_map.get(supplier_id)
                    or creditor_name_hints.get(supplier_id)
                    or normalize_creditor_name(pedido, supplier_id),
                    "nomeComprador": buyer_name,
                    "solicitante": requester_name or buyer_name,
                    "requesterId": requester_name or buyer_name,
                    "createdBy": buyer_name,
                }
            )

        normalized_financial: list[dict[str, Any]] = []
        for item in financeiro:
            creditor_id = str(item.get("creditorId") or item.get("idCredor") or item.get("codigoFornecedor") or "")
            building_id = str(item.get("idObra") or item.get("codigoObra") or item.get("enterpriseId") or "")
            creditor_name = creditor_map.get(creditor_id) or creditor_name_hints.get(creditor_id) or normalize_creditor_name(item, creditor_id)
            building_info = building_map.get(building_id, {})
            company_id = building_info.get("companyId")
            # Fallback: usar debtorId como companyId quando a obra nao tem empresa vinculada.
            # O debtorId nos titulos do Sienge (/bills) indica a empresa emitente (empresa pagadora).
            if company_id is None:
                debtor_id = item.get("debtorId")
                if debtor_id is not None:
                    try:
                        company_id = int(debtor_id)
                    except (ValueError, TypeError):
                        pass
            normalized_financial.append(
                {
                    "id": item.get("id") or item.get("numero") or item.get("codigoTitulo") or item.get("documentNumber") or 0,
                    "companyId": int(company_id) if company_id is not None else None,
                    "creditorId": creditor_id,
                    "buildingId": int(building_id) if building_id.isdigit() else 0,
                    "dataVencimento": item.get("dataVencimento")
                    or item.get("issueDate")
                    or item.get("dueDate")
                    or item.get("dataVencimentoProjetado")
                    or item.get("dataEmissao")
                    or item.get("dataContabil")
                    or "",
                    "descricao": item.get("descricao")
                    or item.get("historico")
                    or item.get("tipoDocumento")
                    or item.get("notes")
                    or item.get("observacao")
                    or "Titulo a Pagar",
                    "valor": float(
                        item.get("totalInvoiceAmount")
                        or item.get("valor")
                        or item.get("amount")
                        or item.get("valorTotal")
                        or item.get("valorLiquido")
                        or item.get("valorBruto")
                        or 0
                    ),
                    "situacao": item.get("situacao") or item.get("status") or "Pendente",
                    "creditorName": creditor_name,
                    "nomeCredor": creditor_name,
                    "nomeObra": building_map.get(building_id, {}).get("name")
                    or building_name_hints.get(building_id)
                    or normalize_building_name(item, building_id),
                }
            )

        normalized_receivable: list[dict[str, Any]] = []
        for item in receber:
            building_id = str(item.get("idObra") or item.get("codigoObra") or item.get("enterpriseId") or "")
            building_info = building_map.get(building_id, {})
            company_id = building_info.get("companyId")
            # Fallback companyId from links[rel=company]
            if company_id is None:
                for lnk in (item.get("links") or []):
                    if lnk.get("rel") == "company" and lnk.get("href"):
                        try:
                            company_id = int(lnk["href"].rstrip("/").split("/")[-1])
                        except Exception:
                            pass
                        break
            # Raw value may be negative for Expense records – keep sign for calc
            raw_value = float(
                item.get("value")
                if item.get("value") is not None
                else (
                    item.get("valor")
                    or item.get("valorSaldo")
                    or item.get("totalInvoiceAmount")
                    or item.get("valorTotal")
                    or item.get("amount")
                    or 0
                )
            )
            # documentId (e.g. "REC", "NFE", "DEBC", "BOL") + documentNumber form the Tit/Parc column
            doc_id = item.get("documentId") or ""
            doc_number = item.get("documentNumber") or ""
            installment = item.get("installmentNumber")
            # Extract bank account code from links for filtering
            bank_account_code = ""
            for lnk in (item.get("links") or []):
                if lnk.get("rel") == "bank-account" and lnk.get("href"):
                    bank_account_code = lnk["href"].rstrip("/").split("/")[-1]
                    break
            normalized_receivable.append(
                {
                    "id": item.get("id")
                    or item.get("numero")
                    or item.get("numeroTitulo")
                    or item.get("codigoTitulo")
                    or doc_number
                    or 0,
                    "companyId": int(company_id) if company_id is not None else None,
                    "buildingId": int(building_id) if building_id.isdigit() else 0,
                    "dataVencimento": item.get("data")
                    or item.get("date")
                    or item.get("dataVencimento")
                    or item.get("dataEmissao")
                    or item.get("issueDate")
                    or item.get("dataVencimentoProjetado")
                    or "",
                    "descricao": item.get("descricao")
                    or item.get("historico")
                    or item.get("observacao")
                    or item.get("notes")
                    or item.get("description")
                    or "Titulo a Receber",
                    # abs value for Contas a Receber/Pagar tables
                    "valor": abs(raw_value),
                    # raw signed value for Fluxo de Caixa (needed to reproduce Sienge PDF exactly)
                    "rawValue": raw_value,
                    "situacao": str(item.get("situacao") or item.get("status") or "ABERTO").upper(),
                    "nomeCliente": item.get("nomeCliente")
                    or item.get("nomeFantasiaCliente")
                    or item.get("cliente")
                    or item.get("clientName")
                    or "Extrato/Cliente",
                    "nomeObra": building_map.get(building_id, {}).get("name")
                    or building_name_hints.get(building_id)
                    or normalize_building_name(item, building_id),
                    # Tit/Parc fields
                    "documentId": doc_id,
                    "documentNumber": doc_number,
                    "installmentNumber": installment,
                    "statementOrigin": item.get("statementOrigin") or "",
                    "statementType": item.get("statementType") or "",
                    "billId": item.get("billId"),
                    "type": item.get("type") or "Income",
                    # Bank account code for transparency/filtering
                    "bankAccountCode": bank_account_code,
                }
            )

        saldo_bancario_calc = sum(float(item.get("value") or 0) for item in receber if str(item.get("type")).strip().lower() == "income") - sum(float(item.get("value") or 0) for item in receber if str(item.get("type")).strip().lower() == "expense")

        return {
            "saldoBancario": saldo_bancario_calc,
            "latestSync": get_latest_sync_info(),
            "obras": list(building_map.values()),
            "usuarios": [
                {
                    "id": str(user.get("id") or user.get("userId") or user.get("username") or ""),
                    "name": normalize_person_name(user),
                    "nome": normalize_person_name(user),
                }
                for user in usuarios
            ],
            "credores": [
                {
                    "id": credor.get("id"),
                    "name": creditor_map.get(str(credor.get("id"))) or normalize_creditor_name(credor, str(credor.get("id"))),
                    "nome": creditor_map.get(str(credor.get("id"))) or normalize_creditor_name(credor, str(credor.get("id"))),
                    "cnpj": credor.get("cnpj") or credor.get("cpfCnpj") or "",
                }
                for credor in credores
            ],
            "companies": [
                {
                    "id": company.get("id"),
                    "name": company.get("name") or company.get("nome") or company.get("companyName") or f"Empresa {company.get('id')}",
                    "cnpj": company.get("cnpj") or company.get("cpfCnpj") or "",
                }
                for company in companies
            ],
            "pedidos": normalized_orders,
            "financeiro": normalized_financial,
            "receber": normalized_receivable,
            "itensPedidos": itens_payload,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/sienge/itens-pedidos")
def api_itens_pedidos() -> Any:
    cached = read_cached_payload("itens_pedidos", "itens_pedidos.json")
    return cached or {}


@app.post("/api/sienge/fetch-items")
async def api_fetch_items(request: Request) -> Any:
    body = await request.json()
    ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(ids, list):
        return {}

    items_map = read_cached_payload("itens_pedidos", "itens_pedidos.json", {}) or {}
    changed = False
    for order_id in ids:
        key = str(order_id)
        if items_map.get(key):
            continue
        try:
            result = sienge_get(f"/public/api/v1/purchase-orders/{order_id}/items")
            if result.status_code < 400:
                payload = result.json()
                items_map[key] = payload.get("results", payload) if isinstance(payload, dict) else payload
                changed = True
        except Exception:
            continue

    if changed:
        write_cached_payload("itens_pedidos", items_map, "itens_pedidos.json")
    return items_map



@app.post("/api/sienge/fetch-quotations")
async def api_fetch_quotations(request: Request) -> Any:
    """Busca as cotações (concorrência de fornecedores) de pedidos de compra no Sienge.

    Estratégia: O Sienge não expõe via GET os preços dos fornecedores concorrentes por cotação.
    Para cada pedido vencedor, identificamos o purchaseQuotationId nos seus itens, e depois
    escaneamos todos os pedidos em cache para encontrar outros que referenciam o mesmo
    purchaseQuotationId — cada um representando um fornecedor concorrente com seus preços.
    """
    body = await request.json()
    ids = body.get("ids") if isinstance(body, dict) else None
    if not isinstance(ids, list):
        return {}

    quotations_map = read_cached_payload("cotacoes_pedidos", "cotacoes_pedidos.json", {}) or {}
    items_map = read_cached_payload("itens_pedidos", "itens_pedidos.json", {}) or {}
    changed = False

    # Load all pedidos from cache to find orders sharing the same quotation
    pedidos_cache = read_cached_payload("pedidos", "pedidos.json", {}) or {}
    all_pedidos = to_array(pedidos_cache)

    # Build a lookup: orderId -> pedido info
    pedido_lookup: dict[str, dict] = {}
    for p in all_pedidos:
        pid = str(p.get("id") or p.get("numero") or "")
        if pid:
            pedido_lookup[pid] = p

    # Build a reverse index: quotationId -> list of order IDs that reference it
    # (from cached items only, to avoid N+1 API calls)
    quotation_order_index: dict[int, list[str]] = {}
    for oid, o_items in items_map.items():
        if not isinstance(o_items, list):
            continue
        for item in o_items:
            for pq in (item.get("purchaseQuotations") or []):
                qid = pq.get("purchaseQuotationId")
                if qid:
                    qid_int = int(qid)
                    if qid_int not in quotation_order_index:
                        quotation_order_index[qid_int] = []
                    if oid not in quotation_order_index[qid_int]:
                        quotation_order_index[qid_int].append(oid)

    def build_quote_from_order(oid: str, order_info: dict, o_items: list) -> dict:
        sup_id = order_info.get("supplierId") or order_info.get("codigoFornecedor")
        return {
            "orderId": int(oid) if str(oid).isdigit() else 0,
            "supplierId": sup_id,
            "creditorId": sup_id,
            "supplierName": None,
            "date": order_info.get("date") or order_info.get("dataEmissao") or "",
            "totalAmount": float(order_info.get("totalAmount") or order_info.get("valorTotal") or 0),
            "items": [
                {
                    "description": it.get("resourceDescription") or it.get("descricao") or "",
                    "resourceId": it.get("resourceId"),
                    "unitPrice": float(it.get("netPrice") or it.get("unitPrice") or it.get("valorUnitario") or 0),
                    "quantity": float(it.get("quantity") or it.get("quantidade") or 0),
                    "unitOfMeasure": it.get("unitOfMeasure") or it.get("unidadeMedidaSigla") or "",
                    "quotationIds": [pq.get("purchaseQuotationId") for pq in (it.get("purchaseQuotations") or [])],
                }
                for it in o_items
            ],
        }

    for order_id in ids:
        key = str(order_id)
        if quotations_map.get(key):
            continue
        try:
            # Step 1: Get items for this order
            order_items = items_map.get(key)
            if not order_items:
                try:
                    res = sienge_get(f"/public/api/v1/purchase-orders/{order_id}/items")
                    if res.status_code < 400:
                        p = res.json()
                        order_items = p.get("results", p) if isinstance(p, dict) else p
                        items_map[key] = order_items
                        # Update index for newly fetched items
                        for item in (order_items or []):
                            for pq in (item.get("purchaseQuotations") or []):
                                qid = pq.get("purchaseQuotationId")
                                if qid:
                                    qid_int = int(qid)
                                    if qid_int not in quotation_order_index:
                                        quotation_order_index[qid_int] = []
                                    if key not in quotation_order_index[qid_int]:
                                        quotation_order_index[qid_int].append(key)
                except Exception:
                    pass

            if not order_items or not isinstance(order_items, list):
                quotations_map[key] = []
                changed = True
                continue

            # Step 2: Collect all purchaseQuotationIds used by this order
            quotation_ids: set[int] = set()
            for item in order_items:
                for pq in (item.get("purchaseQuotations") or []):
                    qid = pq.get("purchaseQuotationId")
                    if qid:
                        quotation_ids.add(int(qid))

            if not quotation_ids:
                quotations_map[key] = []
                changed = True
                continue

            # Step 3: Find all orders sharing the same quotation IDs (competitor orders).
            # NOTE: In Sienge, each purchase quotation typically generates exactly ONE
            # purchase order (for the winning supplier). Other supplier bids are stored
            # internally in the quotation and are NOT accessible via the public API GET.
            # This index lookup will find competitors if they happened to be synced
            # independently with the same purchaseQuotationId.
            competitor_order_ids: set[str] = set()
            for qid in quotation_ids:
                for oid in quotation_order_index.get(qid, []):
                    if oid != key:
                        competitor_order_ids.add(oid)

            # Step 4: Fetch items for competitor orders (if not already cached)
            for oid in list(competitor_order_ids):
                if not items_map.get(oid):
                    try:
                        r2 = sienge_get(f"/public/api/v1/purchase-orders/{oid}/items")
                        if r2.status_code < 400:
                            p2 = r2.json()
                            comp_items = p2.get("results", p2) if isinstance(p2, dict) else p2
                            items_map[oid] = comp_items
                    except Exception:
                        continue

            # Step 5: Fetch metadata from the purchase quotation endpoint
            quotation_meta: dict = {}
            order_data = pedido_lookup.get(key, {})  # Get order info for the winning order
            winning_supplier_id = int(order_data.get("supplierId") or 0)
            if quotation_ids:
                qid_meta = next(iter(quotation_ids))
                try:
                    r_meta = sienge_get(f"/public/api/v1/purchase-quotations/{qid_meta}")
                    if r_meta.status_code < 400:
                        quotation_meta = r_meta.json() or {}
                except Exception:
                    pass

            # Step 6: Build the competitor quotes list
            competitor_quotes: list[dict] = []

            for oid in competitor_order_ids:
                comp_items = items_map.get(oid)
                if not comp_items or not isinstance(comp_items, list):
                    continue
                comp_order = pedido_lookup.get(oid, {})
                competitor_quotes.append(build_quote_from_order(oid, comp_order, comp_items))

            # Include the winning order
            competitor_quotes.append(build_quote_from_order(key, order_data, order_items))

            # Sort by orderId for deterministic output
            competitor_quotes.sort(key=lambda x: x.get("orderId") or 0)

            # Store quotes with metadata
            quotations_map[key] = {
                "quotes": competitor_quotes,
                "quotationIds": sorted(quotation_ids),
                "quotationMeta": quotation_meta,
                "winningSupplier": winning_supplier_id,
            }
            changed = True

        except Exception:
            continue

    if changed:
        write_cached_payload("itens_pedidos", items_map, "itens_pedidos.json")
        write_cached_payload("cotacoes_pedidos", quotations_map, "cotacoes_pedidos.json")
    return quotations_map


@app.get("/api/sienge/debug-order/{order_id}")
def api_debug_order(order_id: str) -> Any:
    """Diagnóstico: retorna todos os dados brutos do Sienge para um pedido específico,
    incluindo tentativas em múltiplos endpoints relacionados a cotações."""
    result = {"order_id": order_id, "endpoints": {}}

    # 1. Dados do pedido
    for ep in [
        f"/public/api/v1/purchase-orders/{order_id}",
        f"{DETECTED_PREFIX}/purchase-orders/{order_id}",
    ]:
        try:
            r = sienge_get(ep)
            result["endpoints"][ep] = {"status": r.status_code, "data": r.json() if r.content else {}}
        except Exception as exc:
            result["endpoints"][ep] = {"error": str(exc)}

    # 2. Cotações do pedido (vários endpoints possíveis)
    for ep in [
        f"/public/api/v1/purchase-orders/{order_id}/quotations",
        f"{DETECTED_PREFIX}/purchase-orders/{order_id}/quotations",
        f"/public/api/v1/purchase-orders/{order_id}/quotation",
        f"{DETECTED_PREFIX}/purchase-orders/{order_id}/quotation",
    ]:
        try:
            r = sienge_get(ep)
            result["endpoints"][ep] = {"status": r.status_code, "data": r.json() if r.content else {}}
        except Exception as exc:
            result["endpoints"][ep] = {"error": str(exc)}

    return result


@app.get("/api/sienge/debug-quotation/{quotation_id}")
def api_debug_quotation(quotation_id: str) -> Any:
    """Diagnóstico: busca cotação pelo ID da cotação (ex: 4641), não pelo ID do pedido."""
    result = {"quotation_id": quotation_id, "endpoints": {}}

    for ep in [
        f"/public/api/v1/quotations/{quotation_id}",
        f"{DETECTED_PREFIX}/quotations/{quotation_id}",
        f"/public/api/v1/quotations/{quotation_id}/items",
        f"{DETECTED_PREFIX}/quotations/{quotation_id}/items",
        f"/public/api/v1/quotations/{quotation_id}/suppliers",
        f"{DETECTED_PREFIX}/quotations/{quotation_id}/suppliers",
        f"/public/api/v1/purchase-requests/quotations/{quotation_id}",
    ]:
        try:
            r = sienge_get(ep)
            result["endpoints"][ep] = {"status": r.status_code, "data": r.json() if r.content else {}}
        except Exception as exc:
            result["endpoints"][ep] = {"error": str(exc)}

    return result


def passthrough_or_cached(cache_key: str, filename: str, endpoint: str, req: Request) -> Any:
    cached = read_dataset_cache(cache_key) or read_from_file(filename)
    force = req.query_params.get("force")
    if cached and not force:
        return cached

    params = dict(req.query_params)
    response = sienge_get(endpoint, params)
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/financeiro")
def api_financeiro(request: Request) -> Any:
    return passthrough_or_cached("financeiro", "financeiro.json", f"{DETECTED_PREFIX}/bills", request)


@app.get("/api/sienge/financeiro/receber")
def api_financeiro_receber(request: Request) -> Any:
    return passthrough_or_cached("receber", "receber.json", f"{DETECTED_PREFIX}/accounts-statements", request)


@app.get("/api/sienge/notas-entrada")
def api_notas_entrada(request: Request) -> Any:
    response = sienge_get(f"{DETECTED_PREFIX}/notas-fiscais-entrada", dict(request.query_params))
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/itens-nota/{item_id}")
def api_itens_nota(item_id: str) -> Any:
    response = sienge_get(f"{DETECTED_PREFIX}/notas-fiscais-entrada/{item_id}/itens")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/obras")
def api_obras() -> Any:
    cached = read_from_file("obras.json") or read_dataset_cache("obras")
    cached_by_code = read_from_file("obras_by_code.json") or read_dataset_cache("obras_by_code")
    pedidos_cache = read_from_file("pedidos.json") or read_dataset_cache("pedidos")
    meta = read_obras_meta()

    if cached or cached_by_code:
        base_list = to_array(cached)
        by_code_list = list(cached_by_code.values()) if isinstance(cached_by_code, dict) else []
        pedidos_list = to_array(pedidos_cache)
        merged: dict[str, Any] = {}

        for obra in [*base_list, *by_code_list]:
            bid = str(obra.get("id") or obra.get("code") or obra.get("codigoVisivel") or "")
            if not bid:
                continue
            current = merged.get(bid, {})
            pedido_fallback = next(
                (
                    item
                    for item in pedidos_list
                    if str(item.get("codigoVisivelObra") or item.get("idObra") or item.get("buildingId") or "")
                    == bid
                ),
                {},
            )
            normalized_name = normalize_building_name(obra, bid)
            fallback_name = normalize_building_name(pedido_fallback, bid)
            final_name = (
                normalized_name
                if normalized_name not in {f"Obra {bid}", "Obra sem nome"}
                else fallback_name or current.get("name") or current.get("nome") or f"Obra {bid}"
            )
            merged[bid] = {
                **current,
                **obra,
                "id": obra.get("id") or current.get("id") or int(bid),
                "code": obra.get("code") or obra.get("codigoVisivel") or current.get("code") or bid,
                "name": final_name,
                "nome": final_name,
                "address": obra.get("address")
                or obra.get("endereco")
                or obra.get("adress")
                or current.get("address")
                or current.get("endereco")
                or "",
                "endereco": obra.get("endereco")
                or obra.get("address")
                or obra.get("adress")
                or current.get("endereco")
                or current.get("address")
                or "",
                "engineer": (meta.get(bid) or {}).get("engineer")
                or obra.get("engineer")
                or obra.get("responsavelTecnico")
                or obra.get("engenheiro")
                or current.get("engineer")
                or "",
            }

        return {"results": list(merged.values())}

    response = sienge_get(f"{DETECTED_PREFIX}/enterprises")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.post("/api/sienge/obras/meta")
async def api_obras_meta(request: Request) -> Any:
    body = await request.json()
    building_id = body.get("id")
    engineer = body.get("engineer")
    if not building_id:
        raise HTTPException(status_code=400, detail="Id da obra e obrigatorio.")

    meta = read_obras_meta()
    key = str(building_id)
    meta[key] = {**(meta.get(key) or {}), "engineer": str(engineer or "").strip()}
    save_obras_meta(meta)
    save_building_meta_to_db(key, str(engineer or "").strip())
    return {"success": True, "meta": meta[key]}

@app.get("/api/sienge/global/{module}/{endpoint:path}")
def api_sienge_global_proxy(module: str, endpoint: str, request: Request) -> Any:
    """
    Universal Proxy: routes requests like /api/sienge/global/commercial/customers
    directly to https://api.sienge.com.br/{subdomain}/api/v1/commercial/customers
    """
    query_params = str(request.query_params)
    path = f"/api/v1/{module}/{endpoint}"
    if query_params:
        path += f"?{query_params}"
    
    response = sienge_get(path)
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()

@app.post("/api/admin/backup/drive")
def api_trigger_backup() -> Any:
    try:
        from backup_manager import run_backup
        result = run_backup()
        if result.get("success"):
            return {"success": True, "message": "Backup enviado ao Google Drive com sucesso!", "file_id": result.get("file_id")}
        return JSONResponse(status_code=500, content={"error": result.get("error", "Erro desconhecido")})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/sienge/obras/{building_id}/kanban")
def api_get_obras_kanban(building_id: str) -> Any:
    conn = get_db()
    cur = conn.cursor()
    if building_id == "all":
        rows = cur.execute(
            "SELECT id, building_id, title, description, status, progress_pct, drive_link, created_by, created_at, updated_at FROM obras_kanban ORDER BY updated_at DESC"
        ).fetchall()
    else:
        rows = cur.execute(
            "SELECT id, building_id, title, description, status, progress_pct, drive_link, created_by, created_at, updated_at FROM obras_kanban WHERE building_id = ?",
            (building_id,)
        ).fetchall()
    conn.close()
    return {"results": [dict(r) for r in rows]}

@app.post("/api/sienge/obras/{building_id}/kanban")
async def api_create_obras_kanban(building_id: str, request: Request) -> Any:
    body = await request.json()
    title = body.get("title", "")
    description = body.get("description", "")
    status = body.get("status", "todo")
    progress_pct = int(body.get("progress_pct", 0))
    drive_link = body.get("drive_link", "")
    created_by = body.get("created_by", "")
    now = now_iso()

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO obras_kanban (building_id, title, description, status, progress_pct, drive_link, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (building_id, title, description, status, progress_pct, drive_link, created_by, now, now)
    )
    task_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": task_id}

@app.post("/api/sienge/obras/kanban/{task_id}/upload")
async def api_upload_kanban_media(task_id: int, file: UploadFile = File(...)) -> Any:
    uploads_dir = DATA_DIR / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = int(datetime.now().timestamp())
    safe_filename = f"{task_id}_{timestamp}_{file.filename}"
    file_path = uploads_dir / safe_filename
    
    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())
        
    url_path = f"/data/uploads/{safe_filename}"
    
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT drive_link FROM obras_kanban WHERE id = ?", (task_id, )).fetchone()
    if row:
        current_links = row[0] or ""
        new_links = f"{current_links},{url_path}" if current_links else url_path
        cur.execute("UPDATE obras_kanban SET drive_link = ?, updated_at = ? WHERE id = ?", (new_links, now_iso(), task_id))
    conn.commit()
    conn.close()
    
    return {"success": True, "url": url_path}

@app.get("/data/uploads/{filename}")
def server_uploads(filename: str):
    file_path = DATA_DIR / "uploads" / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    media_type, _ = mimetypes.guess_type(str(file_path))
    return FileResponse(path=str(file_path), media_type=media_type)

@app.put("/api/sienge/obras/kanban/{task_id}")
async def api_update_obras_kanban(task_id: int, request: Request) -> Any:
    body = await request.json()
    updates = []
    params = []
    
    for key in ["title", "description", "status", "progress_pct", "drive_link"]:
        if key in body:
            updates.append(f"{key} = ?")
            params.append(body[key])
            
    if not updates:
        return {"success": False, "message": "Nothing to update"}
        
    updates.append("updated_at = ?")
    params.append(now_iso())
    params.append(task_id)

    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"UPDATE obras_kanban SET {', '.join(updates)} WHERE id = ?", tuple(params))
    conn.commit()
    conn.close()
    return {"success": True}

@app.delete("/api/sienge/obras/kanban/{task_id}")
def api_delete_obras_kanban(task_id: int) -> Any:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM obras_kanban WHERE id = ?", (task_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@app.get("/api/sienge/usuarios")
def api_usuarios() -> Any:
    cached = read_dataset_cache("usuarios") or read_from_file("usuarios.json")
    if cached:
        return cached
    response = sienge_get(f"{DETECTED_PREFIX}/users")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/credores")
def api_credores() -> Any:
    cached = read_from_file("credores.json") or read_dataset_cache("credores")
    if cached:
        items = to_array(cached)
        return {
            "results": [
                {
                    **credor,
                    "id": credor.get("id"),
                    "code": str(credor.get("code") or credor.get("id") or ""),
                    "name": resolve_creditor_name_from_caches(
                        str(credor.get("id")), normalize_creditor_name(credor, str(credor.get("id")))
                    ),
                    "nome": resolve_creditor_name_from_caches(
                        str(credor.get("id")), normalize_creditor_name(credor, str(credor.get("id")))
                    ),
                }
                for credor in items
            ]
        }
    response = sienge_get(f"{DETECTED_PREFIX}/creditors")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/companies")
def api_companies() -> Any:
    cached = read_dataset_cache("empresas") or read_from_file("empresas.json")
    if cached:
        return cached
    response = sienge_get(f"{DETECTED_PREFIX}/companies")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/clientes")
def api_clientes() -> Any:
    cached = read_dataset_cache("clientes") or read_from_file("clientes.json")
    if cached:
        return cached
    response = sienge_get(f"{DETECTED_PREFIX}/clientes")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/pedidos-compra")
def api_pedidos_compra(request: Request) -> Any:
    return passthrough_or_cached("pedidos", "pedidos.json", f"{DETECTED_PREFIX}/purchase-orders", request)


@app.get("/api/sienge/pedidos-compra/{order_id}/itens")
def api_pedido_itens(order_id: str) -> Any:
    cached_items = read_dataset_cache("itens_pedidos") or read_from_file("itens_pedidos.json") or {}
    if isinstance(cached_items, dict) and cached_items.get(str(order_id)):
        return cached_items[str(order_id)]

    response = sienge_get(f"{DETECTED_PREFIX}/purchase-orders/{order_id}/items")
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/extrato")
def api_extrato(request: Request) -> Any:
    response = sienge_get(f"{DETECTED_PREFIX}/extratos-bancarios", dict(request.query_params))
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content=response.json() if response.content else {"error": response.text})
    return response.json()


@app.get("/api/sienge/logistics/locations")
def api_logistics_locations() -> Any:
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT id, code, name, address, latitude, longitude, type, source
        FROM app_locations
        ORDER BY
          CASE WHEN type = 'hq' THEN 0 WHEN type = 'building' THEN 1 ELSE 2 END,
          name COLLATE NOCASE ASC
        """
    ).fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    return {"results": result}


@app.post("/api/sienge/logistics/route-distance")
async def api_route_distance(request: Request) -> Any:
    body = await request.json()
    origin = (body or {}).get("origin") or {}
    destination = (body or {}).get("destination") or {}

    if not origin.get("address") and (origin.get("latitude") is None or origin.get("longitude") is None):
        raise HTTPException(status_code=400, detail="Origem da rota e obrigatoria.")
    if not destination.get("address") and (
        destination.get("latitude") is None or destination.get("longitude") is None
    ):
        raise HTTPException(status_code=400, detail="Destino da rota e obrigatorio.")

    return calculate_route_distance(origin, destination)


@app.post("/api/sienge/logistics/locations")
async def api_create_location(request: Request) -> Any:
    body = await request.json()
    name = str((body or {}).get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nome do local e obrigatorio.")

    now = now_iso()
    code = str((body or {}).get("code") or f"custom-{int(time.time() * 1000)}").strip()

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO app_locations (code, name, address, latitude, longitude, type, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          name = excluded.name,
          address = excluded.address,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          type = excluded.type,
          source = excluded.source,
          updated_at = excluded.updated_at
        """,
        (
            code,
            name,
            str((body or {}).get("address") or "").strip(),
            (body or {}).get("latitude"),
            (body or {}).get("longitude"),
            str((body or {}).get("type") or "custom"),
            str((body or {}).get("source") or "manual"),
            now,
            now,
        ),
    )
    conn.commit()
    inserted = cur.execute(
        "SELECT id, code, name, address, latitude, longitude, type, source FROM app_locations WHERE code = ?",
        (code,),
    ).fetchone()
    conn.close()

    return {"success": True, "location": dict(inserted) if inserted else None}


@app.post("/api/auth/login")
async def api_auth_login(request: Request) -> Any:
    body = await request.json()
    email = str((body or {}).get("email") or "").strip().lower()
    password = str((body or {}).get("password") or "")

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email e senha sao obrigatorios.")

    conn = get_db()
    cur = conn.cursor()
    user = cur.execute(
        """
        SELECT id, username, email, password_hash, name, role, department, active
        FROM app_users
        WHERE lower(email) = ? OR lower(username) = ?
        LIMIT 1
        """,
        (email, email),
    ).fetchone()
    conn.close()

    if not user or not user[7] or user[3] != hash_password(password):
        raise HTTPException(status_code=401, detail="Credenciais invalidas.")

    return {
        "user": {
            "id": user[0],
            "username": user[1],
            "email": user[2],
            "name": user[4],
            "role": user[5],
            "department": user[6] or "",
        }
    }


@app.post("/api/auth/register")
async def api_auth_register(request: Request) -> Any:
    body = await request.json()
    name = str((body or {}).get("name") or "").strip()
    email = str((body or {}).get("email") or "").strip().lower()
    department = str((body or {}).get("department") or "").strip()
    role_input = str((body or {}).get("role") or "").strip().lower()
    role = role_input if role_input in {"developer", "admin", "user"} else ""

    if not name or not email or not department or not role:
        raise HTTPException(status_code=400, detail="Nome, email, setor e perfil sao obrigatorios.")

    conn = get_db()
    cur = conn.cursor()
    exists = cur.execute(
        "SELECT id FROM app_users WHERE lower(email) = ? OR lower(username) = ? LIMIT 1",
        (email, email),
    ).fetchone()
    if exists:
        conn.close()
        raise HTTPException(status_code=409, detail="Ja existe um usuario cadastrado com este email.")

    now = now_iso()
    temp_password = "123456"
    cur.execute(
        """
        INSERT INTO app_users (username, email, password_hash, name, role, department, active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (email, email, hash_password(temp_password), name, role, department, 1, now, now),
    )
    user_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {
        "success": True,
        "tempPassword": temp_password,
        "user": {
            "id": int(user_id),
            "username": email,
            "email": email,
            "name": name,
            "role": role,
            "department": department,
        },
    }


def _run_boot_sync() -> None:
    try:
        sync_all_data()
    except Exception:
        pass


def _run_interval_sync() -> None:
    auto_sync_interval = os.getenv("AUTO_SYNC_INTERVAL", "true") != "false"
    is_production = os.getenv("NODE_ENV") == "production"
    if not auto_sync_interval and not is_production:
        return

    while True:
        time.sleep(20 * 60)
        try:
            sync_all_data()
        except Exception:
            continue




# ─────────────────────────────────────────────────────────────────
# KANBAN DE OBRAS — Rotas internas (sem Sienge)
# ─────────────────────────────────────────────────────────────────

UPLOADS_DIR = DATA_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

def _gen_id() -> str:
    return str(uuid.uuid4())


def _kanban_sprints_for_building(building_id: str) -> list[dict]:
    conn = get_db()
    cur = conn.cursor()
    sprints = cur.execute(
        "SELECT id, building_id, name, start_date, end_date, color, created_at, updated_at "
        "FROM kanban_sprints WHERE building_id = ? ORDER BY created_at ASC",
        (building_id,),
    ).fetchall()
    result = []
    for s in sprints:
        cards = cur.execute(
            "SELECT id, sprint_id, building_id, title, description, status, priority, "
            "responsible, due_date, tags, attachments, created_by, created_at, updated_at "
            "FROM kanban_cards WHERE sprint_id = ? ORDER BY created_at ASC",
            (s[0],),
        ).fetchall()
        sprint_cards = []
        for c in cards:
            try:
                tags = json.loads(c[9] or "[]")
            except Exception:
                tags = []
            try:
                attachments = json.loads(c[10] or "[]")
            except Exception:
                attachments = []
            sprint_cards.append({
                "id": c[0], "sprintId": c[1], "buildingId": c[2],
                "title": c[3], "description": c[4], "status": c[5],
                "priority": c[6], "responsible": c[7], "dueDate": c[8],
                "tags": tags, "attachments": attachments,
                "createdBy": c[11], "createdAt": c[12], "updatedAt": c[13],
            })
        result.append({
            "id": s[0], "buildingId": s[1], "name": s[2],
            "startDate": s[3], "endDate": s[4], "color": s[5],
            "createdAt": s[6], "updatedAt": s[7],
            "cards": sprint_cards,
        })
    conn.close()
    return result


@app.get("/api/kanban")
async def api_kanban_get(building_id: str = "") -> Any:
    """Returns all sprints and cards for a building (or all buildings)."""
    conn = get_db()
    cur = conn.cursor()
    if building_id:
        rows = cur.execute(
            "SELECT DISTINCT building_id FROM kanban_sprints WHERE building_id = ?",
            (building_id,),
        ).fetchall()
    else:
        rows = cur.execute("SELECT DISTINCT building_id FROM kanban_sprints").fetchall()
    conn.close()

    buildings_data: dict[str, list] = {}
    for row in rows:
        bid = row[0]
        buildings_data[bid] = _kanban_sprints_for_building(bid)
    return {"buildings": buildings_data}


@app.post("/api/kanban/sprint")
async def api_kanban_create_sprint(request: Request) -> Any:
    body = await request.json()
    building_id = str((body or {}).get("buildingId") or "").strip()
    name = str((body or {}).get("name") or "").strip()
    start_date = str((body or {}).get("startDate") or "").strip()
    end_date = str((body or {}).get("endDate") or "").strip()
    color = str((body or {}).get("color") or "#f97316").strip()

    if not building_id or not name:
        raise HTTPException(status_code=400, detail="buildingId e name sao obrigatorios.")

    sprint_id = _gen_id()
    now = now_iso()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO kanban_sprints (id, building_id, name, start_date, end_date, color, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (sprint_id, building_id, name, start_date, end_date, color, now, now),
    )
    conn.commit()
    conn.close()
    return {"success": True, "id": sprint_id}


@app.patch("/api/kanban/sprint/{sprint_id}")
async def api_kanban_update_sprint(sprint_id: str, request: Request) -> Any:
    body = await request.json()
    name = str((body or {}).get("name") or "").strip()
    start_date = str((body or {}).get("startDate") or "").strip()
    end_date = str((body or {}).get("endDate") or "").strip()
    color = str((body or {}).get("color") or "").strip()

    conn = get_db()
    cur = conn.cursor()
    fields, vals = [], []
    if name:
        fields.append("name = ?"); vals.append(name)
    if start_date is not None:
        fields.append("start_date = ?"); vals.append(start_date)
    if end_date is not None:
        fields.append("end_date = ?"); vals.append(end_date)
    if color:
        fields.append("color = ?"); vals.append(color)
    if not fields:
        conn.close()
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar.")
    fields.append("updated_at = ?"); vals.append(now_iso())
    vals.append(sprint_id)
    cur.execute(f"UPDATE kanban_sprints SET {', '.join(fields)} WHERE id = ?", vals)
    conn.commit()
    conn.close()
    return {"success": True}


@app.delete("/api/kanban/sprint/{sprint_id}")
async def api_kanban_delete_sprint(sprint_id: str) -> Any:
    conn = get_db()
    cur = conn.cursor()
    # Also delete related uploads
    cards = cur.execute(
        "SELECT attachments FROM kanban_cards WHERE sprint_id = ?", (sprint_id,)
    ).fetchall()
    for row in cards:
        try:
            attachments = json.loads(row[0] or "[]")
            for att in attachments:
                fpath = UPLOADS_DIR / str(att.get("filename", ""))
                if fpath.exists():
                    fpath.unlink(missing_ok=True)
        except Exception:
            pass
    cur.execute("DELETE FROM kanban_cards WHERE sprint_id = ?", (sprint_id,))
    cur.execute("DELETE FROM kanban_sprints WHERE id = ?", (sprint_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/kanban/card")
async def api_kanban_create_card(request: Request) -> Any:
    body = await request.json()
    sprint_id = str((body or {}).get("sprintId") or "").strip()
    building_id = str((body or {}).get("buildingId") or "").strip()
    title = str((body or {}).get("title") or "").strip()
    description = str((body or {}).get("description") or "").strip()
    status = str((body or {}).get("status") or "planned").strip()
    priority = str((body or {}).get("priority") or "medium").strip()
    responsible = str((body or {}).get("responsible") or "").strip()
    due_date = str((body or {}).get("dueDate") or "").strip()
    tags = json.dumps((body or {}).get("tags") or [], ensure_ascii=False)
    created_by = str((body or {}).get("createdBy") or "").strip()

    if not sprint_id or not title:
        raise HTTPException(status_code=400, detail="sprintId e title sao obrigatorios.")

    card_id = _gen_id()
    now = now_iso()
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO kanban_cards (id, sprint_id, building_id, title, description, status, "
        "priority, responsible, due_date, tags, attachments, created_by, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (card_id, sprint_id, building_id, title, description, status, priority,
         responsible, due_date, tags, "[]", created_by, now, now),
    )
    conn.commit()
    conn.close()
    return {"success": True, "id": card_id}


@app.patch("/api/kanban/card/{card_id}")
async def api_kanban_update_card(card_id: str, request: Request) -> Any:
    body = await request.json()
    conn = get_db()
    cur = conn.cursor()
    fields, vals = [], []
    for key, col in [
        ("title", "title"), ("description", "description"), ("status", "status"),
        ("priority", "priority"), ("responsible", "responsible"), ("dueDate", "due_date"),
        ("sprintId", "sprint_id"),
    ]:
        if key in (body or {}):
            fields.append(f"{col} = ?"); vals.append(str(body[key] or ""))
    if "tags" in (body or {}):
        fields.append("tags = ?"); vals.append(json.dumps(body["tags"] or [], ensure_ascii=False))
    if "attachments" in (body or {}):
        fields.append("attachments = ?"); vals.append(json.dumps(body["attachments"] or [], ensure_ascii=False))
    if not fields:
        conn.close()
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar.")
    fields.append("updated_at = ?"); vals.append(now_iso())
    vals.append(card_id)
    cur.execute(f"UPDATE kanban_cards SET {', '.join(fields)} WHERE id = ?", vals)
    conn.commit()
    conn.close()
    return {"success": True}


@app.delete("/api/kanban/card/{card_id}")
async def api_kanban_delete_card(card_id: str) -> Any:
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT attachments FROM kanban_cards WHERE id = ?", (card_id,)).fetchone()
    if row:
        try:
            attachments = json.loads(row[0] or "[]")
            for att in attachments:
                fpath = UPLOADS_DIR / str(att.get("filename", ""))
                if fpath.exists():
                    fpath.unlink(missing_ok=True)
        except Exception:
            pass
    cur.execute("DELETE FROM kanban_cards WHERE id = ?", (card_id,))
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/kanban/upload")
async def api_kanban_upload(
    card_id: str,
    file: UploadFile = File(...),
) -> Any:
    allowed_types = {
        "image/jpeg", "image/png", "image/gif", "image/webp",
        "video/mp4", "video/webm", "video/quicktime",
        "application/pdf",
        "application/xml", "text/xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    content_type = file.content_type or "application/octet-stream"
    if content_type not in allowed_types:
        raise HTTPException(status_code=415, detail=f"Tipo de arquivo nao permitido: {content_type}")

    max_size = 50 * 1024 * 1024  # 50 MB
    file_data = await file.read()
    if len(file_data) > max_size:
        raise HTTPException(status_code=413, detail="Arquivo muito grande (max 50 MB).")

    ext = Path(file.filename or "file").suffix or ".bin"
    filename = f"{card_id}_{_gen_id()}{ext}"
    dest = UPLOADS_DIR / filename
    dest.write_bytes(file_data)

    file_type = "image" if content_type.startswith("image/") else (
        "video" if content_type.startswith("video/") else "document"
    )

    attachment = {
        "id": _gen_id(),
        "filename": filename,
        "originalName": file.filename or filename,
        "type": file_type,
        "contentType": content_type,
        "url": f"/api/kanban/files/{filename}",
        "uploadedAt": now_iso(),
    }

    # Persist attachment to card
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT attachments FROM kanban_cards WHERE id = ?", (card_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Card nao encontrado.")
    try:
        existing = json.loads(row[0] or "[]")
    except Exception:
        existing = []
    existing.append(attachment)
    cur.execute(
        "UPDATE kanban_cards SET attachments = ?, updated_at = ? WHERE id = ?",
        (json.dumps(existing, ensure_ascii=False), now_iso(), card_id),
    )
    conn.commit()
    conn.close()
    return {"success": True, "attachment": attachment}


@app.delete("/api/kanban/upload")
async def api_kanban_delete_upload(card_id: str, filename: str) -> Any:
    conn = get_db()
    cur = conn.cursor()
    row = cur.execute("SELECT attachments FROM kanban_cards WHERE id = ?", (card_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Card nao encontrado.")
    try:
        existing = json.loads(row[0] or "[]")
    except Exception:
        existing = []
    existing = [a for a in existing if a.get("filename") != filename]
    cur.execute(
        "UPDATE kanban_cards SET attachments = ?, updated_at = ? WHERE id = ?",
        (json.dumps(existing, ensure_ascii=False), now_iso(), card_id),
    )
    conn.commit()
    conn.close()
    fpath = UPLOADS_DIR / filename
    fpath.unlink(missing_ok=True)
    return {"success": True}


@app.get("/api/kanban/files/{filename}")
async def api_kanban_serve_file(filename: str) -> Any:
    fpath = UPLOADS_DIR / filename
    if not fpath.exists():
        raise HTTPException(status_code=404, detail="Arquivo nao encontrado.")
    media_type, _ = mimetypes.guess_type(str(fpath))
    return FileResponse(str(fpath), media_type=media_type or "application/octet-stream")

dist_path = BASE_DIR / "dist"
if dist_path.exists():
    app.mount("/", OptimizedStaticFiles(directory=dist_path, html=True), name="dist")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
