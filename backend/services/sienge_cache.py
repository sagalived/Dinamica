import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.config import DATA_DIR


def _cache_path(filename: str) -> Path:
    return DATA_DIR / filename


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as tmp_file:
        json.dump(payload, tmp_file, ensure_ascii=False, indent=2)
        temp_name = tmp_file.name
    os.replace(temp_name, path)


def read_json_cache(filename: str, default: Any = None) -> Any:
    path = _cache_path(filename)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json_cache(filename: str, payload: Any) -> None:
    _write_json_atomic(_cache_path(filename), payload)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_sync_metadata() -> dict[str, Any] | None:
    metadata = read_json_cache("sienge_sync_meta.json", default=None)
    return metadata if isinstance(metadata, dict) else None


def write_sync_metadata(metadata: dict[str, Any]) -> None:
    write_json_cache("sienge_sync_meta.json", metadata)
