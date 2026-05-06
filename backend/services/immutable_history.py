from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.services.sienge_storage import read_snapshot, write_snapshot


_IMMUTABLE_META_KEY = "sienge_immutable_history_meta"


@dataclass(frozen=True)
class BackfillStatus:
    cursor_month: str
    target_month: str
    completed: bool


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _month_start_end(month: str) -> tuple[str, str]:
    # month: YYYY-MM
    y = int(month[:4])
    m = int(month[5:7])
    start = date(y, m, 1)
    # next month
    if m == 12:
        next_month = date(y + 1, 1, 1)
    else:
        next_month = date(y, m + 1, 1)
    end = next_month - timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _add_month(month: str, delta: int = 1) -> str:
    y = int(month[:4])
    m = int(month[5:7])
    total = (y * 12 + (m - 1)) + delta
    ny = total // 12
    nm = (total % 12) + 1
    return f"{ny:04d}-{nm:02d}"


def _last_complete_month(today: date | None = None) -> str:
    if today is None:
        today = date.today()
    first_this_month = date(today.year, today.month, 1)
    last_prev = first_this_month - timedelta(days=1)
    return _month_key(last_prev)


def get_immutable_backfill_status(db: Session, *, start_month: str = "2019-01") -> BackfillStatus:
    meta = read_snapshot(db, _IMMUTABLE_META_KEY, default={})
    if not isinstance(meta, dict):
        meta = {}

    cursor = str(meta.get("cursor_month") or start_month).strip() or start_month
    target = str(meta.get("target_month") or _last_complete_month()).strip() or _last_complete_month()

    completed = bool(meta.get("completed"))
    if cursor > target:
        completed = True

    return BackfillStatus(cursor_month=cursor, target_month=target, completed=completed)


def update_immutable_meta(
    db: Session,
    *,
    cursor_month: str,
    target_month: str,
    completed: bool,
    operational_rebuild_pending: bool,
    note: str | None = None,
) -> None:
    payload: dict[str, Any] = {
        "cursor_month": cursor_month,
        "target_month": target_month,
        "completed": bool(completed),
        "operational_rebuild_pending": bool(operational_rebuild_pending),
        "updated_at": datetime.utcnow().isoformat(),
    }
    if note:
        payload["note"] = note
    write_snapshot(db, _IMMUTABLE_META_KEY, payload)


def mark_operational_rebuild_done(db: Session) -> None:
    meta = read_snapshot(db, _IMMUTABLE_META_KEY, default={})
    if not isinstance(meta, dict):
        meta = {}
    meta["operational_rebuild_pending"] = False
    meta["operational_rebuild_done_at"] = datetime.utcnow().isoformat()
    write_snapshot(db, _IMMUTABLE_META_KEY, meta)
