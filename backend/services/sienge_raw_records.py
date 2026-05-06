from __future__ import annotations

import json
import hashlib
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy.orm import Session

from backend.models import SiengeRawRecord


def _stable_record_id(record: dict[str, Any], *, id_fields: Iterable[str]) -> str:
    for field in id_fields:
        value = record.get(field)
        if value is None:
            continue
        s = str(value).strip()
        if s and s not in {"None", "null", "undefined"}:
            return s

    raw = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
    return "sha1_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def upsert_raw_records(
    db: Session,
    *,
    dataset: str,
    records: list[dict[str, Any]],
    id_fields: Iterable[str] = ("id", "documentId", "document_id", "number", "codigo", "code"),
) -> dict[str, Any]:
    """Armazena registros brutos do SIENGE em `sienge_raw_records`.

    Estratégia: upsert via `Session.merge()` (compatível com SQLite/Postgres).
    Para bases grandes, é mais lento que ON CONFLICT, mas é robusto e simples.
    """

    dataset = str(dataset or "").strip() or "unknown"
    inserted_or_updated = 0
    now = datetime.utcnow()

    for rec in records:
        if not isinstance(rec, dict):
            continue
        rid = _stable_record_id(rec, id_fields=id_fields)
        db.merge(
            SiengeRawRecord(
                dataset=dataset,
                record_id=rid,
                payload=json.dumps(rec, ensure_ascii=False, default=str),
                created_at=now,
                updated_at=now,
            )
        )
        inserted_or_updated += 1

    db.commit()
    return {"ok": True, "dataset": dataset, "upserted": inserted_or_updated}
