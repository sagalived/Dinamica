from __future__ import annotations

import json
import hashlib
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.models import SiengeNfeDocument
from backend.services.sienge_client import sienge_client


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_yyyy_mm_dd(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    # ISO
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # yyyy-mm-dd
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]

    # dd/mm/yyyy
    try:
        dt = datetime.strptime(raw[:10], "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _extract_results(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return [x for x in data["results"] if isinstance(x, dict)]
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
    return []


def _extract_total_count(payload: Any) -> int | None:
    if not isinstance(payload, dict):
        return None
    meta = payload.get("resultSetMetadata")
    if isinstance(meta, dict) and isinstance(meta.get("count"), int):
        return int(meta["count"])
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("resultSetMetadata"), dict):
        meta2 = data.get("resultSetMetadata")
        if isinstance(meta2.get("count"), int):
            return int(meta2["count"])
    return None


def _stable_document_id(doc: dict[str, Any]) -> str:
    candidates = [
        doc.get("documentId"),
        doc.get("id"),
        doc.get("document_id"),
    ]
    for c in candidates:
        s = str(c or "").strip()
        if s and s not in {"None", "undefined", "null"}:
            return s

    # fallback: assinatura estável com campos típicos
    sig = {
        "companyId": doc.get("companyId") or doc.get("company_id"),
        "supplierId": doc.get("supplierId") or doc.get("supplier_id"),
        "issueDate": doc.get("issueDate") or doc.get("emissionDate") or doc.get("dataEmissao"),
        "series": doc.get("series"),
        "number": doc.get("number") or doc.get("invoiceNumber") or doc.get("documentNumber"),
        "total": doc.get("totalInvoiceAmount") or doc.get("totalAmount") or doc.get("amount") or doc.get("value") or doc.get("valorTotal"),
    }
    raw = json.dumps(sig, sort_keys=True, ensure_ascii=False)
    return "sig_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


async def sync_nfe_documents_range(
    *,
    db: Session,
    start_date: str,
    end_date: str,
    company_id: int | None = None,
    allow_updates: bool = False,
) -> dict[str, Any]:
    """Baixa NF-e no range e persiste no SQLite.

    - Por padrão (`allow_updates=False`): não sobrescreve registros existentes (imutável).
    - Para mês atual (`allow_updates=True`): atualiza registro se já existir.

    Retorna contagem inserida/atualizada.
    """

    if not sienge_client.is_configured:
        return {"ok": False, "inserted": 0, "updated": 0, "reason": "sienge_not_configured"}

    inserted = 0
    updated = 0

    dialect = "unknown"
    try:
        bind = db.get_bind()
        dialect = getattr(getattr(bind, "dialect", None), "name", "unknown") or "unknown"
    except Exception:
        dialect = "unknown"

    def _insert_stmt():
        if dialect == "postgresql":
            return pg_insert(SiengeNfeDocument)
        return sqlite_insert(SiengeNfeDocument)

    limit = 200
    offset = 0
    total_count: int | None = None

    while True:
        payload = await sienge_client.fetch_nfe_documents(
            startDate=start_date,
            endDate=end_date,
            limit=limit,
            offset=offset,
            companyId=company_id,
        )

        results = _extract_results(payload)
        if total_count is None:
            total_count = _extract_total_count(payload)

        if not results:
            break

        rows = []
        for doc in results:
            doc_id = _stable_document_id(doc)
            issue_date = _to_yyyy_mm_dd(doc.get("issueDate") or doc.get("emissionDate") or doc.get("dataEmissao") or doc.get("date"))
            company = doc.get("companyId") or doc.get("company_id")
            supplier = doc.get("supplierId") or doc.get("supplier_id")
            series = doc.get("series")
            number = doc.get("number") or doc.get("invoiceNumber") or doc.get("documentNumber")
            total_amount = _safe_float(
                doc.get("totalInvoiceAmount")
                or doc.get("totalAmount")
                or doc.get("total_amount")
                or doc.get("amount")
                or doc.get("value")
                or doc.get("valorTotal")
                or 0
            )

            rows.append(
                {
                    "document_id": doc_id,
                    "issue_date": issue_date,
                    "company_id": str(company).strip() if company is not None else None,
                    "supplier_id": str(supplier).strip() if supplier is not None else None,
                    "series": str(series).strip() if series is not None else None,
                    "number": str(number).strip() if number is not None else None,
                    "total_amount": float(total_amount or 0.0),
                    "payload": json.dumps(doc, ensure_ascii=False),
                }
            )

        if not rows:
            break

        if allow_updates:
            stmt = _insert_stmt().values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[SiengeNfeDocument.document_id],
                set_={
                    "issue_date": stmt.excluded.issue_date,
                    "company_id": stmt.excluded.company_id,
                    "supplier_id": stmt.excluded.supplier_id,
                    "series": stmt.excluded.series,
                    "number": stmt.excluded.number,
                    "total_amount": stmt.excluded.total_amount,
                    "payload": stmt.excluded.payload,
                    "updated_at": datetime.utcnow(),
                },
            )
            res = db.execute(stmt)
            # sqlite rowcount em upsert é pouco confiável, mas dá um sinal
            changed = int(getattr(res, "rowcount", 0) or 0)
            updated += max(0, changed)
        else:
            stmt = _insert_stmt().values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=[SiengeNfeDocument.document_id])
            res = db.execute(stmt)
            inserted += int(getattr(res, "rowcount", 0) or 0)

        db.commit()

        offset += len(results)
        if len(results) < limit:
            break
        if isinstance(total_count, int) and offset >= total_count:
            break

    return {"ok": True, "inserted": inserted, "updated": updated, "range": {"start": start_date, "end": end_date}}
