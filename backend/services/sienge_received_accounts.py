from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backend.config import BASE_DIR
from backend.services.sienge_cache import utc_now_iso
from backend.services.sienge_client import SiengeClient, sienge_client
from backend.services.sienge_storage import read_snapshot, write_snapshot


TXT_ROOT = BASE_DIR / "assets" / "camada APiteste" / "txt"
RECEIVED_BY_BUILDING_TXT = TXT_ROOT / "recebidas_por_obra_sienge.txt"


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    return SiengeClient._extract_collection(payload)


async def _fetch_pages(
    client: SiengeClient,
    endpoint: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as http:
        while True:
            page_params = {**(params or {}), "limit": limit, "offset": offset}
            payload = await client._get_json_via_client(http, endpoint, page_params)
            page = _extract_rows(payload)
            if not page:
                break
            rows.extend(page)
            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            offset += len(page)
            if len(page) < limit or (isinstance(count, int) and offset >= count):
                break
            await asyncio.sleep(0.03)
    return rows


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _merge_titles(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in existing:
        if not isinstance(item, dict):
            continue
        key = _text(_first(item.get("billId"), item.get("id")))
        if key:
            merged[key] = item
    for item in incoming:
        if not isinstance(item, dict):
            continue
        key = _text(_first(item.get("billId"), item.get("id")))
        if key:
            merged[key] = {**merged.get(key, {}), **item}
    rows = list(merged.values())
    rows.sort(key=lambda item: (_text(item.get("companyId")), _text(item.get("buildingId")), _text(item.get("dueDate")), _text(item.get("id"))))
    return rows


def _company_name_map(db: Session) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in read_snapshot(db, "empresas.json", default=[]) or []:
        if not isinstance(item, dict):
            continue
        cid = _text(_first(item.get("id"), item.get("companyId"), item.get("codigo")))
        name = _text(_first(item.get("name"), item.get("nome"), item.get("fantasyName"), item.get("razaoSocial")))
        if cid and name:
            out[cid] = name
    return out


async def _fetch_detail_and_installments(
    client: SiengeClient,
    http: httpx.AsyncClient,
    bill_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    detail: dict[str, Any] = {}
    installments: list[dict[str, Any]] = []
    for attempt in range(3):
        detail_payload, installments_payload = await asyncio.gather(
            client._get_json_via_client(http, f"/accounts-receivable/receivable-bills/{bill_id}"),
            client._get_json_via_client(
                http,
                f"/accounts-receivable/receivable-bills/{bill_id}/installments",
                {"limit": 100, "offset": 0},
            ),
        )
        detail = detail_payload if isinstance(detail_payload, dict) else {}
        installments = _extract_rows(installments_payload)
        if detail and _text(_first(detail.get("enterpriseCode"), detail.get("buildingId"), detail.get("enterpriseId"))):
            break
        await asyncio.sleep(0.2 * (attempt + 1))
    return detail, installments


async def _fetch_received_titles_by_building(client: SiengeClient) -> list[dict[str, Any]]:
    titles = await _fetch_pages(client, "/accounts-receivable/receivable-bills")
    semaphore = asyncio.Semaphore(4)
    rows: list[dict[str, Any]] = []

    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0), limits=limits) as http:
        async def consume(title: dict[str, Any]) -> None:
            bill_id = _text(_first(title.get("receivableBillId"), title.get("id"), title.get("billId")))
            if not bill_id:
                return
            async with semaphore:
                detail, installments = await _fetch_detail_and_installments(client, http, bill_id)

            source = {**title, **detail}
            building_id = _text(_first(source.get("enterpriseCode"), source.get("buildingId"), source.get("enterpriseId")))
            if not building_id:
                return

            total_value = _safe_float(_first(source.get("receivableBillValue"), source.get("value"), source.get("totalValue")))
            if total_value <= 0:
                return

            balance_due = sum(_safe_float(item.get("balanceDue")) for item in installments if isinstance(item, dict))
            paid_value = max(0.0, total_value - balance_due)
            if paid_value <= 0:
                return

            due_dates = [_text(item.get("dueDate")) for item in installments if isinstance(item, dict) and _text(item.get("dueDate"))]
            rows.append(
                {
                    "id": bill_id,
                    "billId": bill_id,
                    "companyId": _text(source.get("companyId")),
                    "customerId": source.get("customerId"),
                    "buildingId": building_id,
                    "buildingName": _text(_first(source.get("enterpriseName"), source.get("buildingName"), f"Obra {building_id}")),
                    "documentId": _text(source.get("documentId")),
                    "documentNumber": _text(source.get("documentNumber")),
                    "issueDate": _text(source.get("issueDate")),
                    "dueDate": min(due_dates) if due_dates else _text(_first(source.get("dueDate"), source.get("issueDate"))),
                    "payOffDate": _text(source.get("payOffDate")),
                    "grossValue": total_value,
                    "balanceDue": balance_due,
                    "receivedValue": paid_value,
                    "note": _text(source.get("note")),
                    "source": "sienge_receivable_bills_detail",
                }
            )

        valid_titles = [title for title in titles if isinstance(title, dict)]
        await asyncio.gather(*(consume(title) for title in valid_titles))

        captured_ids = {_text(_first(row.get("billId"), row.get("id"))) for row in rows}
        retry_titles = [
            title
            for title in valid_titles
            if _text(_first(title.get("receivableBillId"), title.get("id"), title.get("billId"))) not in captured_ids
        ]
        for title in retry_titles:
            await consume(title)

    rows.sort(key=lambda item: (_text(item.get("companyId")), _text(item.get("buildingId")), _text(item.get("dueDate")), _text(item.get("id"))))
    return rows


def build_received_aggregate(
    db: Session,
    received_titles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    company_names = _company_name_map(db)
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for title in received_titles:
        company_id = _text(title.get("companyId"))
        building_id = _text(title.get("buildingId")) or "0"
        if not company_id or not building_id:
            continue
        key = (company_id, building_id)
        bucket = grouped.setdefault(
            key,
            {
                "companyId": company_id,
                "companyName": company_names.get(company_id) or f"Empresa {company_id}",
                "buildingId": building_id,
                "buildingName": _text(title.get("buildingName")) or ("Sem obra" if building_id == "0" else f"Obra {building_id}"),
                "receita": 0.0,
                "grossReceita": 0.0,
                "balanceDue": 0.0,
                "titleCount": 0,
                "source": "sienge_received_accounts_by_building",
            },
        )
        bucket["receita"] += _safe_float(title.get("receivedValue"))
        bucket["grossReceita"] += _safe_float(title.get("grossValue"))
        bucket["balanceDue"] += _safe_float(title.get("balanceDue"))
        bucket["titleCount"] += 1
        if _text(title.get("buildingName")) and bucket["buildingName"].startswith("Obra "):
            bucket["buildingName"] = _text(title.get("buildingName"))

    rows = list(grouped.values())
    rows.sort(key=lambda item: (-_safe_float(item.get("receita")), _text(item.get("companyId")), _text(item.get("buildingName"))))
    for row in rows:
        row["receita"] = round(_safe_float(row.get("receita")), 2)
        row["grossReceita"] = round(_safe_float(row.get("grossReceita")), 2)
        row["balanceDue"] = round(_safe_float(row.get("balanceDue")), 2)
    return rows


def apply_received_revenue_to_mc_rows(
    db: Session,
    received_aggregate: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if received_aggregate is None:
        received_aggregate = read_snapshot(db, "recebidas_por_obra.json", default=[]) or []
    if not isinstance(received_aggregate, list) or not received_aggregate:
        return read_snapshot(db, "mc_company_building.json", default=[]) or []

    base_rows = read_snapshot(db, "mc_company_building.json", default=[]) or []
    existing: dict[tuple[str, str], dict[str, Any]] = {}
    for row in base_rows:
        if not isinstance(row, dict):
            continue
        key = (_text(row.get("companyId")), _text(row.get("buildingId")) or "0")
        existing[key] = dict(row)

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in received_aggregate:
        if not isinstance(item, dict):
            continue
        key = (_text(item.get("companyId")), _text(item.get("buildingId")) or "0")
        if not key[0]:
            continue
        base = existing.get(key, {})
        receita = round(_safe_float(item.get("receita")), 2)
        custo = round(_safe_float(base.get("custo")), 2)
        mc = round(receita - custo, 2)
        pct = round((mc / receita * 100.0) if receita > 0 else 0.0, 2)
        out.append(
            {
                **base,
                "companyId": key[0],
                "companyName": base.get("companyName") or item.get("companyName") or f"Empresa {key[0]}",
                "buildingId": key[1],
                "buildingName": item.get("buildingName") or base.get("buildingName") or f"Obra {key[1]}",
                "receita": receita,
                "grossReceita": round(_safe_float(item.get("grossReceita")), 2),
                "balanceDue": round(_safe_float(item.get("balanceDue")), 2),
                "custo": custo,
                "mc": mc,
                "pct": pct,
                "titleCount": item.get("titleCount"),
                "source": "sienge_received_accounts_by_building",
                "previousRevenueSource": base.get("source"),
            }
        )
        seen.add(key)

    for key, base in existing.items():
        if key in seen:
            continue
        out.append(base)

    out.sort(key=lambda item: (-_safe_float(item.get("receita")), _text(item.get("companyName")), _text(item.get("buildingName"))))
    write_snapshot(db, "mc_company_building.json", out)
    return out


def _write_txt(aggregate: list[dict[str, Any]]) -> None:
    try:
        TXT_ROOT.mkdir(parents=True, exist_ok=True)
        lines = ["ID_EMPRESA;EMPRESA;ID_OBRA;OBRA;RECEITA_RECEBIDA;RECEITA_BRUTA;SALDO_ABERTO;TITULOS"]
        for item in aggregate:
            lines.append(
                ";".join(
                    [
                        _text(item.get("companyId")),
                        _text(item.get("companyName")),
                        _text(item.get("buildingId")),
                        _text(item.get("buildingName")),
                        f"{_safe_float(item.get('receita')):.2f}",
                        f"{_safe_float(item.get('grossReceita')):.2f}",
                        f"{_safe_float(item.get('balanceDue')):.2f}",
                        _text(item.get("titleCount")),
                    ]
                )
            )
        RECEIVED_BY_BUILDING_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception:
        return


async def sync_received_accounts_by_building_from_sienge(db: Session) -> dict[str, Any]:
    if not sienge_client.is_configured:
        return {"ok": False, "reason": "sienge_not_configured"}

    fetched_titles = await _fetch_received_titles_by_building(sienge_client)
    existing_titles = read_snapshot(db, "recebidas_titulos_por_obra.json", default=[]) or []
    titles = _merge_titles(existing_titles if isinstance(existing_titles, list) else [], fetched_titles)
    aggregate = build_received_aggregate(db, titles)
    mc_rows = apply_received_revenue_to_mc_rows(db, aggregate)
    meta = {
        "ok": True,
        "source": "sienge_live",
        "synced_at": utc_now_iso(),
        "today": date.today().isoformat(),
        "counts": {
            "recebidas_titulos": len(titles),
            "recebidas_por_obra": len(aggregate),
            "mc_company_building": len(mc_rows),
        },
    }
    write_snapshot(db, "recebidas_titulos_por_obra.json", titles)
    write_snapshot(db, "recebidas_por_obra.json", aggregate)
    write_snapshot(db, "sienge_received_accounts_meta", meta)
    _write_txt(aggregate)
    return meta
