from __future__ import annotations

from typing import Any
from datetime import datetime

import asyncio
import hashlib
import time
from collections import deque

import httpx
from sqlalchemy.orm import Session

from backend.repositories.sienge_snapshot_repository import SiengeSnapshotRepository
from backend.services.sienge_cache import utc_now_iso
from backend.services.sienge_client import sienge_client


def _to_array(payload: Any) -> list[dict]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"]
        if isinstance(payload.get("results"), list):
            return payload["results"]
    if isinstance(payload, list):
        return payload
    return []


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_building(item: dict[str, Any]) -> dict[str, Any]:
    code = item.get("code") or item.get("codigoVisivel") or item.get("codigo") or item.get("id")
    name = item.get("name") or item.get("nome") or item.get("enterpriseName") or f"Obra {code}"
    return {"id": item.get("id") or code, "name": name}


def _bill_buildings_cost_cache_key(bill_id: str) -> str:
    safe = "".join(ch for ch in str(bill_id) if ch.isdigit()) or str(bill_id)
    return f"bills_buildings_cost/{safe}.json"


def _extract_buildings_cost_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return [x for x in data["results"] if isinstance(x, dict)]
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
        if isinstance(payload.get("buildingsCost"), list):
            return [x for x in payload["buildingsCost"] if isinstance(x, dict)]
        if isinstance(payload.get("data"), list):
            return [x for x in payload["data"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def _weights_from_buildings_cost(rows: list[dict[str, Any]]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for r in rows:
        bid = r.get("buildingId") or r.get("enterpriseId") or r.get("idObra") or r.get("building")
        bid_str = str(bid or "").strip()
        if not bid_str or bid_str in {"None", "undefined", "null"}:
            continue

        pct = r.get("percentage") or r.get("costPercentage") or r.get("percent") or r.get("rate")
        val = r.get("value") or r.get("costValue") or r.get("amount") or r.get("valor")
        pct_f = _safe_float(pct, 0.0)
        if pct_f:
            out.append((bid_str, pct_f))
            continue

        val_f = _safe_float(val, 0.0)
        if val_f:
            out.append((bid_str, val_f))
            continue

        out.append((bid_str, 1.0))
    return out


async def compute_mc_by_building(
    *,
    filtered: dict[str, Any],
    db: Session,
    top: int,
    debug: bool,
    time_budget_seconds: int,
    max_concurrency: int,
) -> dict[str, Any]:
    repo = SiengeSnapshotRepository(db)

    receber = _to_array(filtered.get("receber", []))
    financeiro = _to_array(filtered.get("financeiro", []))
    obras = _to_array(repo.read("obras.json", []))

    building_name_by_id: dict[str, str] = {}
    building_company_by_alias: dict[str, str] = {}
    for obra in obras:
        if not isinstance(obra, dict):
            continue
        normalized = _normalize_building(obra)
        bid = str(normalized.get("id") or "").strip()
        if bid and bid not in {"None", "undefined"}:
            building_name_by_id[bid] = str(normalized.get("name") or bid)

        cid_raw = obra.get("companyId") or obra.get("idCompany") or obra.get("empresaId") or obra.get("company_id")
        cid = str(cid_raw or "").strip()
        if cid and cid not in {"None", "undefined", "null"}:
            id_candidates = {
                str(obra.get("id") or ""),
                str(obra.get("code") or ""),
                str(obra.get("codigoVisivel") or ""),
                str(obra.get("codigo") or ""),
            }
            for alias in id_candidates:
                alias_str = str(alias or "").strip()
                if alias_str and alias_str not in {"None", "undefined", "null"}:
                    building_company_by_alias[alias_str] = cid

    filters = filtered.get("filters", {}) if isinstance(filtered, dict) else {}
    selected_company_id = str(filters.get("company_id") or "all").strip() or "all"
    selected_building_id = str(filters.get("building_id") or "all").strip() or "all"

    building_aliases: set[str] | None = None
    if selected_building_id != "all":
        selected: dict[str, Any] | None = None
        for obra in obras:
            if not isinstance(obra, dict):
                continue
            if any(
                str(obra.get(k) or "").strip() == selected_building_id
                for k in ("id", "code", "codigoVisivel", "codigo")
            ):
                selected = obra
                break
        if selected is not None:
            building_aliases = {
                str(selected.get("id") or "").strip(),
                str(selected.get("code") or "").strip(),
                str(selected.get("codigoVisivel") or "").strip(),
                str(selected.get("codigo") or "").strip(),
            }
            building_aliases = {b for b in building_aliases if b and b not in {"None", "undefined", "null"}}
        else:
            building_aliases = {selected_building_id}

    def _is_nota_fiscal_income(item: dict[str, Any]) -> bool:
        typ = str(item.get("type") or "").strip().lower()
        if not typ or typ == "expense":
            return False
        doc_id = str(item.get("documentId") or "").strip().upper()
        doc_num = str(item.get("documentNumber") or "").strip().upper()
        return ("NF" in doc_id) or ("NF" in doc_num)

    def _should_ignore(item: dict[str, Any]) -> bool:
        statement_type = str(item.get("statementType") or item.get("operationType") or "").strip().lower()
        origin = str(item.get("statementOrigin") or item.get("origin") or "").strip().lower()
        # Ignora transferências e BC (movimentações bancárias que não entram no operacional)
        if "transf" in statement_type or "transfer" in statement_type:
            return True
        if "saque" in statement_type:
            return True
        if origin == "bc":
            return True
        return False

    def _is_expense(item: dict[str, Any]) -> bool:
        typ = str(item.get("type") or "").strip().lower()
        if typ == "expense":
            return True
        # fallback: alguns payloads podem vir só com valor negativo
        try:
            return float(item.get("rawValue") or 0) < 0
        except (TypeError, ValueError):
            return False

    def _amount_receber(item: dict[str, Any]) -> float:
        return _safe_float(item.get("rawValue") or item.get("amount") or item.get("valor") or item.get("value") or 0)

    def _amount_receber_abs(item: dict[str, Any]) -> float:
        return abs(_amount_receber(item))

    def _amount_financeiro(item: dict[str, Any]) -> float:
        return _safe_float(
            item.get("totalInvoiceAmount")
            or item.get("totalAmount")
            or item.get("amount")
            or item.get("valor")
            or item.get("value")
            or 0
        )

    def _item_building_id(item: dict[str, Any]) -> str | None:
        bid = (
            item.get("buildingId")
            or item.get("building_id")
            or item.get("buildingCode")
            or item.get("building_code")
            or item.get("enterpriseId")
            or item.get("enterprise_id")
            or item.get("idObra")
            or item.get("codigoObra")
            or item.get("codigoVisivelObra")
            or item.get("codigoVisivel")
            or item.get("obraId")
            or item.get("obra_id")
            or ""
        )
        bid_str = str(bid or "").strip()
        if not bid_str or bid_str in {"None", "undefined", "null"}:
            return None
        return bid_str

    receita_por_bill: dict[str, float] = {}
    receita_por_obra_direto: dict[str, float] = {}
    custo_por_bill: dict[str, float] = {}
    custo_por_obra_direto: dict[str, float] = {}
    fallback_buildings_by_bill: dict[str, set[str]] = {}
    for item in receber:
        if not isinstance(item, dict):
            continue
        if _should_ignore(item):
            continue
        bill_id = item.get("billId") or item.get("bill_id")
        bill_id_str = str(bill_id or "").strip()
        bid = _item_building_id(item)

        # Receita operacional: toda entrada (não Expense). Preferimos billId->rateio,
        # mas aceitamos alocação direta por obra quando não houver billId.
        if not _is_expense(item):
            amount = _amount_receber_abs(item)
            if amount == 0:
                continue
            if bid and not bill_id_str:
                receita_por_obra_direto[bid] = receita_por_obra_direto.get(bid, 0.0) + amount
                continue
            if not bill_id_str:
                continue
            receita_por_bill[bill_id_str] = receita_por_bill.get(bill_id_str, 0.0) + amount
            if bid:
                fallback_buildings_by_bill.setdefault(bill_id_str, set()).add(bid)
            continue

        # Custos: despesas do extrato (Expense). Idealmente rateadas por billId->buildings-cost.
        if _is_expense(item):
            amount = _amount_receber_abs(item)
            if amount == 0:
                continue
            if bid and not bill_id_str:
                custo_por_obra_direto[bid] = custo_por_obra_direto.get(bid, 0.0) + amount
                continue
            if not bill_id_str:
                continue
            custo_por_bill[bill_id_str] = custo_por_bill.get(bill_id_str, 0.0) + amount
            if bid:
                fallback_buildings_by_bill.setdefault(bill_id_str, set()).add(bid)
            continue

    # Mantém 'financeiro' ainda disponível para debug/diagnóstico (e para evoluções futuras),
    # mas o custo do MC por obra é calculado a partir do extrato (receber Expense) para
    # refletir melhor o período e evitar "MC=Receita" (100%) por falta de obra em /bills.

    receita_bill_ids = sorted(receita_por_bill.keys(), key=lambda b: float(receita_por_bill.get(b, 0.0)), reverse=True)
    custo_bill_ids = sorted(custo_por_bill.keys(), key=lambda b: float(custo_por_bill.get(b, 0.0)), reverse=True)
    bill_ids = sorted({*receita_bill_ids, *custo_bill_ids})

    if not bill_ids:
        return {
            "rows": [],
            "total": {"receita_operacional": 0.0, "mc": 0.0, "mc_percent": 0.0},
            "filters": filtered.get("filters", {}),
            "diagnostic": {
                "bills": 0,
                "cached": 0,
                "fetched": 0,
                "missing": 0,
                "sienge": {"configured": bool(getattr(sienge_client, "is_configured", False))},
            },
        }

    if not sienge_client.is_configured:
        diag: dict[str, Any] = {
            "status": "not_configured",
            "message": "Credenciais do Sienge não configuradas; não é possível calcular MC por obra via buildings-cost.",
            "bills": len(bill_ids),
            "cached": 0,
            "fetched": 0,
            "missing": len(bill_ids),
            "generated_at": utc_now_iso(),
            "sienge": {
                "configured": False,
                "base_url": getattr(sienge_client, "base_url", None),
            },
        }
        if debug:
            diag["receita_bills"] = len(receita_por_bill)
            diag["custo_bills"] = len(custo_por_bill)
        return {
            "rows": [],
            "total": {"receita_operacional": 0.0, "mc": 0.0, "mc_percent": 0.0},
            "filters": filtered.get("filters", {}),
            "diagnostic": diag,
        }

    cached_count = 0
    fetched_count = 0
    missing_count = 0
    status_counts: dict[str, int] = {}

    weights_by_bill: dict[str, list[tuple[str, float]]] = {}
    to_fetch_receita: list[str] = []
    to_fetch_custo: list[str] = []

    seen: set[str] = set()

    cursor_key = "bills_buildings_cost_cursor.json"
    cursor_payload = repo.read(cursor_key, {}) or {}
    try:
        cursor = int(cursor_payload.get("cursor") or 0)
    except (TypeError, ValueError):
        cursor = 0

    negative_404_ttl_s = 24 * 60 * 60

    auth_fp = hashlib.sha1(
        f"{getattr(sienge_client, 'access_name', '')}:{getattr(sienge_client, 'token', '')}".encode("utf-8")
    ).hexdigest()[:10]

    def _load_from_cache_or_queue(bid: str, queue: list[str]) -> None:
        nonlocal cached_count
        cache_key = _bill_buildings_cost_cache_key(bid)
        cached = repo.read(cache_key, None)
        if cached is None:
            queue.append(bid)
            return
        cached_count += 1
        if isinstance(cached, dict) and cached.get("_status") == 404:
            cached_at = str(cached.get("_cached_at") or "")
            cached_fp = str(cached.get("_auth_fp") or "")
            try:
                age_s = (
                    datetime.now() - datetime.fromisoformat(cached_at.replace("Z", "+00:00"))
                ).total_seconds()
            except Exception:
                age_s = None
            if cached_fp != auth_fp or age_s is None or age_s > negative_404_ttl_s:
                queue.append(bid)
            weights_by_bill[bid] = []
            return
        rows = _extract_buildings_cost_rows(cached)
        weights_by_bill[bid] = _weights_from_buildings_cost(rows)

    for bid in receita_bill_ids:
        if bid in seen:
            continue
        seen.add(bid)
        _load_from_cache_or_queue(bid, to_fetch_receita)
    for bid in custo_bill_ids:
        if bid in seen:
            continue
        seen.add(bid)
        _load_from_cache_or_queue(bid, to_fetch_custo)

    to_fetch: list[str] = []
    ri = 0
    ci = 0
    while ri < len(to_fetch_receita) or ci < len(to_fetch_custo):
        for _ in range(5):
            if ci < len(to_fetch_custo):
                to_fetch.append(to_fetch_custo[ci])
                ci += 1
        if ri < len(to_fetch_receita):
            to_fetch.append(to_fetch_receita[ri])
            ri += 1

    concurrency = max(2, min(int(max_concurrency), 50))
    sem = asyncio.Semaphore(concurrency)
    started_at = time.monotonic()

    min_interval_s = 0.35
    _rate_lock = asyncio.Lock()
    _next_allowed = 0.0

    async def _rate_limit() -> None:
        nonlocal _next_allowed
        async with _rate_lock:
            now = time.monotonic()
            if now < _next_allowed:
                await asyncio.sleep(_next_allowed - now)
            _next_allowed = time.monotonic() + min_interval_s

    processed_fetch = 0
    time_budget_hit = False

    if to_fetch:
        start = cursor % len(to_fetch)
        to_fetch = to_fetch[start:] + to_fetch[:start]

    async with httpx.AsyncClient(timeout=sienge_client.timeout) as http_client:

        async def fetch_one(bid: str) -> tuple[str, Any, dict[str, Any] | None]:
            async with sem:
                await _rate_limit()
                payload, err = await sienge_client.fetch_bill_buildings_cost_with_client_detailed(http_client, bid)
            return (bid, payload, err)

        q: deque[str] = deque(to_fetch)
        pending: set[asyncio.Task] = set()

        def _fill_pending() -> None:
            target = max(concurrency * 2, 4)
            while len(pending) < target and q:
                bid = q.popleft()
                pending.add(asyncio.create_task(fetch_one(bid)))

        _fill_pending()

        try:
            while pending:
                elapsed = (time.monotonic() - started_at)
                remaining = float(time_budget_seconds) - elapsed
                if remaining <= 0:
                    time_budget_hit = True
                    break

                done, pending = await asyncio.wait(
                    pending,
                    timeout=min(0.5, remaining),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for fut in done:
                    bid, payload, err = await fut
                    processed_fetch += 1
                    if payload is None:
                        missing_count += 1
                        code = None
                        if isinstance(err, dict):
                            code = err.get("status_code")
                        status_counts[str(code or "none")] = status_counts.get(str(code or "none"), 0) + 1
                        if code == 404:
                            cache_key = _bill_buildings_cost_cache_key(bid)
                            repo.write(
                                cache_key,
                                {
                                    "_status": 404,
                                    "_cached_at": utc_now_iso(),
                                    "_endpoint": err.get("endpoint") if err else None,
                                    "_auth_fp": auth_fp,
                                },
                            )
                        weights_by_bill[bid] = []
                        continue
                    fetched_count += 1
                    cache_key = _bill_buildings_cost_cache_key(bid)
                    repo.write(cache_key, payload)
                    rows = _extract_buildings_cost_rows(payload)
                    weights_by_bill[bid] = _weights_from_buildings_cost(rows)
                    status_counts["200"] = status_counts.get("200", 0) + 1

                _fill_pending()
        finally:
            if time_budget_hit and pending:
                for t in pending:
                    t.cancel()
            if pending:
                _ = await asyncio.gather(*pending, return_exceptions=True)

    if processed_fetch:
        repo.write(cursor_key, {"cursor": cursor + processed_fetch, "updated_at": utc_now_iso()})

    fallback_used_bills: set[str] = set()

    def weights_for_bill(bid: str) -> list[tuple[str, float]]:
        weights = weights_by_bill.get(bid)
        if weights:
            return weights
        fallback_buildings = fallback_buildings_by_bill.get(bid)
        if not fallback_buildings:
            return []
        fallback_used_bills.add(bid)
        return [(b, 1.0) for b in sorted(fallback_buildings)]

    receita_por_obra: dict[str, float] = dict(receita_por_obra_direto)
    custo_por_obra: dict[str, float] = dict(custo_por_obra_direto)

    def allocate(target: dict[str, float], amount: float, weights: list[tuple[str, float]]) -> None:
        if amount == 0:
            return
        if not weights:
            return
        total_w = sum(w for _, w in weights)
        if total_w <= 0:
            return
        for obra_id, w in weights:
            ratio = w / total_w
            target[obra_id] = target.get(obra_id, 0.0) + (amount * ratio)

    for bid, receita in receita_por_bill.items():
        allocate(receita_por_obra, receita, weights_for_bill(bid))

    for bid, custo in custo_por_bill.items():
        allocate(custo_por_obra, custo, weights_for_bill(bid))

    # Inclui também todas as obras (para mostrar receita 0 no final), respeitando filtros.
    obra_ids: set[str] = set(str(_normalize_building(o).get("id") or "").strip() for o in obras if isinstance(o, dict))
    obra_ids.discard("")
    obra_ids = {*(obra_ids), *(receita_por_obra.keys()), *(custo_por_obra.keys())}
    rows: list[dict[str, Any]] = []
    for obra_id in obra_ids:
        if building_aliases is not None and obra_id not in building_aliases:
            continue
        if selected_company_id != "all":
            cid = building_company_by_alias.get(str(obra_id))
            if cid != selected_company_id:
                continue
        receita = receita_por_obra.get(obra_id, 0.0)
        custos = custo_por_obra.get(obra_id, 0.0)
        mc = receita - custos
        pct = (mc / receita * 100.0) if receita > 0 else 0.0
        name = building_name_by_id.get(obra_id) or f"Obra {obra_id}"
        rows.append(
            {
                "building_id": obra_id,
                "building_name": name,
                "receita_operacional": receita,
                "mc": mc,
                "mc_percent": pct,
            }
        )

    rows.sort(key=lambda r: float(r.get("receita_operacional") or 0.0), reverse=True)

    # Total deve refletir também obras com custo e receita 0 (MC negativo),
    # senão o dashboard fica “Receita = MC” ou “Total = 0” quando só há despesas.
    total_receita = sum(float(r.get("receita_operacional") or 0.0) for r in rows)
    total_mc = sum(float(r.get("mc") or 0.0) for r in rows)
    total_pct = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

    rows = rows[: int(top)]

    diag: dict[str, Any] = {
        "bills": len(bill_ids),
        "cached": cached_count,
        "fetched": fetched_count,
        "missing": missing_count,
        "to_fetch": len(to_fetch),
        "processed_fetch": processed_fetch,
        "fallback_used": len(fallback_used_bills),
        "time_budget_seconds": int(time_budget_seconds),
        "time_budget_hit": bool(time_budget_hit),
        "max_concurrency": int(concurrency),
        "generated_at": utc_now_iso(),
        "sienge": {
            "configured": True,
            "last_error": getattr(sienge_client, "last_error", None),
        },
        "status_counts": status_counts,
    }
    if debug:
        empty_weights = [bid for bid, w in weights_by_bill.items() if not w]
        diag["bills_without_weights"] = len(empty_weights)
        diag["sample_bills_without_weights"] = empty_weights[:20]
        diag["sample_fallback_used"] = sorted(fallback_used_bills)[:20]
        diag["receita_bills"] = len(receita_por_bill)
        diag["custo_bills"] = len(custo_por_bill)

    return {
        "rows": rows,
        "total": {"receita_operacional": total_receita, "mc": total_mc, "mc_percent": total_pct},
        "filters": filtered.get("filters", {}),
        "diagnostic": diag,
    }
