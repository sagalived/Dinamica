from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from backend.models import OperationalMonthlyAggregate, SiengeSnapshot
from backend.services.sienge_storage import read_snapshot


@dataclass(frozen=True)
class OperationalRow:
    month: str  # YYYY-MM
    company_id: str | None
    building_id: str | None
    receita: float
    custo: float


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    raw = str(value).strip()
    if not raw:
        return None

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.date()
    except Exception:
        pass

    try:
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return dt.date()
    except Exception:
        return None


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _should_ignore(item: dict[str, Any]) -> bool:
    statement_type = str(item.get("statementType") or item.get("operationType") or "").strip().lower()
    origin = str(item.get("statementOrigin") or item.get("origin") or "").strip().lower()

    if "transf" in statement_type or "transfer" in statement_type:
        return True
    if "saque" in statement_type:
        return True
    if origin == "bc":
        return True
    return False


def _is_expense(item: dict[str, Any], *, default_expense: bool) -> bool:
    """Determina se um item conta como custo.

    Nem todo dataset vem com `type` ou `rawValue` assinado.
    - `financeiro.json` (títulos a pagar) deve ser despesa por padrão.
    - `receber.json` (títulos a receber) deve ser receita por padrão.
    """

    typ = str(item.get("type") or "").strip().lower()
    if typ == "expense":
        return True
    if typ == "income":
        return False

    raw_value = item.get("rawValue")
    if raw_value is not None:
        try:
            return float(raw_value) < 0
        except (TypeError, ValueError):
            pass

    return default_expense


def _has_any_cost_item(items: list[dict[str, Any]]) -> bool:
    for it in items:
        if _should_ignore(it):
            continue
        if _amount_abs(it) > 0:
            return True
    return False


def _amount_abs(item: dict[str, Any]) -> float:
    amount = _safe_float(
        item.get("rawValue")
        or item.get("amount")
        or item.get("valor")
        or item.get("value")
        or item.get("totalInvoiceAmount")
        or item.get("totalAmount")
        or 0
    )
    return abs(amount)


def _item_due_date(item: dict[str, Any]) -> date | None:
    return _parse_date(
        item.get("dataVencimento")
        or item.get("dueDate")
        or item.get("data")
        or item.get("date")
        or item.get("operationDate")
        or item.get("paymentDate")
        or item.get("issueDate")
        or item.get("dataEmissao")
    )


def _building_id(item: dict[str, Any]) -> str | None:
    candidates = [
        item.get("buildingId"),
        item.get("building_id"),
        item.get("buildingCode"),
        item.get("building_code"),
        item.get("enterpriseId"),
        item.get("enterprise_id"),
        item.get("idObra"),
        item.get("codigoObra"),
        item.get("codigoVisivelObra"),
    ]
    for c in candidates:
        s = str(c or "").strip()
        if s and s not in {"None", "undefined", "null"}:
            return s
    return None


def _company_id(item: dict[str, Any], building_company_map: dict[str, str]) -> str | None:
    direct = item.get("companyId")
    if direct is None:
        direct = item.get("company_id")
    if direct is not None:
        s = str(direct).strip()
        if s and s not in {"None", "undefined", "null"}:
            return s

    bid = _building_id(item)
    if bid and bid in building_company_map:
        return building_company_map[bid]

    return None


def _build_building_company_map(db: Session) -> dict[str, str]:
    obras = read_snapshot(db, "obras.json", default=[]) or []
    out: dict[str, str] = {}
    if not isinstance(obras, list):
        return out
    for obra in obras:
        if not isinstance(obra, dict):
            continue
        cid = str(obra.get("companyId") or obra.get("idCompany") or obra.get("empresaId") or "").strip()
        if not cid or cid in {"None", "undefined", "null"}:
            continue
        id_candidates = {
            str(obra.get("id") or "").strip(),
            str(obra.get("code") or "").strip(),
            str(obra.get("codigoVisivel") or "").strip(),
            str(obra.get("codigo") or "").strip(),
        }
        for bid in id_candidates:
            if bid and bid not in {"None", "undefined", "null"}:
                out[bid] = cid
    return out


def _read_dataset(db: Session, key: str) -> list[dict[str, Any]]:
    payload = read_snapshot(db, key, default=[]) or []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def ensure_operational_aggregates(db: Session, *, today: date | None = None) -> None:
    """Garante que existe base de agregados no banco.

    - Se não houver nada, faz backfill completo (desde o 1º mês com dados no cache).
    - Se já houver, recalcula somente o mês atual (incremental).
    """
    exists = db.scalar(select(OperationalMonthlyAggregate.id).limit(1))
    if exists is None:
        rebuild_operational_aggregates(db)
        return

    # Heurística: se já existem agregados mas NENHUM custo_variavel foi calculado,
    # e há itens no financeiro.json, então a base foi construída com regra antiga
    # (custos sendo tratados como receita). Rebuild completo para corrigir.
    has_any_cost = db.scalar(
        select(OperationalMonthlyAggregate.id)
        .where(OperationalMonthlyAggregate.custo_variavel > 0)
        .limit(1)
    )
    if has_any_cost is None:
        financeiro = _read_dataset(db, "financeiro.json")
        if _has_any_cost_item(financeiro):
            rebuild_operational_aggregates(db)
            return

    if today is None:
        today = date.today()
    recompute_month(db, month=_month_key(date(today.year, today.month, 1)))


def rebuild_operational_aggregates(db: Session) -> None:
    building_company_map = _build_building_company_map(db)
    receber = _read_dataset(db, "receber.json")
    financeiro = _read_dataset(db, "financeiro.json")

    agg: dict[tuple[str, str | None, str | None], OperationalRow] = {}

    def consume(item: dict[str, Any], *, default_expense: bool) -> None:
        if _should_ignore(item):
            return
        d = _item_due_date(item)
        if d is None:
            return
        amount = _amount_abs(item)
        if amount <= 0:
            return
        month = _month_key(d)
        bid = _building_id(item)
        cid = _company_id(item, building_company_map)
        key = (month, cid, bid)
        row = agg.get(key)
        if row is None:
            row = OperationalRow(month=month, company_id=cid, building_id=bid, receita=0.0, custo=0.0)
        if _is_expense(item, default_expense=default_expense):
            row = OperationalRow(
                month=row.month,
                company_id=row.company_id,
                building_id=row.building_id,
                receita=row.receita,
                custo=row.custo + amount,
            )
        else:
            row = OperationalRow(
                month=row.month,
                company_id=row.company_id,
                building_id=row.building_id,
                receita=row.receita + amount,
                custo=row.custo,
            )
        agg[key] = row

    for it in receber:
        consume(it, default_expense=False)
    for it in financeiro:
        consume(it, default_expense=True)

    db.execute(delete(OperationalMonthlyAggregate))

    rows: list[OperationalMonthlyAggregate] = []
    for r in agg.values():
        mc = r.receita - r.custo
        mc_percent = (mc / r.receita * 100.0) if r.receita > 0 else 0.0
        rows.append(
            OperationalMonthlyAggregate(
                month=r.month,
                company_id=r.company_id,
                building_id=r.building_id,
                receita_operacional=r.receita,
                custo_variavel=r.custo,
                mc=mc,
                mc_percent=mc_percent,
            )
        )

    if rows:
        db.add_all(rows)
    db.commit()


def recompute_month(db: Session, *, month: str) -> None:
    """Recalcula um mês (YYYY-MM) inteiro a partir do cache bruto."""
    building_company_map = _build_building_company_map(db)
    receber = _read_dataset(db, "receber.json")
    financeiro = _read_dataset(db, "financeiro.json")

    def is_target_month(item: dict[str, Any]) -> bool:
        d = _item_due_date(item)
        if d is None:
            return False
        return _month_key(d) == month

    agg: dict[tuple[str | None, str | None], OperationalRow] = {}

    def consume(item: dict[str, Any], *, default_expense: bool) -> None:
        if _should_ignore(item):
            return
        if not is_target_month(item):
            return
        amount = _amount_abs(item)
        if amount <= 0:
            return
        bid = _building_id(item)
        cid = _company_id(item, building_company_map)
        key = (cid, bid)
        row = agg.get(key)
        if row is None:
            row = OperationalRow(month=month, company_id=cid, building_id=bid, receita=0.0, custo=0.0)
        if _is_expense(item, default_expense=default_expense):
            row = OperationalRow(month=month, company_id=cid, building_id=bid, receita=row.receita, custo=row.custo + amount)
        else:
            row = OperationalRow(month=month, company_id=cid, building_id=bid, receita=row.receita + amount, custo=row.custo)
        agg[key] = row

    for it in receber:
        consume(it, default_expense=False)
    for it in financeiro:
        consume(it, default_expense=True)

    db.execute(delete(OperationalMonthlyAggregate).where(OperationalMonthlyAggregate.month == month))

    rows: list[OperationalMonthlyAggregate] = []
    for r in agg.values():
        mc = r.receita - r.custo
        mc_percent = (mc / r.receita * 100.0) if r.receita > 0 else 0.0
        rows.append(
            OperationalMonthlyAggregate(
                month=month,
                company_id=r.company_id,
                building_id=r.building_id,
                receita_operacional=r.receita,
                custo_variavel=r.custo,
                mc=mc,
                mc_percent=mc_percent,
            )
        )

    if rows:
        db.add_all(rows)
    db.commit()
