from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user, require_database_ready
from backend.models import AppUser, OperationalMonthlyAggregate
from backend.services.operational_aggregates import ensure_operational_aggregates
from backend.services.sienge_storage import read_snapshot

router = APIRouter(prefix="/api/operational", tags=["operational"])


def _validate_iso_date(value: str, label: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail=f"{label} deve estar no formato yyyy-MM-dd (ex: 2017-08-13)")


def _month_key_from_iso(value: str) -> str:
    _validate_iso_date(value, "date")
    return value[:7]


def _building_name_map(db: Session) -> dict[str, str]:
    obras = read_snapshot(db, "obras.json", default=[]) or []
    out: dict[str, str] = {}
    if not isinstance(obras, list):
        return out
    for obra in obras:
        if not isinstance(obra, dict):
            continue
        name = str(obra.get("name") or obra.get("nome") or obra.get("enterpriseName") or "").strip()
        code = str(obra.get("code") or obra.get("codigoVisivel") or obra.get("codigo") or obra.get("id") or "").strip()
        oid = str(obra.get("id") or "").strip()
        if name:
            if oid:
                out[oid] = name
            if code:
                out[code] = name
    return out


@router.post("/rebuild")
def rebuild_operational(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Rebuild completo dos agregados (pode ser pesado em bases grandes)."""
    from backend.services.operational_aggregates import rebuild_operational_aggregates

    rebuild_operational_aggregates(db)
    return {"ok": True}


@router.post("/ensure")
def ensure_operational(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ensure_operational_aggregates(db, today=date.today())
    return {"ok": True}


@router.get("/series")
def operational_series(
    start_date: str = Query(..., description="yyyy-MM-dd"),
    end_date: str = Query(..., description="yyyy-MM-dd"),
    company_id: str = Query("all"),
    building_id: str = Query("all"),
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _validate_iso_date(start_date, "start_date")
    _validate_iso_date(end_date, "end_date")
    if end_date < start_date:
        raise HTTPException(status_code=422, detail="end_date deve ser >= start_date")

    ensure_operational_aggregates(db, today=date.today())

    start_month = start_date[:7]
    end_month = end_date[:7]

    stmt = select(OperationalMonthlyAggregate).where(
        OperationalMonthlyAggregate.month >= start_month,
        OperationalMonthlyAggregate.month <= end_month,
    )

    if company_id != "all":
        stmt = stmt.where(OperationalMonthlyAggregate.company_id == str(company_id))
    if building_id != "all":
        stmt = stmt.where(OperationalMonthlyAggregate.building_id == str(building_id))

    rows = db.scalars(stmt).all()

    by_month: dict[str, dict[str, float]] = {}
    for r in rows:
        bucket = by_month.setdefault(
            r.month,
            {"receita_operacional": 0.0, "custo_variavel": 0.0, "mc": 0.0},
        )
        bucket["receita_operacional"] += float(r.receita_operacional or 0)
        bucket["custo_variavel"] += float(r.custo_variavel or 0)
        bucket["mc"] += float(r.mc or 0)

    out_rows: list[dict[str, Any]] = []
    for m in sorted(by_month.keys()):
        receita = by_month[m]["receita_operacional"]
        custo = by_month[m]["custo_variavel"]
        mc = receita - custo
        pct = (mc / receita * 100.0) if receita > 0 else 0.0
        out_rows.append(
            {
                "month": m,
                "receita_operacional": receita,
                "custo_variavel": custo,
                "mc": mc,
                "mc_percent": pct,
            }
        )

    total_receita = sum(r["receita_operacional"] for r in out_rows)
    total_custo = sum(r["custo_variavel"] for r in out_rows)
    total_mc = total_receita - total_custo
    total_pct = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

    return {
        "range": {"start": start_month, "end": end_month},
        "rows": out_rows,
        "total": {
            "receita_operacional": total_receita,
            "custo_variavel": total_custo,
            "mc": total_mc,
            "mc_percent": total_pct,
        },
    }


@router.get("/mc-by-building")
def operational_mc_by_building(
    company_id: str = Query("all"),
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """MC por obra (acumulado desde o início disponível no cache), ordenado por Receita desc."""
    ensure_operational_aggregates(db, today=date.today())

    stmt = select(
        OperationalMonthlyAggregate.building_id.label("building_id"),
        func.sum(OperationalMonthlyAggregate.receita_operacional).label("receita"),
        func.sum(OperationalMonthlyAggregate.custo_variavel).label("custo"),
    ).group_by(OperationalMonthlyAggregate.building_id)

    if company_id != "all":
        stmt = stmt.where(OperationalMonthlyAggregate.company_id == str(company_id))

    rows = db.execute(stmt).all()

    name_map = _building_name_map(db)

    out_rows: list[dict[str, Any]] = []
    for building_id, receita, custo in rows:
        bid = str(building_id or "0")
        receita_f = float(receita or 0)
        custo_f = float(custo or 0)
        mc = receita_f - custo_f
        pct = (mc / receita_f * 100.0) if receita_f > 0 else 0.0
        out_rows.append(
            {
                "id": bid,
                "name": name_map.get(bid) or ("Sem obra" if bid == "0" else f"Obra {bid}"),
                "receita": receita_f,
                "mc": mc,
                "pct": pct,
            }
        )

    out_rows.sort(key=lambda r: (-(r.get("receita") or 0), str(r.get("name") or "")))

    total_receita = sum(r["receita"] for r in out_rows)
    total_mc = sum(r["mc"] for r in out_rows)

    return {
        "rows": out_rows,
        "total": {
            "receita": total_receita,
            "mc": total_mc,
            "pct": (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0,
        },
    }
