from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user, require_database_ready
from backend.models import AppUser, Building, Client, Company, Creditor, DirectoryUser

router = APIRouter(prefix="/api", tags=["catalog"])


@router.get("/directory/users")
def list_directory_users(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(DirectoryUser).order_by(DirectoryUser.name)).all()
    return [
        {"id": row.id, "name": row.name, "email": row.email, "active": row.active}
        for row in rows
    ]


@router.get("/companies")
def list_companies(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Company).order_by(Company.name)).all()
    return [
        {"id": row.id, "name": row.name, "trade_name": row.trade_name, "cnpj": row.cnpj}
        for row in rows
    ]


@router.get("/buildings")
def list_buildings(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    active: str = Query(
        "all",
        description="Filtro de obras: all | true | false",
        pattern="^(all|true|false)$",
    ),
    company_id: int | None = Query(None, description="Filtrar por company_id"),
    db: Session = Depends(get_db),
) -> list[dict]:
    stmt = select(Building)
    if company_id is not None:
        stmt = stmt.where(Building.company_id == company_id)
    if active == "true":
        stmt = stmt.where(Building.active.is_(True))
    elif active == "false":
        stmt = stmt.where(Building.active.is_(False))
    rows = db.scalars(stmt.order_by(Building.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "company_id": row.company_id,
            "company_name": row.company_name,
            "cnpj": row.cnpj,
            "address": row.address,
            "building_type": row.building_type,
            "active": row.active,
        }
        for row in rows
    ]


@router.get("/creditors")
def list_creditors(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Creditor).order_by(Creditor.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "trade_name": row.trade_name,
            "cnpj": row.cnpj,
            "city": row.city,
            "state": row.state,
            "active": row.active,
        }
        for row in rows
    ]


@router.get("/clients")
def list_clients(
    __: None = Depends(require_database_ready),
    _: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.scalars(select(Client).order_by(Client.name)).all()
    return [
        {
            "id": row.id,
            "name": row.name,
            "fantasy_name": row.fantasy_name,
            "cnpj_cpf": row.cnpj_cpf,
            "city": row.city,
            "state": row.state,
            "email": row.email,
            "phone": row.phone,
            "status": row.status,
        }
        for row in rows
    ]
