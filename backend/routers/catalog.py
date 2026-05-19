from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user, require_database_ready
from backend.models import AppUser, Building, Client, Company, Creditor, DirectoryUser
from backend.schemas import BuildingCatalogResponse, BuildingCreateRequest

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


@router.post("/buildings")
def create_building(
    payload: BuildingCreateRequest,
    __: None = Depends(require_database_ready),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nome da obra é obrigatório")

    company_name = (payload.company_name or "").strip() or None
    cnpj = (payload.cnpj or "").strip() or None

    if payload.company_id is not None:
        company = db.scalar(select(Company).where(Company.id == int(payload.company_id)))
        if company:
            # usa o catálogo do Sienge (via sync) como base
            company_name = company_name or (company.trade_name or company.name)
            cnpj = cnpj or company.cnpj

    building = Building(
        name=name,
        company_id=int(payload.company_id) if payload.company_id is not None else None,
        company_name=company_name,
        cnpj=cnpj,
        address=(payload.address or None),
        building_type=(payload.building_type or None),
        active=bool(payload.active),
        created_by=current_user.email,
        modified_by=current_user.email,
    )

    db.add(building)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        # Se o sequence/PK estiver fora de sincronia (ambiente seedado/importado),
        # devolve erro explícito para não mascarar como 500.
        msg = str(getattr(exc, "orig", exc))
        if "buildings_pkey" in msg or "UniqueViolation" in msg:
            raise HTTPException(
                status_code=409,
                detail="Falha ao criar obra: conflito de chave primária (sequence desincronizado). Reinicie a API para re-sincronizar ou execute o ajuste de sequence no banco.",
            )
        raise
    db.refresh(building)

    return {"building": BuildingCatalogResponse.model_validate(building).model_dump()}


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
