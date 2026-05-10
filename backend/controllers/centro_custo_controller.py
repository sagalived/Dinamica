from sqlalchemy.orm import Session

from backend.dtos.centro_custo_dto import CentroCustoFiltrosDTO
from backend.services.sienge_financial_reports import (
    cleanup_old_financial_reports,
    get_financial_reports,
    sync_financial_reports,
)


def listar_centro_custo(db: Session, filtros: CentroCustoFiltrosDTO) -> list[dict]:
    return get_financial_reports(
        db,
        company_id=filtros.resolved_company_id,
        building_id=filtros.resolved_building_id,
    )


async def sincronizar_centro_custo(db: Session) -> dict:
    return await sync_financial_reports(db)


def limpar_centro_custo(db: Session, *, from_year: int) -> dict:
    return cleanup_old_financial_reports(db, keep_from_year=from_year)
