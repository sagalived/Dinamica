from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from backend.models import Building, Company, Creditor, DirectoryUser


def _infer_building_active(item: dict[str, Any]) -> bool:
    raw_active = item.get("active")
    if isinstance(raw_active, bool):
        return raw_active
    raw_inactive = item.get("inactive")
    if isinstance(raw_inactive, bool):
        return not raw_inactive

    status = (
        item.get("status")
        or item.get("situation")
        or item.get("situacao")
        or item.get("situacaoObra")
        or item.get("statusDescription")
        or item.get("statusDescricao")
        or ""
    )
    status_text = str(status).strip().lower()
    if not status_text:
        return True

    inactive_markers = ("inativ", "encerr", "cancel", "finaliz", "conclu")
    if any(marker in status_text for marker in inactive_markers):
        return False
    if "ativ" in status_text:
        return True
    return True


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def upsert_catalog_from_sienge(
    db: Session,
    *,
    obras: list[dict[str, Any]] | None = None,
    usuarios: list[dict[str, Any]] | None = None,
    empresas: list[dict[str, Any]] | None = None,
    credores: list[dict[str, Any]] | None = None,
) -> None:
    """Atualiza tabelas de catálogo usadas pela UI.

    Importante: faz upsert (merge). Não deleta registros ausentes.
    """

    if usuarios:
        for item in usuarios:
            user_id = str(item.get("id") or "").strip()
            if not user_id:
                continue
            db.merge(
                DirectoryUser(
                    id=user_id,
                    name=item.get("name") or "Sem nome",
                    email=item.get("email"),
                    active=bool(item.get("active", True)),
                )
            )

    if empresas:
        for item in empresas:
            company_id = _as_int(item.get("id"))
            if company_id is None:
                continue
            db.merge(
                Company(
                    id=company_id,
                    name=item.get("name") or "Sem nome",
                    trade_name=item.get("tradeName"),
                    cnpj=item.get("cnpj"),
                )
            )

    if obras:
        for item in obras:
            building_id = _as_int(item.get("id"))
            if building_id is None:
                continue
            db.merge(
                Building(
                    id=building_id,
                    name=item.get("name") or "Sem nome",
                    company_id=_as_int(item.get("companyId")),
                    company_name=item.get("companyName"),
                    cnpj=item.get("cnpj"),
                    address=item.get("adress"),
                    created_by=item.get("createdBy"),
                    modified_by=item.get("modifiedBy"),
                    building_type=item.get("buildingTypeDescription"),
                    active=_infer_building_active(item),
                )
            )

    if credores:
        for item in credores:
            creditor_id = _as_int(item.get("id"))
            if creditor_id is None:
                continue
            address = item.get("address") or {}
            if not isinstance(address, dict):
                address = {}
            db.merge(
                Creditor(
                    id=creditor_id,
                    name=item.get("name") or "Sem nome",
                    trade_name=item.get("tradeName"),
                    cnpj=item.get("cnpj"),
                    city=address.get("cityName"),
                    state=address.get("state"),
                    active=bool(item.get("active", True)),
                )
            )

    db.commit()
