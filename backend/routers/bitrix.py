from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from backend.config import BITRIX24_WEBHOOK_BASE_URL
from backend.dependencies import get_current_user, get_db
from backend.models import AppUser
from backend.services.bitrix_client import BitrixClient, BitrixResponseError
from backend.services.bitrix_service import (
    export_crm_kanban,
    export_diario_obras,
    fetch_crm_items,
    fetch_crm_smart_process_pipelines,
    fetch_funnels,
    fetch_profile,
)
from backend.services.bitrix_kanban_sync import pull_sprint_from_bitrix, push_sprint_to_bitrix
from backend.services.bitrix_crm_kanban_sync import pull_sprint_from_bitrix_crm, push_sprint_to_bitrix_crm

router = APIRouter(prefix="/api/bitrix", tags=["bitrix"])


def _client() -> BitrixClient:
    return BitrixClient(base_url=BITRIX24_WEBHOOK_BASE_URL, timeout_s=35.0)


@router.get("/health")
async def health(current_user: AppUser = Depends(get_current_user)) -> dict:
    client = _client()
    return {
        "configured": client.is_configured,
    }


@router.get("/profile")
async def profile(current_user: AppUser = Depends(get_current_user)) -> dict:
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        payload = await fetch_profile(client)
        return {"profile": payload}
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/funnels")
async def funnels(current_user: AppUser = Depends(get_current_user)) -> dict:
    """Lista pipelines (funis) e estágios do CRM de Deals no Bitrix24."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await fetch_funnels(client)
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/crm/type/{entity_type_id}/pipelines")
async def crm_pipelines(
    entity_type_id: int,
    current_user: AppUser = Depends(get_current_user),
) -> dict:
    """Lista pipelines (categorias) e stages de um Smart Process (CRM)."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await fetch_crm_smart_process_pipelines(client, entity_type_id=entity_type_id)
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/crm/type/{entity_type_id}/items")
async def crm_items(
    entity_type_id: int,
    category_id: int | None = Query(default=None),
    max_items: int = Query(default=2000, ge=1, le=5000),
    current_user: AppUser = Depends(get_current_user),
) -> dict:
    """Lista itens do kanban do Smart Process (CRM) via API."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await fetch_crm_items(
            client,
            entity_type_id=entity_type_id,
            category_id=category_id,
            max_items=max_items,
        )
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/crm/type/{entity_type_id}/export/kanban")
async def export_crm_type_kanban(
    entity_type_id: int,
    category_id: int | None = Query(default=None),
    max_items: int = Query(default=2000, ge=1, le=5000),
    current_user: AppUser = Depends(get_current_user),
) -> Response:
    """Exporta pipelines + itens do kanban de um Smart Process (CRM) em JSON."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        data = await export_crm_kanban(
            client,
            entity_type_id=entity_type_id,
            category_id=category_id,
            max_items=max_items,
        )
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"bitrix_crm_type_{int(entity_type_id)}_kanban_{stamp}.json"
    raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    return Response(
        content=raw,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/export/diario")
async def export_diario(
    group_id: int | None = Query(default=None),
    max_tasks: int = Query(default=500, ge=1, le=5000),
    current_user: AppUser = Depends(get_current_user),
) -> Response:
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        data = await export_diario_obras(client, group_id=group_id, max_tasks=max_tasks)
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"bitrix_diario_obras_{stamp}.json"
    raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    return Response(
        content=raw,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.post("/kanban/sprint/{sprint_id}/push")
async def push_kanban_sprint(
    sprint_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await push_sprint_to_bitrix(db, client, sprint_id=sprint_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/kanban/sprint/{sprint_id}/pull")
async def pull_kanban_sprint(
    sprint_id: int,
    max_tasks: int = Query(default=5000, ge=1, le=5000),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await pull_sprint_from_bitrix(db, client, sprint_id=sprint_id, max_items=max_tasks)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/crm/kanban/sprint/{sprint_id}/pull")
async def pull_kanban_sprint_from_crm(
    sprint_id: int,
    entity_type_id: int = Query(default=1066, ge=1),
    category_id: int = Query(default=0, ge=0),
    max_items: int = Query(default=2000, ge=1, le=5000),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Importa itens do Smart Process (CRM) para dentro do sprint (Bitrix manda)."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await pull_sprint_from_bitrix_crm(
            db,
            client,
            sprint_id=sprint_id,
            entity_type_id=entity_type_id,
            category_id=category_id,
            max_items=max_items,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/crm/kanban/sprint/{sprint_id}/push")
async def push_kanban_sprint_to_crm(
    sprint_id: int,
    entity_type_id: int = Query(default=1066, ge=1),
    category_id: int = Query(default=0, ge=0),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Cria/atualiza itens do Smart Process (CRM) a partir do sprint (subir projeto)."""
    client = _client()
    if not client.is_configured:
        raise HTTPException(status_code=503, detail="BITRIX24_WEBHOOK_BASE_URL nao configurado")

    try:
        return await push_sprint_to_bitrix_crm(
            db,
            client,
            sprint_id=sprint_id,
            entity_type_id=entity_type_id,
            category_id=category_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except BitrixResponseError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
