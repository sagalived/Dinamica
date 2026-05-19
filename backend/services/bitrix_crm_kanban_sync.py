from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Card, Sprint
from backend.services.bitrix_client import BitrixClient, BitrixResponseError
from backend.services.bitrix_service import _coerce_int, _extract_result_payload, fetch_crm_items


def _coerce_str(value: Any | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _stage_sort_key(stage: dict[str, Any]) -> int:
    raw = stage.get("sort") if isinstance(stage, dict) else None
    if raw is None:
        raw = stage.get("SORT") if isinstance(stage, dict) else None
    try:
        return int(raw)
    except Exception:
        return 0


def _stage_id(stage: dict[str, Any]) -> str | None:
    return _coerce_str(stage.get("id") or stage.get("ID"))


def _stage_name(stage: dict[str, Any]) -> str:
    return str(stage.get("name") or stage.get("NAME") or "").strip()


def _infer_status_from_stage_name(name: str) -> str:
    n = (name or "").strip().lower()

    # concluído/finalizado
    if any(k in n for k in ["concl", "final", "feito", "done", "complete", "aprov"]):
        return "done"

    # bloqueado/cancelado
    if any(k in n for k in ["bloq", "imped", "cancel", "recus", "declin"]):
        return "blocked"

    # em execução / andamento
    if any(k in n for k in ["andament", "exec", "fazendo", "progress", "em exec", "in progress"]):
        return "in_progress"

    return "planned"


async def _fetch_stages(
    client: BitrixClient,
    *,
    entity_type_id: int,
    category_id: int,
) -> list[dict[str, Any]]:
    raw = await client.call(
        "crm.stage.list",
        {"entityTypeId": int(entity_type_id), "categoryId": int(category_id)},
    )
    payload = _extract_result_payload(raw)

    if isinstance(payload, list):
        return [s for s in payload if isinstance(s, dict)]
    if isinstance(payload, dict) and isinstance(payload.get("stages"), list):
        return [s for s in payload.get("stages") if isinstance(s, dict)]
    return []


def _pick_stage_for_status(stages_sorted: list[dict[str, Any]], status: str) -> str | None:
    if not stages_sorted:
        return None
    first = _stage_id(stages_sorted[0])
    last = _stage_id(stages_sorted[-1])
    mid = _stage_id(stages_sorted[len(stages_sorted) // 2])

    st = (status or "").strip().lower()
    if st == "done":
        return last
    if st == "in_progress":
        return mid or first
    if st == "blocked":
        return mid or first
    return first


async def pull_sprint_from_bitrix_crm(
    db: Session,
    client: BitrixClient,
    *,
    sprint_id: int,
    entity_type_id: int,
    category_id: int,
    max_items: int = 2000,
) -> dict[str, Any]:
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise ValueError("Sprint not found")

    entity_type_id = int(entity_type_id)
    category_id = int(category_id)

    # cache de stages para inferir status
    stages = await _fetch_stages(client, entity_type_id=entity_type_id, category_id=category_id)
    stage_name_by_id: dict[str, str] = {}
    for s in stages:
        sid = _stage_id(s)
        if sid:
            stage_name_by_id[sid] = _stage_name(s)

    payload = await fetch_crm_items(
        client,
        entity_type_id=entity_type_id,
        category_id=category_id,
        max_items=max_items,
    )
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        items = []

    existing: dict[int, Card] = {}
    for card in db.scalars(select(Card).where(Card.sprint_id == sprint.id)).all():
        cid = getattr(card, "bitrix_crm_item_id", None)
        if isinstance(cid, int):
            existing[cid] = card

    remote_ids: set[int] = set()
    upserted = 0

    order = 0
    for it in items:
        if not isinstance(it, dict):
            continue

        item_id = _coerce_int(it.get("id") or it.get("ID"))
        if item_id is None:
            continue
        remote_ids.add(item_id)

        order += 1
        title = str(it.get("title") or it.get("TITLE") or f"Item {item_id}").strip() or f"Item {item_id}"
        stage_id = _coerce_str(it.get("stageId") or it.get("STAGE_ID"))
        stage_name = stage_name_by_id.get(stage_id or "", "")
        status = _infer_status_from_stage_name(stage_name)

        card = existing.get(item_id)
        if card is None:
            card = Card(
                sprint_id=sprint.id,
                building_id=sprint.building_id,
                title=title,
                description=None,
                status=status,
                priority="medium",
                responsible=None,
                due_date=None,
                tags=None,
                created_by="bitrix-crm",
                order=order,
                bitrix_crm_entity_type_id=entity_type_id,
                bitrix_crm_category_id=category_id,
                bitrix_crm_item_id=item_id,
                bitrix_crm_stage_id=stage_id,
                bitrix_crm_last_pull_at=datetime.utcnow(),
            )
            db.add(card)
        else:
            card.title = title
            card.status = status
            card.order = order
            card.bitrix_crm_entity_type_id = entity_type_id
            card.bitrix_crm_category_id = category_id
            card.bitrix_crm_stage_id = stage_id
            card.bitrix_crm_last_pull_at = datetime.utcnow()

        upserted += 1

    removed = 0
    if remote_ids:
        for card in db.scalars(
            select(Card)
            .where(Card.sprint_id == sprint.id)
            .where(Card.bitrix_crm_item_id.is_not(None))
        ).all():
            cid = getattr(card, "bitrix_crm_item_id", None)
            if isinstance(cid, int) and cid not in remote_ids:
                db.delete(card)
                removed += 1

    sprint.bitrix_crm_entity_type_id = entity_type_id
    sprint.bitrix_crm_category_id = category_id
    sprint.bitrix_crm_last_pull_at = datetime.utcnow()
    db.commit()

    return {
        "sprint_id": sprint.id,
        "entity_type_id": entity_type_id,
        "category_id": category_id,
        "items_total": len(remote_ids),
        "cards_upserted": upserted,
        "cards_removed": removed,
    }


async def push_sprint_to_bitrix_crm(
    db: Session,
    client: BitrixClient,
    *,
    sprint_id: int,
    entity_type_id: int,
    category_id: int,
) -> dict[str, Any]:
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise ValueError("Sprint not found")

    entity_type_id = int(entity_type_id)
    category_id = int(category_id)

    stages = await _fetch_stages(client, entity_type_id=entity_type_id, category_id=category_id)
    stages_sorted = sorted(stages, key=_stage_sort_key)

    cards = (
        db.scalars(
            select(Card)
            .where(Card.sprint_id == sprint.id)
            .order_by(Card.order.asc(), Card.id.asc())
        )
        .all()
    )

    created = 0
    updated = 0

    for card in cards:
        stage_id = _pick_stage_for_status(stages_sorted, card.status)
        fields: dict[str, Any] = {
            "title": card.title,
            "categoryId": category_id,
        }
        if stage_id:
            fields["stageId"] = stage_id

        item_id = getattr(card, "bitrix_crm_item_id", None)
        if not isinstance(item_id, int):
            raw = await client.call("crm.item.add", {"entityTypeId": entity_type_id, "fields": fields})
            payload = _extract_result_payload(raw)
            new_id: int | None = None
            if isinstance(payload, dict):
                if isinstance(payload.get("item"), dict):
                    new_id = _coerce_int(payload.get("item", {}).get("id") or payload.get("item", {}).get("ID"))
                else:
                    new_id = _coerce_int(payload.get("id") or payload.get("ID"))
            else:
                new_id = _coerce_int(payload)
            if new_id is None:
                raise BitrixResponseError(status_code=None, message="Bitrix nao retornou id em crm.item.add", payload=raw)

            card.bitrix_crm_entity_type_id = entity_type_id
            card.bitrix_crm_category_id = category_id
            card.bitrix_crm_item_id = new_id
            card.bitrix_crm_stage_id = stage_id
            card.updated_at = datetime.utcnow()
            created += 1
        else:
            await client.call(
                "crm.item.update",
                {"entityTypeId": entity_type_id, "id": int(item_id), "fields": fields},
            )
            card.bitrix_crm_entity_type_id = entity_type_id
            card.bitrix_crm_category_id = category_id
            card.bitrix_crm_stage_id = stage_id
            card.updated_at = datetime.utcnow()
            updated += 1

    sprint.bitrix_crm_entity_type_id = entity_type_id
    sprint.bitrix_crm_category_id = category_id
    sprint.updated_at = datetime.utcnow()
    db.commit()

    return {
        "sprint_id": sprint.id,
        "entity_type_id": entity_type_id,
        "category_id": category_id,
        "cards_total": len(cards),
        "items_created": created,
        "items_updated": updated,
    }
