from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Attachment, Building, Card, Sprint
from backend.services.bitrix_client import BitrixClient, BitrixResponseError
from backend.services.bitrix_service import fetch_profile, fetch_tasks


def _coerce_int(value: Any | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_dt(value: Any | None) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    # Bitrix costuma usar ISO; às vezes sem timezone.
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _task_title(task: dict[str, Any]) -> str:
    return str(task.get("TITLE") or task.get("title") or "").strip()


def _task_description(task: dict[str, Any]) -> str | None:
    desc = task.get("DESCRIPTION") or task.get("description")
    if desc is None:
        return None
    s = str(desc).strip()
    return s or None


def _map_task_status_to_card(status: Any | None) -> str:
    # Status Bitrix (mais comuns):
    # 1=New, 2=Pending, 3=In progress, 4=Supposedly completed, 5=Completed, 6=Deferred, 7=Declined
    st = _coerce_int(status)
    if st in {5}:
        return "done"
    if st in {3, 4}:
        return "in_progress"
    if st in {6, 7}:
        return "blocked"
    return "planned"


async def push_sprint_to_bitrix(
    db: Session,
    client: BitrixClient,
    *,
    sprint_id: int,
) -> dict[str, Any]:
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise ValueError("Sprint not found")

    building = db.scalar(select(Building).where(Building.id == sprint.building_id))
    building_name = building.name if building else f"Obra {sprint.building_id}"

    profile = await fetch_profile(client)
    owner_id = _coerce_int(profile.get("ID") or profile.get("id"))
    if owner_id is None:
        raise BitrixResponseError(status_code=None, message="Nao foi possivel obter owner_id via profile")

    # 1) Garante grupo do Bitrix
    group_id = _coerce_int(getattr(sprint, "bitrix_group_id", None))
    if group_id is None:
        name = f"[{building_name}] {sprint.name}".strip()
        desc = f"Dinamica Sprint ID: {sprint.id}"
        raw = await client.call(
            "sonet_group.create",
            {
                "NAME": name,
                "DESCRIPTION": desc,
                "OWNER_ID": owner_id,
                "PROJECT": "Y",
                "VISIBLE": "Y",
                "OPENED": "Y",
            },
        )
        # Result pode ser id direto ou dict.
        res = raw.get("result") if isinstance(raw, dict) else None
        if isinstance(res, dict):
            group_id = _coerce_int(res.get("ID") or res.get("id"))
        else:
            group_id = _coerce_int(res)
        if group_id is None:
            raise BitrixResponseError(status_code=None, message="Bitrix nao retornou group_id em sonet_group.create", payload=raw)
        sprint.bitrix_group_id = group_id
        db.commit()

    # 2) Upsert das tarefas (cards)
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
        fields: dict[str, Any] = {
            "TITLE": card.title,
            "DESCRIPTION": card.description or "",
            "GROUP_ID": group_id,
        }
        deadline = card.due_date
        if deadline is not None:
            fields["DEADLINE"] = deadline.isoformat()

        # Se o responsável for número, tenta mandar como RESPONSIBLE_ID.
        rid = _coerce_int(card.responsible)
        if rid is not None:
            fields["RESPONSIBLE_ID"] = rid

        task_id = _coerce_int(getattr(card, "bitrix_task_id", None))
        if task_id is None:
            raw = await client.call("tasks.task.add", {"fields": fields})
            res = raw.get("result") if isinstance(raw, dict) else None
            if isinstance(res, dict):
                if isinstance(res.get("task"), dict):
                    task_id = _coerce_int(res.get("task", {}).get("id") or res.get("task", {}).get("ID"))
                else:
                    task_id = _coerce_int(res.get("id") or res.get("ID"))
            else:
                task_id = _coerce_int(res)
            if task_id is None:
                raise BitrixResponseError(status_code=None, message="Bitrix nao retornou task_id em tasks.task.add", payload=raw)
            card.bitrix_task_id = task_id
            created += 1
        else:
            await client.call("tasks.task.update", {"taskId": task_id, "fields": fields})
            updated += 1

    sprint.updated_at = datetime.utcnow()
    db.commit()

    return {
        "sprint_id": sprint.id,
        "bitrix_group_id": group_id,
        "cards_total": len(cards),
        "tasks_created": created,
        "tasks_updated": updated,
    }


async def pull_sprint_from_bitrix(
    db: Session,
    client: BitrixClient,
    *,
    sprint_id: int,
    max_items: int = 5000,
) -> dict[str, Any]:
    sprint = db.scalar(select(Sprint).where(Sprint.id == sprint_id))
    if not sprint:
        raise ValueError("Sprint not found")

    group_id = _coerce_int(getattr(sprint, "bitrix_group_id", None))
    if group_id is None:
        raise ValueError("Sprint ainda nao possui bitrix_group_id; faca push primeiro")

    tasks_payload = await fetch_tasks(client, group_id=group_id, max_items=max_items)
    tasks = tasks_payload.get("items") if isinstance(tasks_payload, dict) else None
    if not isinstance(tasks, list):
        tasks = []

    remote_ids: set[int] = set()
    upserted = 0

    # Map por bitrix_task_id
    existing_by_task: dict[int, Card] = {}
    for card in db.scalars(select(Card).where(Card.sprint_id == sprint.id)).all():
        tid = _coerce_int(getattr(card, "bitrix_task_id", None))
        if tid is not None:
            existing_by_task[tid] = card

    order = 0
    for t in tasks:
        if not isinstance(t, dict):
            continue
        tid = _coerce_int(t.get("ID") or t.get("id"))
        if tid is None:
            continue
        remote_ids.add(tid)

        order += 1
        title = _task_title(t)
        if not title:
            title = f"Tarefa {tid}"

        card = existing_by_task.get(tid)
        if card is None:
            card = Card(
                sprint_id=sprint.id,
                building_id=sprint.building_id,
                title=title,
                description=_task_description(t),
                status=_map_task_status_to_card(t.get("STATUS") or t.get("status")),
                priority="medium",
                responsible=str(t.get("RESPONSIBLE_ID") or t.get("responsibleId") or "") or None,
                due_date=_parse_dt(t.get("DEADLINE") or t.get("deadline")),
                tags=None,
                created_by="bitrix",
                order=order,
                bitrix_task_id=tid,
                bitrix_last_pull_at=datetime.utcnow(),
            )
            db.add(card)
        else:
            card.title = title
            card.description = _task_description(t)
            card.status = _map_task_status_to_card(t.get("STATUS") or t.get("status"))
            card.responsible = str(t.get("RESPONSIBLE_ID") or t.get("responsibleId") or "") or None
            card.due_date = _parse_dt(t.get("DEADLINE") or t.get("deadline"))
            card.order = order
            card.bitrix_last_pull_at = datetime.utcnow()
        upserted += 1

    # Remove cards que já estavam linkados ao Bitrix mas sumiram de lá.
    removed = 0
    if remote_ids:
        to_delete = (
            db.scalars(
                select(Card)
                .where(Card.sprint_id == sprint.id)
                .where(Card.bitrix_task_id.is_not(None))
            )
            .all()
        )
        for card in to_delete:
            tid = _coerce_int(getattr(card, "bitrix_task_id", None))
            if tid is not None and tid not in remote_ids:
                # Remove anexos e arquivos (mantém comportamento parecido com o router).
                attachments = db.scalars(select(Attachment).where(Attachment.card_id == card.id)).all()
                for att in attachments:
                    try:
                        if att.file_path and os.path.exists(att.file_path):
                            os.remove(att.file_path)
                    except Exception:
                        pass
                    db.delete(att)
                db.delete(card)
                removed += 1

    sprint.bitrix_last_pull_at = datetime.utcnow()
    db.commit()

    return {
        "sprint_id": sprint.id,
        "bitrix_group_id": group_id,
        "tasks_total": len(remote_ids),
        "cards_upserted": upserted,
        "cards_removed": removed,
    }
