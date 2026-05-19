from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.services.bitrix_client import BitrixClient, BitrixResponseError


def _coerce_int(value: Any | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _extract_result_payload(data: Any) -> Any:
    # Bitrix em geral: { result: <payload>, next: <int?> }
    if isinstance(data, dict) and "result" in data:
        return data.get("result")
    return data


def _extract_next(data: Any) -> int | None:
    if not isinstance(data, dict):
        return None
    # Variacoes observadas: next no root, ou dentro de result
    nxt = data.get("next")
    if nxt is not None:
        return _coerce_int(nxt)
    res = data.get("result")
    if isinstance(res, dict) and res.get("next") is not None:
        return _coerce_int(res.get("next"))
    return None


async def fetch_profile(client: BitrixClient) -> dict[str, Any]:
    data = await client.call("profile")
    payload = _extract_result_payload(data)
    return payload if isinstance(payload, dict) else {"raw": payload}


async def fetch_funnels(client: BitrixClient) -> dict[str, Any]:
    """Busca funis (pipelines) de DEAL e seus estagios.

    Usa os metodos classicos do Bitrix (crm.dealcategory.*) que costumam
    estar disponiveis em contas Bitrix24.
    """
    categories_raw = await client.call("crm.dealcategory.list")
    categories_payload = _extract_result_payload(categories_raw)

    # Pode ser lista direta ou dict com items.
    categories: list[dict[str, Any]]
    if isinstance(categories_payload, list):
        categories = [c for c in categories_payload if isinstance(c, dict)]
    elif isinstance(categories_payload, dict) and isinstance(categories_payload.get("categories"), list):
        categories = [c for c in categories_payload.get("categories") if isinstance(c, dict)]
    else:
        categories = []

    funnels: list[dict[str, Any]] = []
    for cat in categories:
        cat_id = _coerce_int(cat.get("ID") or cat.get("id"))
        name = cat.get("NAME") or cat.get("name") or str(cat_id or "")

        stages: list[dict[str, Any]] = []
        if cat_id is not None:
            try:
                stages_raw = await client.call("crm.dealcategory.stage.list", {"id": cat_id})
                stages_payload = _extract_result_payload(stages_raw)
                if isinstance(stages_payload, list):
                    stages = [s for s in stages_payload if isinstance(s, dict)]
                elif isinstance(stages_payload, dict) and isinstance(stages_payload.get("stages"), list):
                    stages = [s for s in stages_payload.get("stages") if isinstance(s, dict)]
            except BitrixResponseError:
                stages = []

        funnels.append({"id": cat_id, "name": name, "raw": cat, "stages": stages})

    return {
        "total": len(funnels),
        "funnels": funnels,
    }


async def fetch_crm_smart_process_pipelines(
    client: BitrixClient,
    *,
    entity_type_id: int,
) -> dict[str, Any]:
    """Lista pipelines (categorias) e stages de um Smart Process (CRM).

    Exemplo de URL no Bitrix: /crm/type/<entityTypeId>/kanban/category/<categoryId>/
    """
    entity_type_id = int(entity_type_id)

    categories_raw = await client.call("crm.category.list", {"entityTypeId": entity_type_id})
    categories_payload = _extract_result_payload(categories_raw)

    categories: list[dict[str, Any]]
    if isinstance(categories_payload, list):
        categories = [c for c in categories_payload if isinstance(c, dict)]
    elif isinstance(categories_payload, dict) and isinstance(categories_payload.get("categories"), list):
        categories = [c for c in categories_payload.get("categories") if isinstance(c, dict)]
    else:
        categories = []

    pipelines: list[dict[str, Any]] = []
    for cat in categories:
        cat_id = _coerce_int(cat.get("id") or cat.get("ID"))
        name = cat.get("name") or cat.get("NAME") or str(cat_id or "")

        stages: list[dict[str, Any]] = []
        if cat_id is not None:
            try:
                stages_raw = await client.call(
                    "crm.stage.list",
                    {"entityTypeId": entity_type_id, "categoryId": cat_id},
                )
                stages_payload = _extract_result_payload(stages_raw)
                if isinstance(stages_payload, list):
                    stages = [s for s in stages_payload if isinstance(s, dict)]
                elif isinstance(stages_payload, dict) and isinstance(stages_payload.get("stages"), list):
                    stages = [s for s in stages_payload.get("stages") if isinstance(s, dict)]
            except BitrixResponseError:
                stages = []

        pipelines.append({"id": cat_id, "name": name, "raw": cat, "stages": stages})

    return {"total": len(pipelines), "pipelines": pipelines, "entityTypeId": entity_type_id}


async def fetch_crm_items(
    client: BitrixClient,
    *,
    entity_type_id: int,
    category_id: int | None = None,
    max_items: int = 2000,
) -> dict[str, Any]:
    """Lista itens do CRM (Smart Process) via crm.item.list com paginação."""
    entity_type_id = int(entity_type_id)
    max_items = int(max_items or 0)
    if max_items <= 0:
        max_items = 2000
    max_items = min(max_items, 5000)

    start = 0
    items: list[dict[str, Any]] = []

    flt: dict[str, Any] = {}
    if category_id is not None:
        flt["categoryId"] = int(category_id)

    # Campos típicos do CRM; contas podem variar.
    select = [
        "id",
        "title",
        "stageId",
        "categoryId",
        "assignedById",
        "createdTime",
        "updatedTime",
        "begindate",
        "closedate",
    ]

    while len(items) < max_items:
        params: dict[str, Any] = {
            "entityTypeId": entity_type_id,
            "filter": flt,
            "order": {"id": "ASC"},
            "select": select,
            "start": start,
        }
        raw = await client.call("crm.item.list", params)
        payload = _extract_result_payload(raw)

        chunk: list[dict[str, Any]] = []
        if isinstance(payload, list):
            chunk = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
            chunk = [x for x in payload.get("items") if isinstance(x, dict)]

        items.extend(chunk)
        if len(items) >= max_items:
            items = items[:max_items]
            break

        nxt = _extract_next(raw)
        if nxt is None:
            break
        start = nxt

    return {
        "entityTypeId": entity_type_id,
        "categoryId": int(category_id) if category_id is not None else None,
        "total": len(items),
        "items": items,
    }


async def export_crm_kanban(
    client: BitrixClient,
    *,
    entity_type_id: int,
    category_id: int | None = None,
    max_items: int = 2000,
) -> dict[str, Any]:
    pipelines = await fetch_crm_smart_process_pipelines(client, entity_type_id=entity_type_id)
    items = await fetch_crm_items(client, entity_type_id=entity_type_id, category_id=category_id, max_items=max_items)
    return {
        "exportedAt": datetime.utcnow().isoformat(),
        "entityTypeId": int(entity_type_id),
        "categoryId": int(category_id) if category_id is not None else None,
        "pipelines": pipelines.get("pipelines", []),
        "items": items.get("items", []),
        "meta": {
            "pipelinesTotal": pipelines.get("total"),
            "itemsTotal": items.get("total"),
        },
    }


async def fetch_tasks(
    client: BitrixClient,
    *,
    group_id: int | None = None,
    max_items: int = 500,
) -> dict[str, Any]:
    """Lista tarefas do Bitrix (tasks.task.list) com paginacao.

    Observacao: pode retornar muita coisa; por isso tem cap de max_items.
    """
    max_items = int(max_items or 0)
    if max_items <= 0:
        max_items = 500
    max_items = min(max_items, 5000)

    start = 0
    items: list[dict[str, Any]] = []

    base_filter: dict[str, Any] = {}
    if group_id is not None:
        base_filter["GROUP_ID"] = int(group_id)

    # Campo 'select' varia por conta; aqui usamos um conjunto conservador.
    select = [
        "ID",
        "TITLE",
        "STATUS",
        "RESPONSIBLE_ID",
        "CREATED_DATE",
        "CLOSED_DATE",
        "DEADLINE",
        "GROUP_ID",
    ]

    while True:
        params: dict[str, Any] = {
            "select": select,
            "start": start,
        }
        if base_filter:
            params["filter"] = base_filter

        raw = await client.call("tasks.task.list", params)
        payload = _extract_result_payload(raw)

        page_items: list[Any] = []
        if isinstance(payload, dict):
            # Variacoes: { tasks: [...] }
            if isinstance(payload.get("tasks"), list):
                page_items = payload.get("tasks")
            elif isinstance(payload.get("items"), list):
                page_items = payload.get("items")
        elif isinstance(payload, list):
            page_items = payload

        for t in page_items:
            if isinstance(t, dict):
                items.append(t)
                if len(items) >= max_items:
                    break

        if len(items) >= max_items:
            break

        nxt = _extract_next(raw)
        if nxt is None:
            # Sem next => acabou
            break
        start = nxt
        if start == 0:
            break

    return {
        "total": len(items),
        "items": items,
        "max_items": max_items,
    }


async def export_diario_obras(
    client: BitrixClient,
    *,
    group_id: int | None = None,
    max_tasks: int = 500,
) -> dict[str, Any]:
    funnels = await fetch_funnels(client)
    tasks = await fetch_tasks(client, group_id=group_id, max_items=max_tasks)

    return {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "funnels": funnels,
        "tasks": tasks,
        "filters": {
            "group_id": group_id,
            "max_tasks": int(max_tasks or 0),
        },
    }
