from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user
from backend.models import AppUser, Building, Company, Creditor, DirectoryUser
from backend.schemas import BootstrapResponse, FetchItemsRequest, FetchQuotationsRequest
from backend.services.sienge_cache import read_json_cache, read_sync_metadata, utc_now_iso, write_json_cache, write_sync_metadata
from backend.services.sienge_client import sienge_client

router = APIRouter(prefix="/api/sienge", tags=["sienge"])


def _to_array(payload: Any) -> list[dict]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data["results"]
        if isinstance(payload.get("results"), list):
            return payload["results"]
    if isinstance(payload, list):
        return payload
    return []


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_cached_dataset(filename: str, default: Any) -> Any:
    return read_json_cache(filename, default=default)


def _write_cached_dataset(filename: str, payload: Any) -> None:
    write_json_cache(filename, payload)


def _cache_counts() -> dict[str, int]:
    return {
        "obras": len(_to_array(_read_cached_dataset("obras.json", []))),
        "usuarios": len(_to_array(_read_cached_dataset("usuarios.json", []))),
        "credores": len(_to_array(_read_cached_dataset("credores.json", []))),
        "empresas": len(_to_array(_read_cached_dataset("empresas.json", []))),
        "pedidos": len(_to_array(_read_cached_dataset("pedidos.json", []))),
        "financeiro": len(_to_array(_read_cached_dataset("financeiro.json", []))),
        "receber": len(_to_array(_read_cached_dataset("receber.json", []))),
    }


def _normalize_company(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "name": item.get("name") or item.get("nome") or item.get("companyName") or f"Empresa {item.get('id')}",
        "tradeName": item.get("tradeName") or item.get("nomeFantasia"),
        "companyName": item.get("companyName") or item.get("name") or item.get("nome") or f"Empresa {item.get('id')}",
        "cnpj": item.get("cnpj") or item.get("cpfCnpj") or "",
    }


def _normalize_building(item: dict[str, Any]) -> dict[str, Any]:
    company_id = item.get("companyId") or item.get("idCompany") or item.get("empresaId")
    code = item.get("code") or item.get("codigoVisivel") or item.get("codigo") or item.get("id")
    name = item.get("name") or item.get("nome") or item.get("enterpriseName") or f"Obra {code}"
    address = item.get("address") or item.get("endereco") or item.get("adress") or ""
    engineer = item.get("engineer") or item.get("responsavelTecnico") or item.get("responsavel") or ""
    return {
        "id": item.get("id") or code,
        "code": str(code or ""),
        "codigoVisivel": str(code or ""),
        "name": name,
        "nome": name,
        "address": address,
        "endereco": address,
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "companyId": company_id,
        "idCompany": company_id,
        "cnpj": item.get("cnpj"),
        "engineer": engineer or "Aguardando Avaliação",
    }


def _normalize_creditor(item: dict[str, Any]) -> dict[str, Any]:
    address = item.get("address") if isinstance(item.get("address"), dict) else {}
    name = item.get("name") or item.get("nome") or item.get("tradeName") or f"Credor {item.get('id')}"
    return {
        "id": item.get("id"),
        "name": name,
        "nome": name,
        "nomeFantasia": item.get("tradeName") or item.get("nomeFantasia"),
        "cnpj": item.get("cnpj") or item.get("cpfCnpj") or "",
        "city": item.get("city") or item.get("cidade") or address.get("cityName"),
        "state": item.get("state") or item.get("estado") or address.get("state"),
        "active": item.get("ativo") is not False if "ativo" in item else item.get("active", True),
    }


def _normalize_user(item: dict[str, Any]) -> dict[str, Any]:
    name = item.get("name") or item.get("nome") or "Usuário sem nome"
    return {
        "id": str(item.get("id") or item.get("userId") or item.get("username") or ""),
        "name": name,
        "nome": name,
        "email": item.get("email"),
        "active": item.get("active", True),
    }


def _extract_company_id_from_links(links: list[dict[str, Any]]) -> int | None:
    for link in links:
        if link.get("rel") == "company" and link.get("href"):
            tail = link["href"].rstrip("/").split("/")[-1]
            if str(tail).isdigit():
                return int(tail)
    return None


def _legacy_bootstrap_payload(db: Session) -> dict[str, Any]:
    obras = _to_array(_read_cached_dataset("obras.json", []))
    usuarios = _to_array(_read_cached_dataset("usuarios.json", []))
    credores = _to_array(_read_cached_dataset("credores.json", []))
    companies = _to_array(_read_cached_dataset("empresas.json", []))
    pedidos = _to_array(_read_cached_dataset("pedidos.json", []))
    financeiro = _to_array(_read_cached_dataset("financeiro.json", []))
    receber = _to_array(_read_cached_dataset("receber.json", []))
    itens_pedidos = _read_cached_dataset("itens_pedidos.json", {}) or {}

    if not obras:
        obras = [
            {
                "id": b.id,
                "name": b.name,
                "code": b.id,
                "address": b.address,
                "companyId": b.company_id,
                "cnpj": b.cnpj,
            }
            for b in db.scalars(select(Building)).all()
        ]
    if not companies:
        companies = [
            {
                "id": c.id,
                "name": c.name,
                "tradeName": c.trade_name,
                "companyName": c.name,
                "cnpj": c.cnpj,
            }
            for c in db.scalars(select(Company)).all()
        ]
    if not credores:
        credores = [
            {
                "id": c.id,
                "name": c.name,
                "tradeName": c.trade_name,
                "cnpj": c.cnpj,
                "city": c.city,
                "state": c.state,
                "active": c.active,
            }
            for c in db.scalars(select(Creditor)).all()
        ]
    if not usuarios:
        usuarios = [
            {
                "id": row.id,
                "name": row.name,
                "nome": row.name,
                "email": row.email,
                "active": row.active,
            }
            for row in db.scalars(select(DirectoryUser).order_by(DirectoryUser.name)).all()
        ]

    building_map: dict[str, dict[str, Any]] = {}
    for obra in obras:
        normalized = _normalize_building(obra)
        bid = str(normalized.get("code") or normalized.get("id") or "")
        if bid:
            building_map[bid] = normalized

    creditor_map: dict[str, str] = {}
    for credor in credores:
        normalized = _normalize_creditor(credor)
        cid = str(normalized.get("id") or "")
        if cid:
            creditor_map[cid] = normalized["name"]

    user_map: dict[str, str] = {}
    for user in usuarios:
        normalized = _normalize_user(user)
        uid = str(normalized["id"])
        if uid:
            user_map[uid] = normalized["name"]

    normalized_orders: list[dict[str, Any]] = []
    for pedido in pedidos:
        building_id = str(pedido.get("codigoVisivelObra") or pedido.get("idObra") or pedido.get("buildingId") or "")
        supplier_id = str(pedido.get("codigoFornecedor") or pedido.get("idCredor") or pedido.get("supplierId") or "")
        buyer_id = str(pedido.get("idComprador") or pedido.get("codigoComprador") or pedido.get("buyerId") or "")
        building_info = building_map.get(building_id, {})
        normalized_orders.append(
            {
                "id": pedido.get("id") or pedido.get("numero") or 0,
                "buildingId": int(building_id) if building_id.isdigit() else 0,
                "idObra": int(building_id) if building_id.isdigit() else 0,
                "codigoVisivelObra": building_id,
                "companyId": pedido.get("companyId") or building_info.get("companyId"),
                "buyerId": buyer_id,
                "idComprador": buyer_id,
                "codigoComprador": buyer_id,
                "supplierId": int(supplier_id) if supplier_id.isdigit() else 0,
                "codigoFornecedor": int(supplier_id) if supplier_id.isdigit() else 0,
                "date": pedido.get("data") or pedido.get("dataEmissao") or pedido.get("date") or "",
                "dataEmissao": pedido.get("data") or pedido.get("dataEmissao") or pedido.get("date") or "",
                "totalAmount": _safe_float(pedido.get("totalAmount") or pedido.get("valorTotal")),
                "valorTotal": _safe_float(pedido.get("totalAmount") or pedido.get("valorTotal")),
                "status": pedido.get("status") or pedido.get("situacao") or "N/A",
                "situacao": pedido.get("status") or pedido.get("situacao") or "N/A",
                "paymentCondition": pedido.get("condicaoPagamento") or pedido.get("paymentMethod") or "A Prazo",
                "condicaoPagamento": pedido.get("condicaoPagamento") or pedido.get("paymentMethod") or "A Prazo",
                "deliveryDate": pedido.get("dataEntrega") or pedido.get("prazoEntrega") or "",
                "dataEntrega": pedido.get("dataEntrega") or pedido.get("prazoEntrega") or "",
                "internalNotes": pedido.get("internalNotes") or pedido.get("observacao") or "",
                "observacao": pedido.get("internalNotes") or pedido.get("observacao") or "",
                "nomeObra": pedido.get("nomeObra") or building_info.get("name") or (f"Obra {building_id}" if building_id else "Obra sem nome"),
                "nomeFornecedor": pedido.get("nomeFornecedor") or creditor_map.get(supplier_id) or (f"Credor {supplier_id}" if supplier_id else "Credor sem nome"),
                "nomeComprador": pedido.get("nomeComprador") or pedido.get("buyerName") or user_map.get(buyer_id) or buyer_id,
                "solicitante": pedido.get("solicitante") or pedido.get("requesterId") or pedido.get("createdBy") or user_map.get(buyer_id) or buyer_id,
                "requesterId": pedido.get("requesterId") or pedido.get("solicitante") or pedido.get("createdBy") or user_map.get(buyer_id) or buyer_id,
                "createdBy": pedido.get("createdBy") or pedido.get("nomeComprador") or user_map.get(buyer_id) or buyer_id,
            }
        )

    normalized_financial: list[dict[str, Any]] = []
    for item in financeiro:
        creditor_id = str(item.get("creditorId") or item.get("idCredor") or item.get("codigoFornecedor") or item.get("debtorId") or "")
        building_id = str(item.get("idObra") or item.get("codigoObra") or item.get("enterpriseId") or item.get("buildingId") or "")
        building_info = building_map.get(building_id, {})
        company_id = item.get("companyId") or item.get("debtorId") or building_info.get("companyId")
        name = item.get("nomeCredor") or item.get("creditorName") or item.get("nomeFantasiaCredor") or item.get("fornecedor") or item.get("credor") or creditor_map.get(creditor_id) or "Credor sem nome"
        normalized_financial.append(
            {
                "id": item.get("id") or item.get("numero") or item.get("codigoTitulo") or item.get("documentNumber") or 0,
                "companyId": int(company_id) if str(company_id).isdigit() else company_id,
                "creditorId": creditor_id,
                "buildingId": int(building_id) if building_id.isdigit() else 0,
                "idObra": int(building_id) if building_id.isdigit() else 0,
                "dataVencimento": item.get("dataVencimento") or item.get("issueDate") or item.get("dueDate") or item.get("dataVencimentoProjetado") or item.get("dataEmissao") or item.get("dataContabil") or "",
                "descricao": item.get("descricao") or item.get("historico") or item.get("tipoDocumento") or item.get("notes") or item.get("observacao") or "Título a Pagar",
                "valor": _safe_float(item.get("totalInvoiceAmount") or item.get("valor") or item.get("amount") or item.get("valorTotal") or item.get("valorLiquido") or item.get("valorBruto")),
                "situacao": item.get("situacao") or item.get("status") or "Pendente",
                "creditorName": name,
                "nomeCredor": name,
                "nomeObra": item.get("nomeObra") or building_info.get("name") or (f"Obra {building_id}" if building_id else "Obra sem nome"),
                "links": item.get("links") or [],
            }
        )

    normalized_receivable: list[dict[str, Any]] = []
    for item in receber:
        building_id = str(item.get("idObra") or item.get("codigoObra") or item.get("enterpriseId") or item.get("buildingId") or "")
        building_info = building_map.get(building_id, {})
        links = item.get("links") or []
        raw_value = _safe_float(
            item.get("rawValue")
            if item.get("rawValue") is not None
            else item.get("value")
            or item.get("valor")
            or item.get("valorSaldo")
            or item.get("totalInvoiceAmount")
            or item.get("valorTotal")
            or item.get("amount")
        )
        company_id = item.get("companyId") or building_info.get("companyId") or _extract_company_id_from_links(links)
        normalized_receivable.append(
            {
                "id": item.get("id") or item.get("numero") or item.get("numeroTitulo") or item.get("codigoTitulo") or item.get("documentNumber") or 0,
                "companyId": int(company_id) if str(company_id).isdigit() else company_id,
                "buildingId": int(building_id) if building_id.isdigit() else 0,
                "idObra": int(building_id) if building_id.isdigit() else 0,
                "dataVencimento": item.get("data") or item.get("date") or item.get("dataVencimento") or item.get("dataEmissao") or item.get("issueDate") or item.get("dataVencimentoProjetado") or "",
                "descricao": item.get("descricao") or item.get("historico") or item.get("observacao") or item.get("notes") or item.get("description") or "Título a Receber",
                "nomeCliente": item.get("nomeCliente") or item.get("nomeFantasiaCliente") or item.get("cliente") or item.get("clientName") or "Extrato/Cliente",
                "valor": abs(raw_value),
                "rawValue": raw_value,
                "situacao": str(item.get("situacao") or item.get("status") or "ABERTO").upper(),
                "nomeObra": item.get("nomeObra") or building_info.get("name") or (f"Obra {building_id}" if building_id else "Obra sem nome"),
                "documentId": item.get("documentId") or "",
                "documentNumber": item.get("documentNumber") or "",
                "installmentNumber": item.get("installmentNumber"),
                "statementOrigin": item.get("statementOrigin") or "",
                "statementType": item.get("statementType") or "",
                "billId": item.get("billId"),
                "type": item.get("type") or "Income",
                "bankAccountCode": item.get("bankAccountCode") or "",
                "links": links,
            }
        )

    saldo_bancario = sum(
        _safe_float(item.get("rawValue"))
        for item in normalized_receivable
        if str(item.get("type") or "").strip().lower() != "expense"
    ) - sum(
        _safe_float(item.get("rawValue"))
        for item in normalized_receivable
        if str(item.get("type") or "").strip().lower() == "expense"
    )

    return {
        "obras": list(building_map.values()),
        "usuarios": [_normalize_user(user) for user in usuarios],
        "credores": [_normalize_creditor(credor) for credor in credores],
        "companies": [_normalize_company(company) for company in companies],
        "pedidos": normalized_orders,
        "financeiro": normalized_financial,
        "receber": normalized_receivable,
        "itensPedidos": {str(key): value for key, value in itens_pedidos.items()},
        "saldoBancario": saldo_bancario,
        "latestSync": read_sync_metadata(),
    }


def _normalize_response_payload(payload: dict[str, Any], db: Session) -> BootstrapResponse:
    normalized = _legacy_bootstrap_payload(db)
    if payload.get("latestSync"):
        normalized["latestSync"] = payload["latestSync"]
    if payload.get("itensPedidos"):
        normalized["itensPedidos"] = payload["itensPedidos"]
    return BootstrapResponse(**normalized)


async def _perform_sync() -> dict[str, Any]:
    started_at = utc_now_iso()

    obras = await sienge_client.fetch_obras()
    usuarios = await sienge_client.fetch_users()
    empresas = await sienge_client.fetch_empresas()
    credores = await sienge_client.fetch_credores()
    pedidos = await sienge_client.fetch_pedidos()
    financeiro = await sienge_client.fetch_financeiro()
    receber = await sienge_client.fetch_receber()
    itens_pedidos = await sienge_client.fetch_itens_pedidos()

    if not any([obras, usuarios, empresas, credores, pedidos, financeiro, receber, itens_pedidos]):
        metadata = {
            "status": "error",
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "message": "Sienge did not return any dataset",
            "counts": _cache_counts(),
        }
        write_sync_metadata(metadata)
        raise HTTPException(status_code=502, detail="Falha na sincronização com o Sienge: nenhum dado retornado.")

    if obras:
        _write_cached_dataset("obras.json", obras)
    if usuarios:
        _write_cached_dataset("usuarios.json", usuarios)
    if empresas:
        _write_cached_dataset("empresas.json", empresas)
    if credores:
        _write_cached_dataset("credores.json", credores)
    if pedidos:
        _write_cached_dataset("pedidos.json", pedidos)
    if financeiro:
        _write_cached_dataset("financeiro.json", financeiro)
    if receber:
        _write_cached_dataset("receber.json", receber)
    if itens_pedidos:
        _write_cached_dataset("itens_pedidos.json", itens_pedidos)

    metadata = {
        "status": "success",
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "message": "Sincronizado com sucesso no Sienge",
        "counts": {
            "obras": len(obras),
            "usuarios": len(usuarios),
            "empresas": len(empresas),
            "credores": len(credores),
            "pedidos": len(pedidos),
            "financeiro": len(financeiro),
            "receber": len(receber),
            "itensPedidos": len(itens_pedidos),
        },
    }
    write_sync_metadata(metadata)

    return {
        "latestSync": metadata,
        "itensPedidos": {str(key): value for key, value in itens_pedidos.items()},
    }


@router.get("/test")
async def test_connection(db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        _ = db.scalar(select(Company).limit(1))
        sienge_status = await sienge_client.test_connection()
        counts = _cache_counts()
        has_cache = any(counts.values())
        live = sienge_status.get("live", {"ok": False})
        return {
            "ok": bool(live.get("ok")) or has_cache,
            "live": live,
            "cache": counts,
            "latestSync": read_sync_metadata(),
            "database": {"ok": True},
        }
    except Exception as e:
        return {
            "ok": False,
            "live": {"ok": False, "error": str(e)},
            "cache": _cache_counts(),
            "latestSync": read_sync_metadata(),
            "database": {"ok": False, "error": str(e)},
        }


@router.get("/bootstrap", response_model=BootstrapResponse)
async def bootstrap(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> BootstrapResponse:
    counts = _cache_counts()
    if not any(counts.values()):
        try:
            await _perform_sync()
        except HTTPException:
            pass
    return _normalize_response_payload({}, db)


@router.post("/sync")
async def sync(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    payload = await _perform_sync()
    return {
        "status": "ok",
        "message": "Sync completed from Sienge API",
        "synced": True,
        "latestSync": payload["latestSync"],
        "data": payload["latestSync"]["counts"],
    }


@router.post("/fetch-items")
async def fetch_items(
    payload: FetchItemsRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    try:
        items_map = _read_cached_dataset("itens_pedidos.json", {}) or {}
        changed = False
        requested_ids = {str(order_id) for order_id in payload.ids}

        for order_id in payload.ids:
            key = str(order_id)
            if items_map.get(key):
                continue
            items = await sienge_client.fetch_purchase_order_items(order_id)
            if items:
                items_map[key] = items
                changed = True

        if changed:
            _write_cached_dataset("itens_pedidos.json", items_map)

        return {str(key): value for key, value in items_map.items() if str(key) in requested_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/fetch-quotations")
async def fetch_quotations(
    payload: FetchQuotationsRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        target_ids = {str(order_id) for order_id in payload.ids}
        quotations_map = _read_cached_dataset("cotacoes_pedidos.json", {}) or {}
        items_map = _read_cached_dataset("itens_pedidos.json", {}) or {}
        pedidos = _to_array(_read_cached_dataset("pedidos.json", []))
        pedido_lookup = {
            str(item.get("id") or item.get("numero")): item
            for item in pedidos
            if item.get("id") or item.get("numero")
        }
        changed = False

        def build_quote(oid: str, order_info: dict[str, Any], order_items: list[dict]) -> dict[str, Any]:
            supplier_id = order_info.get("supplierId") or order_info.get("codigoFornecedor")
            return {
                "orderId": int(oid) if oid.isdigit() else 0,
                "supplierId": supplier_id,
                "creditorId": supplier_id,
                "supplierName": order_info.get("nomeFornecedor"),
                "date": order_info.get("date") or order_info.get("dataEmissao") or "",
                "totalAmount": _safe_float(order_info.get("totalAmount") or order_info.get("valorTotal")),
                "items": [
                    {
                        "description": item.get("resourceDescription") or item.get("descricao") or "",
                        "resourceId": item.get("resourceId"),
                        "unitPrice": _safe_float(item.get("netPrice") or item.get("unitPrice") or item.get("valorUnitario")),
                        "quantity": _safe_float(item.get("quantity") or item.get("quantidade")),
                        "unitOfMeasure": item.get("unitOfMeasure") or item.get("unidadeMedidaSigla") or "",
                        "quotationIds": [pq.get("purchaseQuotationId") for pq in (item.get("purchaseQuotations") or [])],
                    }
                    for item in order_items
                ],
            }

        quotation_index: dict[int, list[str]] = {}
        for oid, order_items in items_map.items():
            if not isinstance(order_items, list):
                continue
            for item in order_items:
                for quotation in item.get("purchaseQuotations") or []:
                    quotation_id = quotation.get("purchaseQuotationId")
                    if quotation_id:
                        quotation_index.setdefault(int(quotation_id), [])
                        if oid not in quotation_index[int(quotation_id)]:
                            quotation_index[int(quotation_id)].append(oid)

        for order_id in payload.ids:
            key = str(order_id)
            if quotations_map.get(key):
                continue

            order_items = items_map.get(key)
            if not order_items:
                order_items = await sienge_client.fetch_purchase_order_items(order_id)
                if order_items:
                    items_map[key] = order_items
                    changed = True

            if not isinstance(order_items, list) or not order_items:
                quotations_map[key] = []
                changed = True
                continue

            quotation_ids: set[int] = set()
            for item in order_items:
                for quotation in item.get("purchaseQuotations") or []:
                    quotation_id = quotation.get("purchaseQuotationId")
                    if quotation_id:
                        quotation_ids.add(int(quotation_id))

            if not quotation_ids:
                quotations_map[key] = []
                changed = True
                continue

            competitor_ids: set[str] = set()
            for quotation_id in quotation_ids:
                for candidate_order_id in quotation_index.get(quotation_id, []):
                    if candidate_order_id != key:
                        competitor_ids.add(candidate_order_id)

            competitor_quotes: list[dict[str, Any]] = []
            for competitor_id in competitor_ids:
                competitor_items = items_map.get(competitor_id)
                if not competitor_items and competitor_id.isdigit():
                    fetched_items = await sienge_client.fetch_purchase_order_items(int(competitor_id))
                    if fetched_items:
                        competitor_items = fetched_items
                        items_map[competitor_id] = fetched_items
                        changed = True
                if competitor_items:
                    competitor_quotes.append(build_quote(competitor_id, pedido_lookup.get(competitor_id, {}), competitor_items))

            quotation_meta = await sienge_client.fetch_purchase_quotation(next(iter(quotation_ids)))
            winning_order = pedido_lookup.get(key, {})
            competitor_quotes.append(build_quote(key, winning_order, order_items))
            competitor_quotes.sort(key=lambda item: item.get("orderId") or 0)

            quotations_map[key] = {
                "quotes": competitor_quotes,
                "quotationIds": sorted(quotation_ids),
                "quotationMeta": quotation_meta,
                "winningSupplier": winning_order.get("supplierId") or winning_order.get("codigoFornecedor"),
            }
            changed = True

        if changed:
            _write_cached_dataset("itens_pedidos.json", items_map)
            _write_cached_dataset("cotacoes_pedidos.json", quotations_map)

        return {key: value for key, value in quotations_map.items() if key in target_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
