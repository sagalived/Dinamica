from typing import Any
from datetime import datetime
import threading

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.dependencies import get_current_user
from backend.models import AppUser, Building, Company, Creditor, DirectoryUser
from backend.schemas import BootstrapResponse, FetchItemsRequest, FetchQuotationsRequest
from backend.services.sienge_cache import utc_now_iso
from backend.services.sienge_client import sienge_client
from backend.services.sienge_storage import (
    read_snapshot,
    read_sync_metadata,
    write_snapshot,
    write_sync_metadata,
)

router = APIRouter(prefix="/api/sienge", tags=["sienge"])
_SYNC_LOCK = threading.Lock()
_SYNC_STATE: dict[str, Any] = {
    "running": False,
    "source": None,
    "started_at": None,
}


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


def _read_cached_dataset(db: Session, filename: str, default: Any) -> Any:
    return read_snapshot(db, filename, default=default)


def _write_cached_dataset(db: Session, filename: str, payload: Any) -> None:
    write_snapshot(db, filename, payload)


def _cache_counts(db: Session) -> dict[str, int]:
    return {
        "obras": len(_to_array(_read_cached_dataset(db, "obras.json", []))),
        "usuarios": len(_to_array(_read_cached_dataset(db, "usuarios.json", []))),
        "credores": len(_to_array(_read_cached_dataset(db, "credores.json", []))),
        "empresas": len(_to_array(_read_cached_dataset(db, "empresas.json", []))),
        "pedidos": len(_to_array(_read_cached_dataset(db, "pedidos.json", []))),
        "financeiro": len(_to_array(_read_cached_dataset(db, "financeiro.json", []))),
        "receber": len(_to_array(_read_cached_dataset(db, "receber.json", []))),
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


def _to_date_number(value: Any) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
        try:
            return int(datetime.strptime(raw[:19], fmt).timestamp() * 1000)
        except ValueError:
            continue
    return 0


def _in_range(date_number: int, start_ms: int | None, end_exclusive_ms: int | None) -> bool:
    if start_ms is None and end_exclusive_ms is None:
        return True
    if not date_number:
        return False
    if start_ms is not None and date_number < start_ms:
        return False
    if end_exclusive_ms is not None and date_number >= end_exclusive_ms:
        return False
    return True


def _date_start_ms(value: str | None) -> int | None:
    if not value:
        return None
    return _to_date_number(value)


def _date_end_exclusive_ms(value: str | None) -> int | None:
    if not value:
        return None
    base = _to_date_number(value)
    if base == 0:
        return None
    return base + 24 * 60 * 60 * 1000


def _legacy_bootstrap_payload(db: Session, include_transactions: bool = True) -> dict[str, Any]:
    obras = _to_array(_read_cached_dataset(db, "obras.json", []))
    usuarios = _to_array(_read_cached_dataset(db, "usuarios.json", []))
    credores = _to_array(_read_cached_dataset(db, "credores.json", []))
    companies = _to_array(_read_cached_dataset(db, "empresas.json", []))
    if include_transactions:
        pedidos = _to_array(_read_cached_dataset(db, "pedidos.json", []))
        financeiro = _to_array(_read_cached_dataset(db, "financeiro.json", []))
        receber = _to_array(_read_cached_dataset(db, "receber.json", []))
        itens_pedidos = _read_cached_dataset(db, "itens_pedidos.json", {}) or {}
    else:
        pedidos = []
        financeiro = []
        receber = []
        itens_pedidos = {}

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
        for bid in {
            str(normalized.get("code") or ""),
            str(normalized.get("id") or ""),
        }:
            if bid and bid not in {"None", "undefined"}:
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
        company_id = (
            item.get("companyId")
            or next(
                (
                    lnk["href"].rstrip("/").split("/")[-1]
                    for lnk in (item.get("links") or [])
                    if lnk.get("rel") == "company" and lnk.get("href")
                ),
                None,
            )
            or building_info.get("companyId")
        )
        name = item.get("nomeCredor") or item.get("creditorName") or item.get("nomeFantasiaCredor") or item.get("fornecedor") or item.get("credor") or creditor_map.get(creditor_id) or "Credor sem nome"
        normalized_financial.append(
            {
                "id": item.get("id") or item.get("numero") or item.get("codigoTitulo") or item.get("documentNumber") or 0,
                "companyId": int(company_id) if str(company_id).isdigit() else company_id,
                "creditorId": creditor_id,
                "buildingId": int(building_id) if building_id.isdigit() else 0,
                "idObra": int(building_id) if building_id.isdigit() else 0,
                "codigoObra": building_id,
                "dataVencimento": item.get("dataVencimento") or item.get("issueDate") or item.get("dueDate") or item.get("dataVencimentoProjetado") or item.get("dataEmissao") or item.get("dataContabil") or "",
                "descricao": item.get("descricao") or item.get("historico") or item.get("tipoDocumento") or item.get("notes") or item.get("observacao") or "Título a Pagar",
                "valor": _safe_float(item.get("totalInvoiceAmount") or item.get("valor") or item.get("amount") or item.get("valorTotal") or item.get("valorLiquido") or item.get("valorBruto")),
                "situacao": item.get("situacao") or item.get("status") or "Pendente",
                "creditorName": name,
                "nomeCredor": name,
                "nomeObra": item.get("nomeObra") or building_info.get("name") or (f"Obra {building_id}" if building_id else "Obra sem nome"),
                "documentNumber": item.get("documentNumber") or item.get("numeroDocumento") or item.get("numero") or item.get("codigoTitulo") or "",
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
                "codigoObra": building_id,
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
                # bankAccountCode extraído do link rel="bank-account" (ex: ...bank-account/SANTANDER)
                "bankAccountCode": (
                    item.get("bankAccountCode")
                    or next(
                        (lnk["href"].rstrip("/").split("/")[-1]
                         for lnk in links
                         if lnk.get("rel") == "bank-account" and lnk.get("href")),
                        "",
                    )
                ),
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
        "latestSync": read_sync_metadata(db),
    }


def _normalize_response_payload(payload: dict[str, Any], db: Session, include_transactions: bool = False) -> BootstrapResponse:
    normalized = _legacy_bootstrap_payload(db, include_transactions=include_transactions)
    if payload.get("latestSync"):
        normalized["latestSync"] = payload["latestSync"]
    if payload.get("itensPedidos"):
        normalized["itensPedidos"] = payload["itensPedidos"]
    return BootstrapResponse(**normalized)


def get_sync_state() -> dict[str, Any]:
    return {
        "running": bool(_SYNC_STATE.get("running")),
        "source": _SYNC_STATE.get("source"),
        "started_at": _SYNC_STATE.get("started_at"),
    }


async def run_sync_once(db: Session, source: str = "manual") -> dict[str, Any]:
    acquired = _SYNC_LOCK.acquire(blocking=False)
    if not acquired:
        latest_sync = read_sync_metadata(db) or {}
        return {
            "latestSync": latest_sync,
            "itensPedidos": _read_cached_dataset(db, "itens_pedidos.json", {}) or {},
            "synced": False,
            "source": latest_sync.get("source") or "cache",
            "in_progress": True,
            "message": "Sincronizacao ja em andamento.",
        }

    _SYNC_STATE["running"] = True
    _SYNC_STATE["source"] = source
    _SYNC_STATE["started_at"] = utc_now_iso()

    try:
        payload = await _perform_sync(db)
        payload["in_progress"] = False
        payload["message"] = (payload.get("latestSync") or {}).get("message")
        return payload
    finally:
        _SYNC_STATE["running"] = False
        _SYNC_STATE["source"] = None
        _SYNC_STATE["started_at"] = None
        _SYNC_LOCK.release()


async def _perform_sync(db: Session) -> dict[str, Any]:
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
        cached_counts = _cache_counts(db)
        has_cache = any(cached_counts.values())
        diagnostic = sienge_client.last_error or {}
        status_code = diagnostic.get("status_code")
        reason = "SIENGE indisponível"
        if status_code == 401:
            reason = "SIENGE retornou 401 (credenciais inválidas/expiradas)"

        metadata = {
            "status": "degraded" if has_cache else "error",
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "message": (
                f"{reason}. Usando cache local." if has_cache else f"{reason}. Cache local vazio."
            ),
            "counts": cached_counts,
            "source": "cache" if has_cache else "none",
        }
        write_sync_metadata(db, metadata)
        return {
            "latestSync": metadata,
            "itensPedidos": _read_cached_dataset(db, "itens_pedidos.json", {}) or {},
            "synced": False,
            "source": metadata["source"],
        }

    if obras:
        _write_cached_dataset(db, "obras.json", obras)
    if usuarios:
        _write_cached_dataset(db, "usuarios.json", usuarios)
    if empresas:
        _write_cached_dataset(db, "empresas.json", empresas)
    if credores:
        _write_cached_dataset(db, "credores.json", credores)
    if pedidos:
        _write_cached_dataset(db, "pedidos.json", pedidos)
    if financeiro:
        _write_cached_dataset(db, "financeiro.json", financeiro)
    if receber:
        _write_cached_dataset(db, "receber.json", receber)
    if itens_pedidos:
        _write_cached_dataset(db, "itens_pedidos.json", itens_pedidos)

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
    write_sync_metadata(db, metadata)

    return {
        "latestSync": metadata,
        "itensPedidos": {str(key): value for key, value in itens_pedidos.items()},
        "synced": True,
        "source": "sienge_live",
    }


@router.get("/test")
async def test_connection(db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        _ = db.scalar(select(Company).limit(1))
        counts = _cache_counts(db)
        has_cache = any(counts.values())
        latest_sync = read_sync_metadata(db) or {}
        sync_status = str(latest_sync.get("status") or "unknown")
        live_ok = sync_status == "success"
        live = {
            "ok": live_ok,
            "status": sync_status,
            "message": latest_sync.get("message") or "Sem sincronizacao recente.",
        }
        return {
            "ok": live_ok or has_cache,
            "live": live,
            "cache": counts,
            "latestSync": latest_sync,
            "syncState": get_sync_state(),
            "database": {"ok": True},
        }
    except Exception as e:
        return {
            "ok": False,
            "live": {"ok": False, "error": str(e)},
            "cache": _cache_counts(db),
            "latestSync": read_sync_metadata(db),
            "syncState": get_sync_state(),
            "database": {"ok": False, "error": str(e)},
        }


@router.get("/bootstrap", response_model=BootstrapResponse)
async def bootstrap(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> BootstrapResponse:
    # Bootstrap leve: retorna apenas metadados (obras/usuarios/credores).
    # Pedidos/financeiro/receber chegam via /filtered (filtrado, pequeno).
    result = _normalize_response_payload({}, db)
    counts = _cache_counts(db)
    result.cacheReady = counts.get("pedidos", 0) > 0 or counts.get("financeiro", 0) > 0
    result.cacheCounts = counts
    return result


@router.post("/sync")
async def sync(
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    payload = await run_sync_once(db, source="manual")
    latest_sync = payload.get("latestSync", {})
    synced = bool(payload.get("synced", False))
    in_progress = bool(payload.get("in_progress", False))
    degraded = latest_sync.get("status") == "degraded"
    return {
        "status": "in_progress" if in_progress else ("ok" if synced else ("degraded" if degraded else "error")),
        "message": (
            payload.get("message")
            or latest_sync.get("message")
            or ("Sync completed from Sienge API" if synced else "Sync executado com fallback")
        ),
        "synced": synced,
        "in_progress": in_progress,
        "syncState": get_sync_state(),
        "source": payload.get("source", "unknown"),
        "latestSync": latest_sync,
        "data": latest_sync.get("counts", {}),
    }


@router.get("/filtered")
async def filtered_data(
    start_date: str | None = None,
    end_date: str | None = None,
    company_id: str = "all",
    user_id: str = "all",
    requester_id: str = "all",
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    payload = _legacy_bootstrap_payload(db)

    obras = _to_array(payload.get("obras", []))
    pedidos = _to_array(payload.get("pedidos", []))
    financeiro = _to_array(payload.get("financeiro", []))
    receber = _to_array(payload.get("receber", []))

    building_company_map: dict[str, str] = {}
    for obra in obras:
        cid = str(obra.get("companyId") or obra.get("idCompany") or "")
        if not cid:
            continue
        id_candidates = {
            str(obra.get("id") or ""),
            str(obra.get("code") or ""),
            str(obra.get("codigoVisivel") or ""),
        }
        for bid in id_candidates:
            if bid and bid not in {"None", "undefined"}:
                building_company_map[bid] = cid

    start_ms = _date_start_ms(start_date)
    end_exclusive_ms = _date_end_exclusive_ms(end_date)

    def order_company(order: dict[str, Any]) -> str:
        direct = order.get("companyId")
        if direct is not None and str(direct) not in {"", "None", "undefined"}:
            return str(direct)
        bid = str(order.get("buildingId") or order.get("idObra") or order.get("codigoVisivelObra") or "")
        return building_company_map.get(bid, "")

    filtered_orders = []
    for order in pedidos:
        date_numeric = _to_date_number(order.get("date") or order.get("dataEmissao"))
        if not _in_range(date_numeric, start_ms, end_exclusive_ms):
            continue
        if company_id != "all" and order_company(order) != company_id:
            continue
        if user_id != "all" and str(order.get("buyerId") or order.get("idComprador") or "") != user_id:
            continue
        if requester_id != "all" and str(order.get("requesterId") or order.get("solicitante") or "") != requester_id:
            continue
        filtered_orders.append(order)

    def financial_company(item: dict[str, Any]) -> str:
        direct = item.get("companyId")
        if direct is not None and str(direct) not in {"", "None", "undefined"}:
            return str(direct)
        bid = str(item.get("buildingId") or item.get("idObra") or item.get("codigoObra") or "")
        return building_company_map.get(bid, "")

    filtered_financial = []
    for item in financeiro:
        date_numeric = _to_date_number(
            item.get("dataVencimento")
            or item.get("dueDate")
            or item.get("issueDate")
            or item.get("dataVencimentoProjetado")
            or item.get("dataEmissao")
            or item.get("dataContabil")
        )
        if not _in_range(date_numeric, start_ms, end_exclusive_ms):
            continue
        if company_id != "all" and financial_company(item) != company_id:
            continue
        filtered_financial.append(item)

    filtered_receber = []
    for item in receber:
        date_numeric = _to_date_number(
            item.get("dataVencimento")
            or item.get("dueDate")
            or item.get("data")
            or item.get("date")
            or item.get("dataEmissao")
            or item.get("issueDate")
            or item.get("dataVencimentoProjetado")
        )
        if not _in_range(date_numeric, start_ms, end_exclusive_ms):
            continue
        if company_id != "all" and financial_company(item) != company_id:
            continue
        filtered_receber.append(item)

    return {
        "pedidos": filtered_orders,
        "financeiro": filtered_financial,
        "receber": filtered_receber,
        "latestSync": payload.get("latestSync"),
        "filters": {
            "start_date": start_date,
            "end_date": end_date,
            "company_id": company_id,
            "user_id": user_id,
            "requester_id": requester_id,
        },
        "counts": {
            "pedidos": len(filtered_orders),
            "financeiro": len(filtered_financial),
            "receber": len(filtered_receber),
        },
    }


@router.post("/fetch-items")
async def fetch_items(
    payload: FetchItemsRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict[str, list[dict]]:
    try:
        items_map = _read_cached_dataset(db, "itens_pedidos.json", {}) or {}
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
            _write_cached_dataset(db, "itens_pedidos.json", items_map)

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
        quotations_map = _read_cached_dataset(db, "cotacoes_pedidos.json", {}) or {}
        items_map = _read_cached_dataset(db, "itens_pedidos.json", {}) or {}
        pedidos = _to_array(_read_cached_dataset(db, "pedidos.json", []))
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
            _write_cached_dataset(db, "itens_pedidos.json", items_map)
            _write_cached_dataset(db, "cotacoes_pedidos.json", quotations_map)

        return {key: value for key, value in quotations_map.items() if key in target_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
