from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import select, func
from sqlalchemy.orm import Session, selectinload

from backend.database import get_db
from backend.dependencies import get_current_user
from backend.models import AppUser, Building, Contract, ContractDocument, Sprint
from backend.schemas import ContractRequest, ContractResponse, ContractDocumentResponse
from backend.services.sienge_raw_reader import read_categoria_generica
from backend.services.sienge_client import sienge_client
from backend.services.sienge_storage import read_snapshot, write_snapshot
from backend.services.catalog_sync import upsert_catalog_from_sienge

router = APIRouter(prefix="/api/juridico", tags=["juridico"])

_UPLOAD_DIR = Path("uploads/contracts")
_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _safe_filename(name: str) -> str:
    # simples e suficiente p/ Windows/Linux
    keep = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._- "
    cleaned = "".join(c for c in (name or "") if c in keep).strip()
    return cleaned or f"file_{int(datetime.utcnow().timestamp())}.bin"


def _render_contract_html(contract: Contract) -> str:
    # Template simples (HTML) pronto para impressão.
    # Evolução: versionamento de template por tipo de contrato.
    def s(v: Any | None) -> str:
        return (str(v).strip() if v is not None else "")

    def money(v: Any | None) -> str:
        try:
            if v is None:
                return ""
            return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return s(v)

    start = contract.start_date.strftime("%d/%m/%Y") if contract.start_date else ""
    end = contract.end_date.strftime("%d/%m/%Y") if contract.end_date else ""

    return f"""<!doctype html>
<html lang=\"pt-br\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Contrato {s(contract.contract_number) or contract.id}</title>
  <style>
    @page {{ size: A4; margin: 18mm; }}
    body {{ font-family: Arial, Helvetica, sans-serif; color: #111; }}
    .meta {{ font-size: 12px; color: #444; }}
    h1 {{ font-size: 18px; margin: 0 0 8px; }}
    h2 {{ font-size: 14px; margin: 18px 0 6px; }}
    p, li {{ font-size: 12.5px; line-height: 1.45; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
    .box {{ border: 1px solid #ddd; border-radius: 8px; padding: 10px; }}
    .label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: .08em; }}
    .value {{ font-size: 12.5px; font-weight: 600; margin-top: 3px; white-space: pre-wrap; }}
    .hr {{ height: 1px; background: #eee; margin: 14px 0; }}
    .sig {{ margin-top: 28px; display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }}
    .line {{ border-top: 1px solid #999; padding-top: 6px; font-size: 12px; color: #333; }}
  </style>
</head>
<body>
  <div class=\"meta\">Dinâmica · Jurídico / Contratos</div>
  <h1>{s(contract.title)}</h1>
  <div class=\"meta\">Contrato: <b>{s(contract.contract_number) or contract.id}</b> · Status: <b>{s(contract.status)}</b></div>

  <div class=\"hr\"></div>

  <div class=\"grid\">
    <div class=\"box\">
      <div class=\"label\">Contratante</div>
      <div class=\"value\">{s(contract.owner_legal_name)}\nCNPJ: {s(contract.owner_cnpj)}\n{s(contract.owner_address)}\n{s(contract.owner_representatives)}</div>
    </div>
    <div class=\"box\">
      <div class=\"label\">Contratada</div>
      <div class=\"value\">{s(contract.supplier_legal_name)}\nCNPJ: {s(contract.supplier_cnpj)}\n{s(contract.supplier_address)}\n{s(contract.supplier_representatives)}</div>
    </div>
  </div>

  <h2>Objeto</h2>
  <p>{s(contract.object_text)}</p>

  <h2>Vigência</h2>
  <p>Início: <b>{start}</b> · Fim: <b>{end}</b></p>

  <h2>Valores</h2>
  <ul>
    <li>Valor total: <b>{money(contract.total_value)}</b></li>
    <li>Retenção: <b>{s(contract.retention_percent)}%</b></li>
  </ul>

  <h2>Garantias e Seguros</h2>
  <p><b>Garantias:</b> {s(contract.guarantee_text)}</p>
  <p><b>Seguros:</b> {s(contract.insurance_text)}</p>

  <h2>Observações</h2>
  <p>{s(contract.notes)}</p>

  <div class=\"sig\">
    <div class=\"line\">Assinatura - Contratante</div>
    <div class=\"line\">Assinatura - Contratada</div>
  </div>
</body>
</html>"""


@router.get("/contratos")
def list_contratos(
    building_id: int | None = Query(default=None),
    q: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    stmt = select(Contract).options(selectinload(Contract.documents)).order_by(Contract.created_at.desc())
    if building_id is not None:
        stmt = stmt.where(Contract.building_id == int(building_id))
    if q:
        term = f"%{q.strip().lower()}%"
        stmt = stmt.where(
            func.lower(func.coalesce(Contract.title, "")).like(term)
            | func.lower(func.coalesce(Contract.contract_number, "")).like(term)
            | func.lower(func.coalesce(Contract.supplier_legal_name, "")).like(term)
        )

    if kind and kind.strip().lower() not in {"all", "todos", "todas"}:
        stmt = stmt.where(func.lower(Contract.kind) == kind.strip().lower())

    rows = db.scalars(stmt.limit(limit)).all()
    return {"total": len(rows), "contracts": [ContractResponse.model_validate(r).model_dump() for r in rows]}


def _pick_first(row: dict, keys: list[str]) -> str:
    for k in keys:
        if k in row and row.get(k) not in (None, ""):
            return str(row.get(k)).strip()
        # tentativa case-insensitive
        for rk, rv in row.items():
            if str(rk).strip().lower() == k.strip().lower() and rv not in (None, ""):
                return str(rv).strip()
    return ""


def _norm(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


@router.post("/contratos/sync-sienge")
def sync_contratos_from_sienge_raw(
    create_missing_buildings: bool = Query(default=True),
    limit: int = Query(default=2000, ge=1, le=20000),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Importa contratos a partir do export bruto em `data/sienge_raw/contratos`.

    - Lê arquivo `data/sienge_raw/contratos/contratos.(csv|txt)` (separador ';')
    - Faz upsert no banco local por (building + contract_number)
    - Se não encontrar obra e `create_missing_buildings=true`, cria obra local automaticamente.
    """

    raw = read_categoria_generica("contratos")
    rows = list(raw.get("rows") or [])
    created_template = bool(raw.get("created"))
    if created_template:
        return {
            "source": raw.get("source"),
            "total": 0,
            "created": 0,
            "updated": 0,
            "skipped": 0,
            "message": raw.get("message") or "Template criado para contratos. Preencha e rode sync novamente.",
        }

    rows = rows[: int(limit)]

    building_name_keys = ["OBRA", "obra", "NOME_OBRA", "NOME DA OBRA", "BUILDING", "building"]
    building_id_keys = ["ID_OBRA", "id_obra", "BUILDING_ID", "building_id"]
    contract_number_keys = [
        "NUMERO",
        "NÚMERO",
        "CONTRATO",
        "CONTRATO_NUMERO",
        "NR_CONTRATO",
        "N_CONTRATO",
        "contract_number",
        "contractNumber",
    ]
    title_keys = ["TITULO", "TÍTULO", "TITULO_CONTRATO", "DESCRICAO", "DESCRIÇÃO", "OBJETO", "title"]
    supplier_keys = ["FORNECEDOR", "Fornecedor", "CONTRATADA", "RAZAO_SOCIAL", "RAZÃO SOCIAL", "SUPPLIER"]
    supplier_cnpj_keys = ["CNPJ_FORNECEDOR", "CNPJ", "CNPJ CONTRATADA", "supplier_cnpj", "supplierCnpj"]
    status_keys = ["STATUS", "status"]

    # Cache de obras por nome normalizado
    buildings_by_norm: dict[str, Building] = {}
    for b in db.scalars(select(Building)).all():
        buildings_by_norm[_norm(b.name)] = b

    created = 0
    updated = 0
    skipped = 0
    created_buildings = 0

    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue

        building_name = _pick_first(row, building_name_keys)
        building_id_raw = _pick_first(row, building_id_keys)
        contract_number = _pick_first(row, contract_number_keys)
        title = _pick_first(row, title_keys)
        supplier_name = _pick_first(row, supplier_keys)
        supplier_cnpj = _pick_first(row, supplier_cnpj_keys)
        status = _pick_first(row, status_keys) or "draft"

        building: Building | None = None
        # Se o arquivo trouxer um ID que coincida com o nosso banco local, usa.
        if building_id_raw.strip().isdigit():
            building = db.scalar(select(Building).where(Building.id == int(building_id_raw.strip())))

        if building is None and building_name:
            building = buildings_by_norm.get(_norm(building_name))

        if building is None and building_name and create_missing_buildings:
            building = Building(
                name=building_name.strip(),
                company_id=None,
                company_name=None,
                cnpj=None,
                address=None,
                building_type=None,
                active=True,
                created_by=current_user.email,
                modified_by=current_user.email,
            )
            db.add(building)
            db.flush()  # obtém building.id
            buildings_by_norm[_norm(building.name)] = building
            created_buildings += 1

        if building is None:
            skipped += 1
            continue

        clean_number = contract_number.strip() or None
        clean_title = title.strip() or (f"Contrato {clean_number}" if clean_number else "Contrato (Sienge)")
        clean_supplier = supplier_name.strip() or None
        clean_supplier_cnpj = supplier_cnpj.strip() or None
        clean_status = (status or "draft").strip() or "draft"

        existing: Contract | None = None
        if clean_number:
            existing = db.scalar(
                select(Contract).where(
                    Contract.building_id == int(building.id),
                    Contract.contract_number == clean_number,
                )
            )

        if existing is None and clean_number:
            # fallback: tenta por número (sem obra) para evitar duplicar se obra não veio correta no arquivo
            existing = db.scalar(select(Contract).where(Contract.contract_number == clean_number).order_by(Contract.id.desc()))

        if existing is None:
            contract = Contract(
                building_id=int(building.id),
                contract_number=clean_number,
                title=clean_title[:255],
                kind="suprimentos",
                supplier_legal_name=clean_supplier,
                supplier_cnpj=clean_supplier_cnpj,
                object_text=None,
                status=clean_status,
                created_by=current_user.email,
            )
            db.add(contract)
            created += 1
            continue

        changed = False
        if int(existing.building_id) != int(building.id):
            existing.building_id = int(building.id)
            changed = True
        if clean_title and (existing.title or "") != clean_title:
            existing.title = clean_title[:255]
            changed = True
        if clean_number and (existing.contract_number or "") != clean_number:
            existing.contract_number = clean_number
            changed = True
        if clean_supplier and (existing.supplier_legal_name or "") != clean_supplier:
            existing.supplier_legal_name = clean_supplier
            changed = True
        if clean_supplier_cnpj and (existing.supplier_cnpj or "") != clean_supplier_cnpj:
            existing.supplier_cnpj = clean_supplier_cnpj
            changed = True
        if clean_status and (existing.status or "") != clean_status:
            existing.status = clean_status
            changed = True

        if (existing.kind or "") != "suprimentos":
            existing.kind = "suprimentos"
            changed = True

        if changed:
            updated += 1

    db.commit()

    return {
        "source": raw.get("source"),
        "total": len(rows),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "created_buildings": created_buildings,
    }


def _safe_int(v: object) -> int | None:
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


_FIRST_INT_RE = re.compile(r"\d+")


def _first_int_from_text(v: object) -> int | None:
    if not isinstance(v, str):
        return None
    m = _FIRST_INT_RE.search(v)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


def _sienge_contract_to_local_fields(payload: dict, *, kind: str) -> dict:
    """Mapeia payload do Sienge (contratos) para campos do nosso Contract.

    Observação: o schema do Sienge pode variar por endpoint/versão. Aqui priorizamos
    os campos mais comuns e mantemos fallbacks.
    """
    building_id = (
        payload.get("enterpriseId")
        or payload.get("buildingId")
        or payload.get("idObra")
        or payload.get("workId")
        or payload.get("obraId")
        or payload.get("cdObra")
        or payload.get("codigoObra")
    )
    if building_id is None:
        # No endpoint de suprimentos, a obra costuma vir como string em `obras`.
        # Ex.: "28 - NOME DA OBRA ..."; pegamos o primeiro número como id.
        building_id = _first_int_from_text(payload.get("obras"))

    number = (
        payload.get("number")
        or payload.get("contractNumber")
        or payload.get("nuContrato")
        or payload.get("cdDocumento")
        or payload.get("codigo")
        or payload.get("code")
        or payload.get("id")
    )
    title = (
        payload.get("title")
        or payload.get("description")
        or payload.get("object")
        or payload.get("objeto")
        or payload.get("objetoDoContrato")
        or ""
    )
    supplier = (
        payload.get("supplierName")
        or payload.get("creditorName")
        or payload.get("fornecedor")
        or payload.get("supplier")
        or payload.get("nmFornecedor")
        or ""
    )
    status = payload.get("status") or payload.get("situacao") or payload.get("tpSituacao") or "draft"

    contract_number = str(number).strip() if number is not None else None
    title_s = str(title).strip()
    if not title_s:
        title_s = f"Contrato {contract_number}" if contract_number else "Contrato (Sienge)"

    return {
        "building_id": _safe_int(building_id),
        "contract_number": contract_number,
        "title": title_s,
        "supplier_legal_name": str(supplier).strip() or None,
        "status": str(status).strip() or "draft",
        "kind": kind,
    }


def _normalize_kind(v: str | None) -> str:
    raw = (v or "privado").strip().lower()
    # normaliza acentos mais comuns
    raw = raw.replace("ç", "c").replace("ã", "a").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    allowed = {"suprimentos", "medicoes", "privado", "licitacao"}
    return raw if raw in allowed else "privado"


@router.post("/contratos/sync-sienge-live")
async def sync_contratos_from_sienge_live(
    use_cache: bool = Query(default=True),
    refresh_cache: bool = Query(default=False),
    limit_per_kind: int = Query(default=20000, ge=1, le=20000),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    """Baixa contratos do Sienge (ao vivo) e salva no banco local.

    Tipos importados:
    - suprimentos
    - medicoes
    """

    cache_key_sup = "juridico/contratos_suprimentos.json"
    cache_key_med = "juridico/contratos_medicoes.json"
    cache_key_meta = "juridico/contratos_meta.json"

    suprimentos_rows: list[dict] = []
    medicoes_rows: list[dict] = []
    suprimentos_diag: dict | None = None
    medicoes_diag: dict | None = None
    source = ""

    if use_cache and not refresh_cache:
        cached_sup = read_snapshot(db, cache_key_sup, default=[]) or []
        cached_med = read_snapshot(db, cache_key_med, default=[]) or []
        if isinstance(cached_sup, list) and isinstance(cached_med, list) and (cached_sup or cached_med):
            suprimentos_rows = [x for x in cached_sup if isinstance(x, dict)][: int(limit_per_kind)]
            medicoes_rows = [x for x in cached_med if isinstance(x, dict)][: int(limit_per_kind)]
            source = "cache"

    if not source:
        if not sienge_client.is_configured:
            return {
                "ok": False,
                "message": "SIENGE não configurado (.env) e cache de contratos ainda não existe.",
                "diagnostic": sienge_client.last_error,
            }

        suprimentos_rows_raw, suprimentos_diag = await sienge_client.fetch_suprimentos_contratos()
        medicoes_rows_raw, medicoes_diag = await sienge_client.fetch_medicoes_contratos()
        suprimentos_rows = [x for x in (suprimentos_rows_raw or []) if isinstance(x, dict)][: int(limit_per_kind)]
        medicoes_rows = [x for x in (medicoes_rows_raw or []) if isinstance(x, dict)][: int(limit_per_kind)]
        source = "sienge_live"

        total_live = len(suprimentos_rows) + len(medicoes_rows)
        if total_live <= 0:
            # Não cacheia vazio: evita "envenenar" o cache quando o endpoint está errado
            # ou quando há falta de permissões/parametrização.
            write_snapshot(
                db,
                cache_key_meta,
                {
                    "ok": False,
                    "source": source,
                    "cached_at": datetime.utcnow().isoformat(),
                    "counts": {"suprimentos": len(suprimentos_rows), "medicoes": len(medicoes_rows)},
                    "diagnostic": {"suprimentos": suprimentos_diag, "medicoes": medicoes_diag},
                    "message": "SIENGE retornou 0 contratos. Cache NÃO foi atualizado. Verifique permissões/endpoint e tente novamente.",
                },
            )
            return {
                "ok": False,
                "source": source,
                "message": "SIENGE retornou 0 contratos (suprimentos+medições). Cache NÃO foi atualizado. Verifique permissões/endpoint e tente novamente.",
                "diagnostic": {"suprimentos": suprimentos_diag, "medicoes": medicoes_diag},
            }

        # Persistimos no banco para não precisar baixar novamente.
        write_snapshot(db, cache_key_sup, suprimentos_rows)
        write_snapshot(db, cache_key_med, medicoes_rows)
        write_snapshot(
            db,
            cache_key_meta,
            {
                "ok": True,
                "source": source,
                "cached_at": datetime.utcnow().isoformat(),
                "counts": {"suprimentos": len(suprimentos_rows), "medicoes": len(medicoes_rows)},
                "diagnostic": {"suprimentos": suprimentos_diag, "medicoes": medicoes_diag},
            },
        )

    rows_by_kind: list[tuple[str, list[dict], dict | None]] = [
        ("suprimentos", suprimentos_rows[: int(limit_per_kind)], suprimentos_diag),
        ("medicoes", medicoes_rows[: int(limit_per_kind)], medicoes_diag),
    ]

    created = 0
    updated = 0
    skipped = 0

    for kind, rows, err in rows_by_kind:
        if isinstance(err, dict) and err.get("ok") is False and not rows:
            continue
        for item in rows:
            if not isinstance(item, dict):
                skipped += 1
                continue
            mapped = _sienge_contract_to_local_fields(item, kind=kind)
            building_id = mapped.get("building_id")
            if not building_id:
                skipped += 1
                continue
            building = db.scalar(select(Building).where(Building.id == int(building_id)))
            if building is None:
                # tenta criar/atualizar a obra via cache de catlogo (obras.json), se existir
                obras_cache = read_snapshot(db, "obras.json", default=[]) or []
                if isinstance(obras_cache, list):
                    hint = next((x for x in obras_cache if isinstance(x, dict) and str(x.get("id")) == str(building_id)), None)
                    if isinstance(hint, dict):
                        try:
                            upsert_catalog_from_sienge(db, obras=[hint])
                        except Exception:
                            pass
                        building = db.scalar(select(Building).where(Building.id == int(building_id)))
                if building is None:
                    skipped += 1
                    continue

            contract_number = mapped.get("contract_number")
            title = mapped.get("title") or (f"Contrato {contract_number}" if contract_number else "Contrato")

            existing: Contract | None = None
            if contract_number:
                existing = db.scalar(
                    select(Contract).where(
                        Contract.building_id == int(building.id),
                        Contract.contract_number == str(contract_number),
                    )
                )

            if existing is None and contract_number:
                existing = db.scalar(select(Contract).where(Contract.contract_number == str(contract_number)).order_by(Contract.id.desc()))

            if existing is None:
                db.add(
                    Contract(
                        building_id=int(building.id),
                        contract_number=str(contract_number) if contract_number else None,
                        title=str(title)[:255],
                        kind=kind,
                        supplier_legal_name=mapped.get("supplier_legal_name"),
                        status=mapped.get("status") or "draft",
                        created_by=current_user.email,
                    )
                )
                created += 1
                continue

            changed = False
            if (existing.kind or "") != kind:
                existing.kind = kind
                changed = True
            if title and (existing.title or "") != str(title):
                existing.title = str(title)[:255]
                changed = True
            if contract_number and (existing.contract_number or "") != str(contract_number):
                existing.contract_number = str(contract_number)
                changed = True
            supplier_name = mapped.get("supplier_legal_name")
            if supplier_name and (existing.supplier_legal_name or "") != str(supplier_name):
                existing.supplier_legal_name = str(supplier_name)
                changed = True
            status = mapped.get("status")
            if status and (existing.status or "") != str(status):
                existing.status = str(status)
                changed = True

            if changed:
                updated += 1

    db.commit()

    return {
        "ok": True,
        "source": source,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "diagnostic": {
            "suprimentos": suprimentos_diag,
            "medicoes": medicoes_diag,
        },
    }


@router.get("/contratos/{contract_id}")
def get_contrato(
    contract_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    row = db.scalar(
        select(Contract).options(selectinload(Contract.documents)).where(Contract.id == int(contract_id))
    )
    if not row:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    return {"contract": ContractResponse.model_validate(row).model_dump()}


@router.post("/contratos")
def create_contrato(
    payload: ContractRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    building = db.scalar(select(Building).where(Building.id == int(payload.building_id)))
    if not building:
        raise HTTPException(status_code=400, detail="Obra inválida")

    contract = Contract(
        building_id=int(payload.building_id),
        cost_center_code=payload.cost_center_code,
        stage_label=payload.stage_label,
        enterprise_label=payload.enterprise_label,
        contract_number=payload.contract_number,
        title=payload.title,
        kind=_normalize_kind(payload.kind),
        owner_legal_name=payload.owner_legal_name,
        owner_cnpj=payload.owner_cnpj,
        owner_address=payload.owner_address,
        owner_representatives=payload.owner_representatives,
        supplier_legal_name=payload.supplier_legal_name,
        supplier_cnpj=payload.supplier_cnpj,
        supplier_address=payload.supplier_address,
        supplier_representatives=payload.supplier_representatives,
        object_text=payload.object_text,
        start_date=payload.start_date,
        end_date=payload.end_date,
        total_value=payload.total_value,
        retention_percent=payload.retention_percent,
        guarantee_text=payload.guarantee_text,
        insurance_text=payload.insurance_text,
        status=(payload.status or "draft").strip() or "draft",
        notes=payload.notes,
        created_by=current_user.email,
    )

    # criação do workspace/sprint operacional imediatamente após o cadastro
    sprint_name = f"Contrato {payload.contract_number or ''} - {payload.supplier_legal_name or payload.title}".strip()
    sprint = Sprint(
        building_id=int(payload.building_id),
        name=sprint_name[:255],
        start_date=payload.start_date,
        end_date=payload.end_date,
        color="#22c55e",  # usa verde padrão do sistema (tailwind green-500)
        created_by=current_user.email,
        is_active=True,
        contract_ref=(payload.contract_number or None),
    )

    db.add(sprint)
    db.flush()  # obtém sprint.id

    contract.sprint_id = sprint.id
    db.add(contract)
    db.commit()
    db.refresh(contract)

    out = ContractResponse.model_validate(contract).model_dump()
    out["sprint_id"] = contract.sprint_id
    return {"contract": out}


@router.patch("/contratos/{contract_id}")
def update_contrato(
    contract_id: int,
    payload: ContractRequest,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    contract = db.scalar(select(Contract).where(Contract.id == int(contract_id)))
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")

    if int(payload.building_id) != int(contract.building_id):
        building = db.scalar(select(Building).where(Building.id == int(payload.building_id)))
        if not building:
            raise HTTPException(status_code=400, detail="Obra inválida")
        contract.building_id = int(payload.building_id)

    contract.cost_center_code = payload.cost_center_code
    contract.stage_label = payload.stage_label
    contract.enterprise_label = payload.enterprise_label
    contract.contract_number = payload.contract_number
    contract.title = payload.title
    contract.kind = _normalize_kind(payload.kind)

    contract.owner_legal_name = payload.owner_legal_name
    contract.owner_cnpj = payload.owner_cnpj
    contract.owner_address = payload.owner_address
    contract.owner_representatives = payload.owner_representatives

    contract.supplier_legal_name = payload.supplier_legal_name
    contract.supplier_cnpj = payload.supplier_cnpj
    contract.supplier_address = payload.supplier_address
    contract.supplier_representatives = payload.supplier_representatives

    contract.object_text = payload.object_text
    contract.start_date = payload.start_date
    contract.end_date = payload.end_date

    contract.total_value = payload.total_value
    contract.retention_percent = payload.retention_percent
    contract.guarantee_text = payload.guarantee_text
    contract.insurance_text = payload.insurance_text
    contract.status = (payload.status or contract.status).strip() or contract.status
    contract.notes = payload.notes

    db.commit()
    db.refresh(contract)
    return {"contract": ContractResponse.model_validate(contract).model_dump()}


@router.delete("/contratos/{contract_id}")
def delete_contrato(
    contract_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    contract = db.scalar(select(Contract).options(selectinload(Contract.documents)).where(Contract.id == int(contract_id)))
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")

    # remove docs do disco
    for doc in list(contract.documents or []):
        try:
            p = Path(doc.file_path)
            if p.exists():
                p.unlink(missing_ok=True)
        except Exception:
            pass
        db.delete(doc)

    db.delete(contract)
    db.commit()
    return {"ok": True}


@router.get("/contratos/{contract_id}/documents")
def list_documents(
    contract_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    contract = db.scalar(select(Contract).where(Contract.id == int(contract_id)))
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")

    docs = db.scalars(
        select(ContractDocument)
        .where(ContractDocument.contract_id == int(contract_id))
        .order_by(ContractDocument.created_at.desc())
    ).all()

    return {"total": len(docs), "documents": [ContractDocumentResponse.model_validate(d).model_dump() for d in docs]}


@router.post("/contratos/{contract_id}/upload")
def upload_document(
    contract_id: int,
    doc_type: str = Query(..., min_length=1),
    title: str | None = Query(default=None),
    file: UploadFile = File(...),
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    contract = db.scalar(select(Contract).where(Contract.id == int(contract_id)))
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")

    fname = _safe_filename(file.filename or "documento")
    contract_dir = _UPLOAD_DIR / str(contract.id)
    contract_dir.mkdir(parents=True, exist_ok=True)

    # versionamento por tipo+nome
    existing_versions = db.scalars(
        select(ContractDocument.version)
        .where(ContractDocument.contract_id == contract.id)
        .where(ContractDocument.doc_type == doc_type)
        .where(ContractDocument.filename == fname)
    ).all()
    version = (max(existing_versions) + 1) if existing_versions else 1

    target_path = contract_dir / f"v{version:03d}_{fname}"

    raw = file.file.read()
    with target_path.open("wb") as f:
        f.write(raw)

    doc = ContractDocument(
        contract_id=contract.id,
        doc_type=doc_type,
        title=title,
        filename=fname,
        file_path=str(target_path),
        file_size=len(raw) if raw is not None else None,
        mime_type=file.content_type,
        uploaded_by=current_user.email,
        version=version,
    )
    db.add(doc)
    db.commit()
    db.refresh(doc)

    return {"document": ContractDocumentResponse.model_validate(doc).model_dump()}


@router.delete("/contratos/{contract_id}/documents/{doc_id}")
def delete_document(
    contract_id: int,
    doc_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    doc = db.scalar(
        select(ContractDocument)
        .where(ContractDocument.id == int(doc_id))
        .where(ContractDocument.contract_id == int(contract_id))
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Documento não encontrado")

    try:
        p = Path(doc.file_path)
        if p.exists():
            p.unlink(missing_ok=True)
    except Exception:
        pass

    db.delete(doc)
    db.commit()
    return {"ok": True}


@router.get("/contratos/{contract_id}/print")
def print_contract(
    contract_id: int,
    current_user: AppUser = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    contract = db.scalar(select(Contract).where(Contract.id == int(contract_id)))
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")

    html = _render_contract_html(contract)
    return {"html": html}
