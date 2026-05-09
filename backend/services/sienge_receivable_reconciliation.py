"""
Serviço para reconciliação de títulos a receber com SIENGE.
Sincroniza dados do SIENGE com banco local, fazendo UPSERT conforme diferenças.
"""
from datetime import datetime
from typing import Any, Dict, List
from sqlalchemy.orm import Session
import logging

from backend.services.sienge_client import sienge_client
from backend.services.sienge_storage import read_snapshot, write_snapshot
from backend.models import Building, Company

logger = logging.getLogger(__name__)


async def sync_receivable_titles_from_sienge(
    db: Session,
    start_date: str | None = None,
    end_date: str | None = None,
    company_id: str | None = None,
    building_id: str | None = None,
) -> Dict[str, Any]:
    """
    Sincroniza títulos a receber do SIENGE com banco local.
    
    Args:
        db: Session do banco
        start_date: ISO date string (2026-01-01)
        end_date: ISO date string (2026-12-31)
        company_id: ID da empresa para filtro (str)
        building_id: ID da obra para filtro (str)
    
    Returns:
        Dict com { synced_count: int, updated_count: int, errors: List[str] }
    """
    result = {
        "synced_count": 0,
        "updated_count": 0,
        "errors": [],
        "message": "",
    }

    try:
        # 1. Busca dados SIENGE de títulos a receber
        logger.info(f"Sincronizando títulos a receber. Period: {start_date} to {end_date}")
        
        sienge_titles = await sienge_client.fetch_receivable_titles(
            start_date=start_date,
            end_date=end_date,
            company_id=company_id,
            building_id=building_id,
        )
        
        if not sienge_titles:
            result["message"] = "Nenhum título a receber encontrado no SIENGE"
            logger.warning(result["message"])
            return result

        # 2. Carrega snapshot local atual
        local_snapshot = read_snapshot(db, "receber", default={})
        if not isinstance(local_snapshot, dict):
            local_snapshot = {}

        local_titles_by_id = {str(t.get("id")): t for t in local_snapshot.get("rows", [])}
        
        # 3. Processa títulos do SIENGE
        titles_to_upsert = []
        synced = 0
        updated = 0

        for sienge_title in sienge_titles:
            title_id = str(sienge_title.get("id", ""))
            if not title_id:
                continue

            # Normaliza dados do SIENGE
            normalized_title = {
                "id": sienge_title.get("id"),
                "numero": sienge_title.get("numero"),
                "numeroTitulo": sienge_title.get("numeroTitulo"),
                "codigoTitulo": sienge_title.get("codigoTitulo"),
                "idObra": sienge_title.get("idObra"),
                "codigoObra": sienge_title.get("codigoObra"),
                "nomeObra": sienge_title.get("nomeObra"),
                "descricao": sienge_title.get("descricao"),
                "nomeCliente": sienge_title.get("nomeCliente"),
                "dataVencimento": sienge_title.get("dataVencimento"),
                "valor": sienge_title.get("valor"),
                "situacao": sienge_title.get("situacao"),
                "dataPagamento": sienge_title.get("dataPagamento"),
                "companyId": sienge_title.get("companyId"),
                "rawValue": sienge_title.get("rawValue", sienge_title.get("valor")),
                "bankAccountCode": sienge_title.get("bankAccountCode"),
                "documentNumber": sienge_title.get("documentNumber"),
                "billId": sienge_title.get("billId"),
                "synced_at": datetime.utcnow().isoformat(),
            }

            # Verifica se já existe local
            if title_id in local_titles_by_id:
                # Compara valores para detectar mudanças
                local_val = float(local_titles_by_id[title_id].get("valor", 0))
                sienge_val = float(normalized_title.get("valor", 0))
                
                if abs(local_val - sienge_val) > 0.01:
                    updated += 1
                    logger.debug(f"Título {title_id}: valor alterado de R$ {local_val} para R$ {sienge_val}")
            else:
                synced += 1
                logger.debug(f"Título {title_id}: novo registro")

            titles_to_upsert.append(normalized_title)

        # 4. Atualiza snapshot local
        if titles_to_upsert:
            local_snapshot["rows"] = local_snapshot.get("rows", [])
            
            # Remove títulos que já existem (para fazer merge)
            existing_ids = {str(t.get("id")) for t in titles_to_upsert}
            local_snapshot["rows"] = [
                t for t in local_snapshot["rows"]
                if str(t.get("id")) not in existing_ids
            ]
            
            # Adiciona/atualiza títulos sincronizados
            local_snapshot["rows"].extend(titles_to_upsert)
            local_snapshot["synced_at"] = datetime.utcnow().isoformat()
            local_snapshot["total_count"] = len(local_snapshot["rows"])
            
            write_snapshot(db, "receber", local_snapshot)
            
            logger.info(
                f"Sincronização concluída: {synced} novos, {updated} atualizados, "
                f"total: {len(local_snapshot['rows'])} registros"
            )

        result["synced_count"] = synced
        result["updated_count"] = updated
        result["message"] = f"✓ {synced} novos registros, {updated} atualizados"

    except Exception as e:
        error_msg = f"Erro ao sincronizar títulos a receber: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result["errors"].append(error_msg)
        result["message"] = error_msg

    return result


async def validate_receivable_titles(
    db: Session,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict[str, Any]:
    """
    Valida se valores de títulos a receber batem entre banco local e SIENGE.
    
    Returns:
        Dict com { matches: bool, local_total: float, sienge_total: float, diffs: List }
    """
    result = {
        "matches": False,
        "local_total": 0.0,
        "sienge_total": 0.0,
        "diffs": [],
        "message": "",
    }

    try:
        # Busca dados SIENGE
        sienge_titles = await sienge_client.fetch_receivable_titles(
            start_date=start_date,
            end_date=end_date,
        )

        sienge_by_id = {str(t.get("id")): t for t in sienge_titles}
        sienge_total = sum(float(t.get("valor", 0)) for t in sienge_titles)

        # Busca dados locais
        local_snapshot = read_snapshot(db, "receber", default={})
        if isinstance(local_snapshot, dict):
            local_titles = local_snapshot.get("rows", [])
        else:
            local_titles = []

        local_by_id = {str(t.get("id")): t for t in local_titles}
        local_total = sum(float(t.get("valor", 0)) for t in local_titles)

        result["local_total"] = local_total
        result["sienge_total"] = sienge_total

        # Detecta diferenças
        all_ids = set(sienge_by_id.keys()) | set(local_by_id.keys())
        diffs = []

        for title_id in all_ids:
            local_title = local_by_id.get(title_id)
            sienge_title = sienge_by_id.get(title_id)

            if not local_title:
                diffs.append({
                    "id": title_id,
                    "type": "missing_local",
                    "local_value": None,
                    "sienge_value": float(sienge_title.get("valor", 0)),
                })
            elif not sienge_title:
                diffs.append({
                    "id": title_id,
                    "type": "extra_local",
                    "local_value": float(local_title.get("valor", 0)),
                    "sienge_value": None,
                })
            else:
                local_val = float(local_title.get("valor", 0))
                sienge_val = float(sienge_title.get("valor", 0))
                if abs(local_val - sienge_val) > 0.01:
                    diffs.append({
                        "id": title_id,
                        "type": "value_mismatch",
                        "local_value": local_val,
                        "sienge_value": sienge_val,
                        "diff": sienge_val - local_val,
                    })

        result["matches"] = len(diffs) == 0 and abs(local_total - sienge_total) < 0.01
        result["diffs"] = diffs
        
        if result["matches"]:
            result["message"] = f"✓ Valores batem! Total: R$ {local_total:,.2f}"
        else:
            result["message"] = f"⚠ {len(diffs)} diferenças detectadas"

    except Exception as e:
        result["message"] = f"Erro ao validar: {str(e)}"
        logger.error(result["message"], exc_info=True)

    return result
