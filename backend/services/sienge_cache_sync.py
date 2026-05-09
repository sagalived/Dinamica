"""
Sincronização e cache inteligente com SIENGE.

Estratégia:
1. Carrega dados do snapshot (banco) em primeiro lugar
2. Se faltar dados no range, busca do SIENGE e repopula
3. Salva também em arquivo local (data/) para próximo carregamento ser instantâneo
4. Sincronização automática busca apenas últimas horas do SIENGE
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from backend.config import DATA_DIR
from backend.services.sienge_storage import read_snapshot, write_snapshot, read_sync_metadata
from backend.services.sienge_client import sienge_client
from backend.services.sienge_local_files import list_pending_payload_records


RECEBER_SNAPSHOT_KEY = "receber.json"


def _get_local_file_path(key: str) -> Path:
    """Retorna caminho do arquivo local de cache."""
    return DATA_DIR / f"{key}.json"


def _read_local_cache(key: str) -> Any:
    """Lê dados do arquivo local em data/."""
    file_path = _get_local_file_path(key)
    if not file_path.exists():
        return None
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_local_cache(key: str, payload: Any) -> None:
    """Salva dados no arquivo local em data/."""
    file_path = _get_local_file_path(key)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache local {key}: {e}")


def _normalize_sienge_date(value: str | None) -> str | None:
    """Normaliza datas para formato esperado pelo SIENGE: YYYY-MM-DD."""
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    # Ja esta no formato YYYY-MM-DD(THH:mm:ss...)
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]

    # Formato BR DD/MM/YYYY
    if len(raw) >= 10 and raw[2] == "/" and raw[5] == "/":
        dd = raw[0:2]
        mm = raw[3:5]
        yyyy = raw[6:10]
        if yyyy.isdigit() and mm.isdigit() and dd.isdigit():
            return f"{yyyy}-{mm}-{dd}"

    # Fallback ISO parser
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return raw


def _merge_receivable_titles(local: list[dict], sienge: list[dict]) -> list[dict]:
    """
    Merge inteligente de títulos a receber.
    
    Prioriza dados SIENGE mais recentes (por ID), mantém dados locais antigos.
    """
    if not sienge:
        return local if isinstance(local, list) else []
    
    local = local if isinstance(local, list) else []
    
    # Index por ID para evitar duplicatas
    merged_dict = {}
    
    # Primeiro adiciona dados locais
    for item in local:
        item_id = item.get("id") or item.get("numero")
        if item_id:
            merged_dict[str(item_id)] = item
    
    # Depois sobrescreve com dados SIENGE (mais recentes)
    for item in sienge:
        item_id = item.get("id") or item.get("numero")
        if item_id:
            merged_dict[str(item_id)] = item
    
    return list(merged_dict.values())


async def load_receivable_titles_with_fallback(
    db: Session,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    """
    Carrega títulos a receber com fallback automático.
    
    Fluxo:
    1. Tenta carregar do snapshot (banco)
    2. Se arquivo local existe, usa como cache
    3. Se faltar intervalo de datas, busca do SIENGE
    4. Salva resultado em banco + arquivo local
    """
    
    # Tenta carregar do snapshot (banco)
    snapshot_data = read_snapshot(db, RECEBER_SNAPSHOT_KEY, default=None)
    if isinstance(snapshot_data, list) and snapshot_data:
        # Snapshot existe, usa como base
        all_titles = snapshot_data
    else:
        # Tenta carregar do arquivo local
        local_data = _read_local_cache("receber")
        all_titles = local_data if isinstance(local_data, list) else []
    
    # Se não temos dados ou precisa de intervalo específico, busca do SIENGE
    if not all_titles or (start_date and end_date):
        try:
            normalized_start = _normalize_sienge_date(start_date)
            normalized_end = _normalize_sienge_date(end_date)
            if normalized_start and normalized_end:
                sienge_titles = await sienge_client.fetch_receber_range(normalized_start, normalized_end)
            else:
                sienge_titles = await sienge_client.fetch_receber()
            
            # Merge com dados existentes
            if isinstance(sienge_titles, list):
                all_titles = _merge_receivable_titles(all_titles, sienge_titles)
                
                # Salva em banco e arquivo local
                write_snapshot(db, RECEBER_SNAPSHOT_KEY, all_titles)
                _write_local_cache("receber", all_titles)
        except Exception as e:
            print(f"Erro ao buscar títulos do SIENGE: {e}")
            # Continua com dados locais mesmo se falhar
    
    return all_titles if isinstance(all_titles, list) else []


async def sync_and_repopulate_cache(
    db: Session,
    start_date: str | None = None,
    end_date: str | None = None,
    company_id: str | None = None,
    building_id: str | None = None,
) -> dict[str, Any]:
    """
    Sincroniza com SIENGE e repopula arquivo local.
    
    Esta é a função chamada na sincronização automática e manual.
    Busca apenas últimas horas do SIENGE, faz UPSERT no banco e salva em arquivo.
    """
    
    # Se nenhuma data foi passada, usa últimas 48 horas
    if not start_date or not end_date:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=2)
        start_date = start.isoformat()
        end_date = end.isoformat()

    normalized_start = _normalize_sienge_date(start_date)
    normalized_end = _normalize_sienge_date(end_date)
    
    synced_count = 0
    updated_count = 0
    errors = []
    sync_started_at = datetime.now(timezone.utc)
    
    try:
        # Busca do SIENGE
        if not normalized_start or not normalized_end:
            return {
                "status": "error",
                "synced_count": 0,
                "updated_count": 0,
                "total_cached": 0,
                "errors": ["Datas invalidas para sincronizacao SIENGE"],
                "message": "✗ Erro ao sincronizar: datas invalidas para SIENGE",
            }

        sienge_titles = await sienge_client.fetch_receber_range(normalized_start, normalized_end)
        
        # Aplicar filtros de company_id e building_id se fornecidos
        filtered_titles = sienge_titles
        if isinstance(sienge_titles, list):
            filtered_titles = [
                t for t in sienge_titles
                if (
                    (not company_id or str(t.get("companyId")) == company_id)
                    and (not building_id or str(t.get("idObra")) == building_id)
                )
            ]
        
        # Carrega dados locais atuais
        current_data = read_snapshot(db, RECEBER_SNAPSHOT_KEY, default=[])
        if not isinstance(current_data, list):
            current_data = []
        
        # Merge com prioridade para novos dados SIENGE
        merged = _merge_receivable_titles(current_data, filtered_titles)
        
        # Conta novos e atualizados
        synced_count = len(filtered_titles)
        updated_count = len([t for t in merged if t in filtered_titles])
        
        # Salva em banco
        write_snapshot(db, RECEBER_SNAPSHOT_KEY, merged)
        
        # Salva em arquivo local para cache rápido
        _write_local_cache("receber", merged)

        updated_files = list_pending_payload_records("receber", created_after=sync_started_at)

        latest_sync = {
            "status": "success",
            "started_at": sync_started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "message": f"Sincronizado {synced_count} títulos ({updated_count} atualizados). Total em cache: {len(merged)}",
            "counts": {
                "synced_count": synced_count,
                "updated_count": updated_count,
                "total_cached": len(merged),
            },
            "updated_files": updated_files,
            "updated_files_count": len(updated_files),
            "source": "sienge_receber",
        }
        write_snapshot(db, "sienge_sync_meta", latest_sync)
        
        return {
            "status": "ok",
            "synced_count": synced_count,
            "updated_count": updated_count,
            "total_cached": len(merged),
            "updated_files": updated_files,
            "updated_files_count": len(updated_files),
            "latest_sync": latest_sync,
            "errors": errors,
            "message": f"✓ Sincronizado {synced_count} títulos ({updated_count} atualizados). Total em cache: {len(merged)}",
        }
    
    except Exception as e:
        error_msg = str(e)
        errors.append(error_msg)
        return {
            "status": "error",
            "synced_count": 0,
            "updated_count": 0,
            "total_cached": 0,
            "errors": errors,
            "message": f"✗ Erro ao sincronizar: {error_msg}",
        }


def get_cache_stats(db: Session) -> dict[str, Any]:
    """Retorna estatísticas de cache."""
    receber_data = read_snapshot(db, RECEBER_SNAPSHOT_KEY, default=[])
    receber_count = len(receber_data) if isinstance(receber_data, list) else 0
    
    financeiro_data = read_snapshot(db, "financeiro", default=[])
    financeiro_count = len(financeiro_data) if isinstance(financeiro_data, list) else 0
    
    pedidos_data = read_snapshot(db, "pedidos", default=[])
    pedidos_count = len(pedidos_data) if isinstance(pedidos_data, list) else 0
    
    # Verifica últimas alterações nos arquivos
    receber_file = _get_local_file_path("receber")
    receber_mtime = receber_file.stat().st_mtime if receber_file.exists() else None
    latest_sync = read_sync_metadata(db) or {}
    
    return {
        "receber": {
            "cached_records": receber_count,
            "local_file_exists": receber_file.exists(),
            "last_updated": datetime.fromtimestamp(receber_mtime).isoformat() if receber_mtime else None,
        },
        "financeiro": {
            "cached_records": financeiro_count,
        },
        "pedidos": {
            "cached_records": pedidos_count,
        },
        "total_records": receber_count + financeiro_count + pedidos_count,
        "latest_sync": latest_sync,
    }
