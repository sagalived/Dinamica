#!/usr/bin/env python3
"""
Script para fazer sync completo com SIENGE
Baixa TODOS os dados e salva no banco local (ignora cache existente)
"""

import asyncio
import os
import sys
from pathlib import Path

# Adicionar backend à path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.database import SessionLocal
from backend.services.sienge_client import sienge_client
from backend.routers.sienge import (
    write_sync_metadata, read_sync_metadata, upsert_catalog_from_sienge,
    _write_cached_dataset, utc_now_iso, _read_cached_dataset
)

async def full_sync():
    """Faz sync completo: baixa TUDO do SIENGE sem usar cache"""
    
    print("[SYNC COMPLETO] Iniciando download completo do SIENGE...")
    print(f"[CONFIG] SIENGE conectado: {sienge_client.is_configured}")
    
    if not sienge_client.is_configured:
        print("[ERRO] SIENGE não configurado!")
        return False
    
    db = SessionLocal()
    started_at = utc_now_iso()
    
    try:
        # 1. CATÁLOGOS - Sempre baixa do SIENGE (não usa cache)
        print("\n[PASSO 1] Baixando CATÁLOGOS...")
        
        print("  - Obras...")
        obras = await sienge_client.fetch_obras()
        if obras:
            _write_cached_dataset(db, "obras.json", obras)
            print(f"    ✓ {len(obras)} obras")
        
        print("  - Usuários...")
        usuarios = await sienge_client.fetch_users()
        if usuarios:
            _write_cached_dataset(db, "usuarios.json", usuarios)
            print(f"    ✓ {len(usuarios)} usuários")
        
        print("  - Empresas...")
        empresas = await sienge_client.fetch_empresas()
        if empresas:
            _write_cached_dataset(db, "empresas.json", empresas)
            print(f"    ✓ {len(empresas)} empresas")
        
        print("  - Credores...")
        credores = await sienge_client.fetch_credores()
        if credores:
            _write_cached_dataset(db, "credores.json", credores)
            print(f"    ✓ {len(credores)} credores")
        
        # Atualizar catálogos no banco
        if any([obras, usuarios, empresas, credores]):
            upsert_catalog_from_sienge(
                db,
                obras=obras if obras else None,
                usuarios=usuarios if usuarios else None,
                empresas=empresas if empresas else None,
                credores=credores if credores else None,
            )
            print("  ✓ Catálogos atualizados no banco")
        
        # 2. DADOS TRANSACIONAIS - Sempre baixa do SIENGE
        print("\n[PASSO 2] Baixando DADOS TRANSACIONAIS (sem limites de data)...")
        
        print("  - Pedidos...")
        pedidos = await sienge_client.fetch_pedidos()
        if pedidos:
            _write_cached_dataset(db, "pedidos.json", pedidos)
            print(f"    ✓ {len(pedidos)} pedidos")
        
        print("  - Financeiro...")
        financeiro = await sienge_client.fetch_financeiro()
        if financeiro:
            _write_cached_dataset(db, "financeiro.json", financeiro)
            print(f"    ✓ {len(financeiro)} financeiro")
        
        print("  - Receber...")
        receber = await sienge_client.fetch_receber()
        if receber:
            _write_cached_dataset(db, "receber.json", receber)
            print(f"    ✓ {len(receber)} receber")
        
        # 3. ATUALIZAR METADADOS
        metadata = {
            "status": "success",
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "message": "Sync completo realizado: TODOS os dados baixados do SIENGE",
            "counts": {
                "obras": len(obras) if obras else 0,
                "usuarios": len(usuarios) if usuarios else 0,
                "empresas": len(empresas) if empresas else 0,
                "credores": len(credores) if credores else 0,
                "pedidos": len(pedidos) if pedidos else 0,
                "financeiro": len(financeiro) if financeiro else 0,
                "receber": len(receber) if receber else 0,
            },
            "source": "sienge_full",
        }
        write_sync_metadata(db, metadata)
        
        print("\n[SUCESSO] Sync completo concluído!")
        print(f"  - Obras: {len(obras) if obras else 0}")
        print(f"  - Usuarios: {len(usuarios) if usuarios else 0}")
        print(f"  - Empresas: {len(empresas) if empresas else 0}")
        print(f"  - Credores: {len(credores) if credores else 0}")
        print(f"  - Pedidos: {len(pedidos) if pedidos else 0}")
        print(f"  - Financeiro: {len(financeiro) if financeiro else 0}")
        print(f"  - Receber: {len(receber) if receber else 0}")
        
        return True
        
    except Exception as e:
        print(f"\n[ERRO] Sync completo falhou: {e}")
        import traceback
        traceback.print_exc()
        
        metadata = {
            "status": "error",
            "started_at": started_at,
            "finished_at": utc_now_iso(),
            "message": f"Sync completo falhou: {str(e)}",
            "source": "sienge_full",
        }
        write_sync_metadata(db, metadata)
        
        return False
    finally:
        db.close()

if __name__ == '__main__':
    success = asyncio.run(full_sync())
    sys.exit(0 if success else 1)
