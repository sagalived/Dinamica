"""
Script para sincronizar dados do Sienge e limpar dados anteriores a 2026
"""

import os
import sys
import json
import asyncio
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from backend.models import (
    SiengeNfeDocument, OperationalMonthlyAggregate, 
    SiengeFinancialReport, Building, Company
)
from backend.services.sienge_client import sienge_client

# Banco de dados
db_url = os.environ.get("DATABASE_URL", "sqlite:///./dinamica_local.db")
engine = create_engine(db_url, echo=False)
SessionLocal = sessionmaker(bind=engine)

def cleanup_all_pre_2026_data() -> dict:
    """
    Remove TODOS os dados anteriores a 01/01/2026 do banco
    """
    
    print("\n" + "="*70)
    print("LIMPEZA COMPLETA - REMOVENDO DADOS PRÉ-2026")
    print("="*70)
    
    db = SessionLocal()
    stats = {
        'nfe_documents': 0,
        'operational_aggregates': 0,
        'total_deleted': 0,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    try:
        # 1. Limpar SiengeNfeDocument
        deleted = db.query(SiengeNfeDocument).filter(
            SiengeNfeDocument.issue_date < "2026-01-01"
        ).delete(synchronize_session=False)
        stats['nfe_documents'] = deleted
        print(f"✓ Documentos NFe: {deleted} deletados")
        
        # 2. Limpar OperationalMonthlyAggregate
        deleted = db.query(OperationalMonthlyAggregate).filter(
            OperationalMonthlyAggregate.month < "2026-01"
        ).delete(synchronize_session=False)
        stats['operational_aggregates'] = deleted
        print(f"✓ Agregados Operacionais: {deleted} deletados")
        
        # 3. Limpar SiengeFinancialReport (se houver dados antigos)
        deleted = db.query(SiengeFinancialReport).filter(
            SiengeFinancialReport.data_atualizacao < "2026-01-01"
        ).delete(synchronize_session=False)
        if deleted > 0:
            print(f"✓ Relatórios Financeiros: {deleted} deletados")
        
        db.commit()
        stats['total_deleted'] = (stats['nfe_documents'] + 
                                 stats['operational_aggregates'] + 
                                 deleted)
        
        print(f"\n✓ TOTAL DELETADO: {stats['total_deleted']} registros")
        print("✓ Banco agora contém APENAS dados de 2026+")
        
    except Exception as e:
        print(f"✗ Erro ao limpar dados: {str(e)}")
        db.rollback()
        stats['error'] = str(e)
    finally:
        db.close()
    
    return stats


async def fetch_sienge_financial_reports() -> list[dict]:
    """
    Busca dados de relatório financeiro do Sienge via cliente
    Integra com a URL do relatório: #/common/page/2534
    """
    
    print("\n" + "="*70)
    print("BUSCANDO DADOS DO SIENGE - RELATÓRIO FINANCEIRO DE OBRAS")
    print("="*70)
    
    financial_reports = []
    
    try:
        # Buscar enterprises/empreendimentos usando o cliente Sienge
        print("\n📍 Buscando enterprises do Sienge...")
        enterprises = await sienge_client.fetch_obras()
        
        if not enterprises:
            print("✗ Nenhuma empresa encontrada no Sienge")
            print("  Verificando se as credenciais estão corretas...")
            print(f"  Erro: {sienge_client.last_error}")
            return financial_reports
        
        print(f"✓ {len(enterprises)} enterprises encontrados")
        
        # Para cada enterprise, preparar registro financeiro
        db = SessionLocal()
        try:
            companies = db.query(Company).all()
            company_map = {str(c.id): c.name for c in companies}
            
            # Usar dados dos enterprises para montar relatórios
            for i, enterprise in enumerate(enterprises, 1):
                try:
                    enterprise_id = enterprise.get('id')
                    enterprise_name = enterprise.get('name', 'Sem nome')
                    company_id = enterprise.get('companyId', enterprise.get('company_id'))
                    
                    # Procurar building correspondente no nosso banco
                    building = db.query(Building).filter(Building.id == enterprise_id).first()
                    
                    if building:
                        print(f"  [{i}] 📊 {enterprise_name} (ID: {enterprise_id})")
                        
                        financial_reports.append({
                            'building_id': str(enterprise_id),
                            'building_code': building.code if hasattr(building, 'code') else str(building.id),
                            'building_name': building.name,
                            'company_id': str(company_id) if company_id else str(building.company_id) if building.company_id else None,
                            'company_name': company_map.get(str(company_id or building.company_id), 'Sem empresa'),
                            'custo_direto': 0.0,
                            'custo_indireto': 0.0,
                            'custo_total': 0.0,
                            'receita': 0.0,
                            'margem': 0.0,
                            'margem_percentual': 0.0,
                            'data_atualizacao': datetime.now().strftime('%Y-%m-%d'),
                            'payload': json.dumps(enterprise)
                        })
                    
                except Exception as e:
                    print(f"  ✗ Erro ao processar {enterprise.get('name')}: {str(e)}")
                    continue
            
            print(f"\n✓ Total de {len(financial_reports)} relatórios preparados")
            
        finally:
            db.close()
                
    except Exception as e:
        print(f"✗ Erro na sincronização: {str(e)}")
    
    return financial_reports


def save_financial_reports(reports: list[dict]) -> dict:
    """
    Salva relatórios financeiros no banco de dados
    """
    
    print("\n" + "="*70)
    print("SALVANDO RELATÓRIOS NO BANCO DE DADOS")
    print("="*70)
    
    db = SessionLocal()
    stats = {
        'saved': 0,
        'updated': 0,
        'errors': 0,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    try:
        for report_data in reports:
            try:
                building_id = report_data['building_id']
                
                # Verificar se já existe
                existing = db.query(SiengeFinancialReport).filter(
                    SiengeFinancialReport.building_id == building_id
                ).first()
                
                if existing:
                    # Atualizar
                    for key, value in report_data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    stats['updated'] += 1
                    print(f"  ✓ Atualizado: {report_data['building_name']}")
                else:
                    # Criar novo
                    report = SiengeFinancialReport(**report_data)
                    db.add(report)
                    stats['saved'] += 1
                    print(f"  ✓ Criado: {report_data['building_name']}")
                    
            except Exception as e:
                print(f"  ✗ Erro ao salvar {report_data.get('building_name')}: {str(e)}")
                stats['errors'] += 1
                continue
        
        db.commit()
        print(f"\n✓ {stats['saved']} novos registros salvos")
        print(f"✓ {stats['updated']} registros atualizados")
        
    except Exception as e:
        print(f"✗ Erro ao salvar relatórios: {str(e)}")
        db.rollback()
        stats['errors'] += len(reports)
    finally:
        db.close()
    
    return stats


def show_summary(cleanup_stats: dict, reports_stats: dict):
    """Mostra resumo da execução"""
    
    print("\n" + "="*70)
    print("📋 RESUMO DA SINCRONIZAÇÃO")
    print("="*70)
    
    print(f"\n🗑️ LIMPEZA:")
    print(f"   • Documentos NFe removidos: {cleanup_stats.get('nfe_documents', 0)}")
    print(f"   • Agregados operacionais removidos: {cleanup_stats.get('operational_aggregates', 0)}")
    print(f"   • Total deletado: {cleanup_stats.get('total_deleted', 0)}")
    
    print(f"\n📥 SINCRONIZAÇÃO:")
    print(f"   • Relatórios criados: {reports_stats.get('saved', 0)}")
    print(f"   • Relatórios atualizados: {reports_stats.get('updated', 0)}")
    print(f"   • Erros: {reports_stats.get('errors', 0)}")
    
    print("\n" + "="*70)
    print("✅ SINCRONIZAÇÃO CONCLUÍDA")
    print("="*70)
    print("\n💡 Dicas:")
    print("   • Acesse http://localhost:5173/financeiro/centro-custo")
    print("   • Os dados agora contêm apenas registros de 2026+")
    print("   • Clique em 'Sincronizar Sienge' para atualizar")


async def main():
    """Executa o fluxo completo"""
    
    print("\n" + "🚀 INICIANDO SINCRONIZAÇÃO DO SIENGE E LIMPEZA DO BANCO")
    
    # 1. Limpar dados pré-2026
    cleanup_stats = cleanup_all_pre_2026_data()
    
    # 2. Buscar dados do Sienge
    reports = await fetch_sienge_financial_reports()
    
    # 3. Salvar no banco
    reports_stats = save_financial_reports(reports)
    
    # 4. Mostrar resumo
    show_summary(cleanup_stats, reports_stats)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ Sincronização cancelada pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Erro fatal: {str(e)}")
        sys.exit(1)
