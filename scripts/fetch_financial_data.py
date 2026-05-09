"""
Script para popular a tabela SiengeFinancialReport com dados financeiros reais do Sienge
Usa dados agregados já presentes no banco de dados
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from backend.models import (
    SiengeFinancialReport, Building, Company,
    OperationalMonthlyAggregate, SiengeNfeDocument
)

# Database
db_url = os.environ.get("DATABASE_URL", "sqlite:///./dinamica_local.db")
engine = create_engine(db_url, echo=False)
SessionLocal = sessionmaker(bind=engine)


def populate_from_aggregates():
    """
    Popula SiengeFinancialReport com dados de OperationalMonthlyAggregate
    Usa dados agregados que já estão no banco de dados
    """
    
    print("\n" + "="*70)
    print("POPULANDO DADOS FINANCEIROS A PARTIR DE AGREGADOS")
    print("="*70)
    
    db = SessionLocal()
    try:
        # Buscar todos os relatórios
        reports = db.query(SiengeFinancialReport).all()
        print(f"\n📝 Processando {len(reports)} registros...")
        
        updated_count = 0
        
        for report in reports:
            try:
                building_id = report.building_id
                
                # Buscar agregados para esta obra
                aggregates = db.query(OperationalMonthlyAggregate).filter(
                    OperationalMonthlyAggregate.building_id == building_id
                ).all()
                
                if aggregates:
                    # Somar custos e receitas de todos os meses
                    total_custo = sum(float(agg.custo or 0) for agg in aggregates)
                    total_receita = sum(float(agg.receita or 0) for agg in aggregates)
                    
                    # Atualizar registro
                    report.custo_direto = total_custo
                    report.custo_indireto = 0.0  # Será calculado do rateio de bills
                    report.custo_total = total_custo
                    report.receita = total_receita
                    
                    # Calcular margem
                    report.margem = total_receita - total_custo
                    if total_receita > 0:
                        report.margem_percentual = (report.margem / total_receita) * 100
                    else:
                        report.margem_percentual = 0.0
                    
                    report.data_atualizacao = datetime.now().strftime('%Y-%m-%d')
                    updated_count += 1
                    
                    if updated_count % 50 == 0:
                        print(f"  ✓ Processados {updated_count} registros...")
                
            except Exception as e:
                print(f"  ✗ Erro ao processar {report.building_id}: {str(e)}")
                continue
        
        # Commit
        db.commit()
        print(f"\n✓ {updated_count} registros atualizados com sucesso")
        
        # Estatísticas
        reports_updated = db.query(SiengeFinancialReport).all()
        total_custo = sum(float(r.custo_total or 0) for r in reports_updated)
        total_receita = sum(float(r.receita or 0) for r in reports_updated)
        total_margem = sum(float(r.margem or 0) for r in reports_updated)
        
        print("\n" + "="*70)
        print("📋 RESUMO FINANCEIRO TOTAL")
        print("="*70)
        print(f"💰 Custo Total:        R$ {total_custo:,.2f}")
        print(f"📈 Receita Total:      R$ {total_receita:,.2f}")
        print(f"📊 Margem Total:       R$ {total_margem:,.2f}")
        if total_receita > 0:
            margem_pct = (total_margem / total_receita) * 100
            print(f"📌 Margem %:           {margem_pct:.2f}%")
        else:
            print(f"📌 Margem %:           0.00%")
        
    except Exception as e:
        print(f"✗ Erro ao popular registros: {str(e)}")
        db.rollback()
    finally:
        db.close()


def main():
    print("\n🚀 INICIANDO PREENCHIMENTO DE DADOS FINANCEIROS")
    
    populate_from_aggregates()
    
    print("\n" + "="*70)
    print("✅ PREENCHIMENTO CONCLUÍDO")
    print("="*70)
    print("\n💡 Acesse http://localhost:5173/financeiro/centro-custo para ver os dados")


if __name__ == "__main__":
    main()
