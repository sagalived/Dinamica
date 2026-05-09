"""
Script para popular SiengeFinancialReport com dados de teste realistas
Demonstra o funcionamento do módulo de relatórios financeiros
"""

import os
import sys
import random
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend.models import SiengeFinancialReport

# Database
db_url = os.environ.get("DATABASE_URL", "sqlite:///./dinamica_local.db")
engine = create_engine(db_url, echo=False)
SessionLocal = sessionmaker(bind=engine)


def populate_test_data():
    """Popula dados de teste nas 348 obras"""
    
    print("\n" + "="*70)
    print("POPULANDO DADOS DE TESTE NO CENTRO DE CUSTO")
    print("="*70)
    
    db = SessionLocal()
    try:
        # Buscar todos os registros
        reports = db.query(SiengeFinancialReport).all()
        print(f"\n📝 Gerando dados para {len(reports)} obras...")
        
        updated_count = 0
        
        for i, report in enumerate(reports, 1):
            try:
                # Gerar valores realistas baseado no ID da obra
                # Obras menores: R$ 50k a R$ 500k
                # Obras médias: R$ 500k a R$ 2M
                # Obras grandes: R$ 2M a R$ 10M
                
                building_id = int(report.building_id) if report.building_id.isdigit() else i
                
                # Distribuição: 60% pequenas, 30% médias, 10% grandes
                random_val = random.random()
                
                if random_val < 0.60:
                    # Obra pequena
                    custo_total = random.uniform(50_000, 500_000)
                elif random_val < 0.90:
                    # Obra média
                    custo_total = random.uniform(500_000, 2_000_000)
                else:
                    # Obra grande
                    custo_total = random.uniform(2_000_000, 10_000_000)
                
                # Margem varia de -10% a 40%
                margem_pct = random.uniform(-10, 40)
                
                # Calcular receita a partir do custo e margem
                if margem_pct > -100:
                    receita = custo_total / (1 - margem_pct/100)
                else:
                    receita = custo_total * 1.2
                
                # Distribuir custo entre direto e indireto
                custo_direto = custo_total * random.uniform(0.6, 0.9)
                custo_indireto = custo_total - custo_direto
                
                # Calcular margem real
                margem = receita - custo_total
                margem_percentual = (margem / receita * 100) if receita > 0 else 0
                
                # Atualizar registro
                report.custo_direto = round(custo_direto, 2)
                report.custo_indireto = round(custo_indireto, 2)
                report.custo_total = round(custo_total, 2)
                report.receita = round(receita, 2)
                report.margem = round(margem, 2)
                report.margem_percentual = round(margem_percentual, 2)
                report.data_atualizacao = datetime.now().strftime('%Y-%m-%d')
                
                updated_count += 1
                
                if updated_count % 50 == 0:
                    print(f"  ✓ Processadas {updated_count} obras...")
                
            except Exception as e:
                print(f"  ✗ Erro ao processar {report.building_id}: {str(e)}")
                continue
        
        # Commit
        db.commit()
        print(f"\n✓ {updated_count} registros preenchidos com sucesso")
        
        # Estatísticas
        reports_updated = db.query(SiengeFinancialReport).all()
        total_custo = sum(float(r.custo_total or 0) for r in reports_updated)
        total_receita = sum(float(r.receita or 0) for r in reports_updated)
        total_margem = sum(float(r.margem or 0) for r in reports_updated)
        
        print("\n" + "="*70)
        print("📊 RESUMO FINANCEIRO TOTAL (TODAS AS OBRAS)")
        print("="*70)
        print(f"💰 Custo Total:        R$ {total_custo:,.2f}")
        print(f"📈 Receita Total:      R$ {total_receita:,.2f}")
        print(f"📊 Margem Total:       R$ {total_margem:,.2f}")
        if total_receita > 0:
            margem_pct = (total_margem / total_receita) * 100
            print(f"📌 Margem %:           {margem_pct:.2f}%")
        
        # Top 5 maiores custos
        print("\n" + "="*70)
        print("🏆 TOP 5 MAIORES CUSTOS")
        print("="*70)
        top_custos = sorted(reports_updated, key=lambda r: float(r.custo_total or 0), reverse=True)[:5]
        for idx, report in enumerate(top_custos, 1):
            print(f"{idx}. {report.building_name}")
            print(f"   Custo: R$ {float(report.custo_total):,.2f}")
            print(f"   Receita: R$ {float(report.receita):,.2f}")
            print(f"   Margem: {report.margem_percentual:.2f}%\n")
        
        # Top 5 maiores margens
        print("="*70)
        print("💎 TOP 5 MAIORES MARGENS")
        print("="*70)
        top_margens = sorted(reports_updated, key=lambda r: float(r.margem_percentual or 0), reverse=True)[:5]
        for idx, report in enumerate(top_margens, 1):
            print(f"{idx}. {report.building_name}")
            print(f"   Margem: {report.margem_percentual:.2f}%")
            print(f"   Receita: R$ {float(report.receita):,.2f}\n")
        
    except Exception as e:
        print(f"✗ Erro ao popular registros: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()


def main():
    print("\n🚀 INICIANDO PREENCHIMENTO COM DADOS DE TESTE")
    
    populate_test_data()
    
    print("\n" + "="*70)
    print("✅ PREENCHIMENTO CONCLUÍDO")
    print("="*70)
    print("\n💡 Acesse http://localhost:5173/financeiro/centro-custo para ver os dados")
    print("   Os valores são dados de teste para demonstração\n")


if __name__ == "__main__":
    main()
