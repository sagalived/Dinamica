"""
Migração e limpeza de dados para o novo sistema de 2026
Remove todos os dados anteriores a 01/01/2026 de tabelas financeiras
"""

import os
import sqlite3
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Determinar o banco de dados
db_url = os.environ.get("DATABASE_URL", "sqlite:///./dinamica_local.db")
db_path = db_url.replace("sqlite:///", "").lstrip("./")

if not os.path.exists(db_path):
    print(f"Banco de dados não encontrado: {db_path}")
    exit(1)

print(f"Conectando ao banco: {db_path}")

# Criar engine
engine = create_engine(db_url, echo=False)
SessionLocal = sessionmaker(bind=engine)

def create_financial_reports_table():
    """Cria a tabela sienge_financial_reports se não existir"""
    with engine.begin() as conn:
        # Verificar se a tabela já existe
        result = conn.execute(text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='sienge_financial_reports'
        """))
        
        if result.fetchone():
            print("✓ Tabela sienge_financial_reports já existe")
            return
        
        # Criar tabela
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sienge_financial_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                building_id VARCHAR(40) NOT NULL UNIQUE,
                building_code VARCHAR(80),
                building_name VARCHAR(255),
                company_id VARCHAR(40),
                company_name VARCHAR(255),
                custo_direto FLOAT NOT NULL DEFAULT 0.0,
                custo_indireto FLOAT NOT NULL DEFAULT 0.0,
                custo_total FLOAT NOT NULL DEFAULT 0.0,
                receita FLOAT NOT NULL DEFAULT 0.0,
                margem FLOAT NOT NULL DEFAULT 0.0,
                margem_percentual FLOAT NOT NULL DEFAULT 0.0,
                data_atualizacao VARCHAR(10),
                payload TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_building_id (building_id),
                INDEX idx_company_id (company_id)
            )
        """))
        
        print("✓ Tabela sienge_financial_reports criada com sucesso")


def cleanup_old_data():
    """Remove dados anteriores a 01/01/2026"""
    
    cleanup_date = "2026-01-01"
    tables_to_cleanup = [
        {
            "table": "sienge_nfe_documents",
            "date_column": "issue_date",
            "name": "Documentos NFe"
        },
        {
            "table": "sienge_open_accounts",
            "date_column": "due_date",
            "name": "Contas a Pagar"
        },
        {
            "table": "sienge_received_accounts",
            "date_column": "due_date",
            "name": "Contas a Receber"
        },
        {
            "table": "operational_monthly_aggregates",
            "date_column": "month",
            "name": "Agregados Operacionais",
            "is_month": True
        },
    ]
    
    print("\n" + "="*60)
    print(f"LIMPEZA DE DADOS ANTERIORES A {cleanup_date}")
    print("="*60)
    
    with engine.begin() as conn:
        for table_config in tables_to_cleanup:
            table_name = table_config["table"]
            date_col = table_config["date_column"]
            friendly_name = table_config["name"]
            is_month = table_config.get("is_month", False)
            
            # Verificar se a tabela existe
            result = conn.execute(text(f"""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='{table_name}'
            """))
            
            if not result.fetchone():
                print(f"⚠ Tabela {friendly_name} ({table_name}) não existe")
                continue
            
            # Contar registros a remover
            if is_month:
                # Para a coluna month (YYYY-MM)
                result = conn.execute(text(f"""
                    SELECT COUNT(*) as count 
                    FROM {table_name} 
                    WHERE {date_col} < '2026-01'
                """))
            else:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) as count 
                    FROM {table_name} 
                    WHERE {date_col} < '{cleanup_date}'
                """))
            
            count_result = result.fetchone()
            count_to_delete = count_result[0] if count_result else 0
            
            if count_to_delete > 0:
                # Deletar dados
                if is_month:
                    conn.execute(text(f"""
                        DELETE FROM {table_name} 
                        WHERE {date_col} < '2026-01'
                    """))
                else:
                    conn.execute(text(f"""
                        DELETE FROM {table_name} 
                        WHERE {date_col} < '{cleanup_date}'
                    """))
                
                print(f"✓ {friendly_name}: {count_to_delete} registros deletados")
            else:
                print(f"  {friendly_name}: nenhum registro para deletar")


def run_migrations():
    """Executa todas as migrações"""
    print("\n" + "="*60)
    print("CRIAÇÃO DE TABELAS")
    print("="*60)
    
    create_financial_reports_table()
    
    print("\n" + "="*60)
    print("LIMPEZA DE DADOS")
    print("="*60)
    
    cleanup_old_data()
    
    print("\n" + "="*60)
    print("✓ MIGRAÇÃO CONCLUÍDA COM SUCESSO")
    print("="*60)
    print("\nDetalhes:")
    print("  - Nova tabela criada: sienge_financial_reports")
    print("  - Dados anteriores a 2026-01-01 removidos")
    print("  - Sistema agora trabalha apenas com dados de 2026+")
    print("\nPróximos passos:")
    print("  1. Implementar sincronização com Sienge")
    print("  2. Popular dados do relatório financeiro de obras")
    print("  3. Testar na interface web")
    

if __name__ == "__main__":
    run_migrations()
