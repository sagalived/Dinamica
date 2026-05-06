import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import DATABASE_URL

def create_views():
    engine = create_engine(DATABASE_URL)
    
    sql_pedidos = """
    CREATE OR REPLACE VIEW vw_pedidos AS
    SELECT
        record_id,
        COALESCE(NULLIF(payload::jsonb->>'id', ''), NULLIF(payload::jsonb->>'numero', '0'))::integer AS id,
        COALESCE(NULLIF(payload::jsonb->>'idObra', ''), NULLIF(payload::jsonb->>'buildingId', ''))::integer AS building_id,
        NULLIF(payload::jsonb->>'companyId', '')::integer AS company_id,
        COALESCE(NULLIF(payload::jsonb->>'idComprador', ''), NULLIF(payload::jsonb->>'codigoComprador', ''), NULLIF(payload::jsonb->>'buyerId', '')) AS buyer_id,
        COALESCE(NULLIF(payload::jsonb->>'codigoFornecedor', ''), NULLIF(payload::jsonb->>'idCredor', ''), NULLIF(payload::jsonb->>'supplierId', ''))::integer AS supplier_id,
        COALESCE(NULLIF(payload::jsonb->>'data', ''), NULLIF(payload::jsonb->>'dataEmissao', ''), NULLIF(payload::jsonb->>'date', '')) AS data_emissao,
        COALESCE(NULLIF(payload::jsonb->>'totalAmount', ''), NULLIF(payload::jsonb->>'valorTotal', ''))::numeric AS total_amount,
        COALESCE(NULLIF(payload::jsonb->>'status', ''), NULLIF(payload::jsonb->>'situacao', '')) AS status,
        COALESCE(NULLIF(payload::jsonb->>'condicaoPagamento', ''), NULLIF(payload::jsonb->>'paymentMethod', '')) AS payment_condition,
        COALESCE(NULLIF(payload::jsonb->>'dataEntrega', ''), NULLIF(payload::jsonb->>'prazoEntrega', '')) AS delivery_date,
        COALESCE(NULLIF(payload::jsonb->>'internalNotes', ''), NULLIF(payload::jsonb->>'observacao', '')) AS internal_notes,
        payload::jsonb->>'nomeObra' AS nome_obra,
        payload::jsonb->>'nomeFornecedor' AS nome_fornecedor,
        COALESCE(NULLIF(payload::jsonb->>'nomeComprador', ''), NULLIF(payload::jsonb->>'buyerName', '')) AS nome_comprador,
        COALESCE(NULLIF(payload::jsonb->>'solicitante', ''), NULLIF(payload::jsonb->>'requesterId', ''), NULLIF(payload::jsonb->>'createdBy', '')) AS solicitante
    FROM sienge_raw_records
    WHERE dataset = 'purchase-orders';
    """

    sql_financeiro = """
    CREATE OR REPLACE VIEW vw_financeiro AS
    SELECT
        record_id,
        COALESCE(NULLIF(payload::jsonb->>'id', ''), NULLIF(payload::jsonb->>'numero', ''), NULLIF(payload::jsonb->>'codigoTitulo', ''), NULLIF(payload::jsonb->>'documentNumber', ''))::integer AS id,
        NULLIF(payload::jsonb->>'companyId', '')::integer AS company_id,
        COALESCE(NULLIF(payload::jsonb->>'creditorId', ''), NULLIF(payload::jsonb->>'idCredor', ''), NULLIF(payload::jsonb->>'codigoFornecedor', ''), NULLIF(payload::jsonb->>'debtorId', '')) AS creditor_id,
        COALESCE(NULLIF(payload::jsonb->>'idObra', ''), NULLIF(payload::jsonb->>'codigoObra', ''), NULLIF(payload::jsonb->>'enterpriseId', ''), NULLIF(payload::jsonb->>'buildingId', ''))::integer AS building_id,
        COALESCE(NULLIF(payload::jsonb->>'dataVencimento', ''), NULLIF(payload::jsonb->>'issueDate', ''), NULLIF(payload::jsonb->>'dueDate', ''), NULLIF(payload::jsonb->>'dataVencimentoProjetado', ''), NULLIF(payload::jsonb->>'dataEmissao', ''), NULLIF(payload::jsonb->>'dataContabil', '')) AS data_vencimento,
        COALESCE(NULLIF(payload::jsonb->>'descricao', ''), NULLIF(payload::jsonb->>'historico', ''), NULLIF(payload::jsonb->>'tipoDocumento', ''), NULLIF(payload::jsonb->>'notes', ''), NULLIF(payload::jsonb->>'observacao', '')) AS descricao,
        COALESCE(NULLIF(payload::jsonb->>'totalInvoiceAmount', ''), NULLIF(payload::jsonb->>'valor', ''), NULLIF(payload::jsonb->>'amount', ''), NULLIF(payload::jsonb->>'valorTotal', ''), NULLIF(payload::jsonb->>'valorLiquido', ''), NULLIF(payload::jsonb->>'valorBruto', ''))::numeric AS valor,
        COALESCE(NULLIF(payload::jsonb->>'situacao', ''), NULLIF(payload::jsonb->>'status', '')) AS situacao,
        COALESCE(NULLIF(payload::jsonb->>'nomeCredor', ''), NULLIF(payload::jsonb->>'creditorName', ''), NULLIF(payload::jsonb->>'nomeFantasiaCredor', ''), NULLIF(payload::jsonb->>'fornecedor', ''), NULLIF(payload::jsonb->>'credor', '')) AS creditor_name,
        payload::jsonb->>'nomeObra' AS nome_obra,
        COALESCE(NULLIF(payload::jsonb->>'documentNumber', ''), NULLIF(payload::jsonb->>'numeroDocumento', ''), NULLIF(payload::jsonb->>'numero', ''), NULLIF(payload::jsonb->>'codigoTitulo', '')) AS document_number
    FROM sienge_raw_records
    WHERE dataset = 'bills';
    """

    sql_receber = """
    CREATE OR REPLACE VIEW vw_receber AS
    SELECT
        record_id,
        COALESCE(NULLIF(payload::jsonb->>'id', ''), NULLIF(payload::jsonb->>'numero', ''), NULLIF(payload::jsonb->>'numeroTitulo', ''), NULLIF(payload::jsonb->>'codigoTitulo', ''), NULLIF(payload::jsonb->>'documentNumber', ''))::integer AS id,
        NULLIF(payload::jsonb->>'companyId', '')::integer AS company_id,
        COALESCE(NULLIF(payload::jsonb->>'clientId', ''), NULLIF(payload::jsonb->>'idCLiente', ''), NULLIF(payload::jsonb->>'codigoCliente', '')) AS client_id,
        COALESCE(NULLIF(payload::jsonb->>'idObra', ''), NULLIF(payload::jsonb->>'codigoObra', ''), NULLIF(payload::jsonb->>'enterpriseId', ''), NULLIF(payload::jsonb->>'buildingId', ''))::integer AS building_id,
        COALESCE(NULLIF(payload::jsonb->>'dataVencimento', ''), NULLIF(payload::jsonb->>'dueDate', ''), NULLIF(payload::jsonb->>'dataEmissao', ''), NULLIF(payload::jsonb->>'issueDate', ''), NULLIF(payload::jsonb->>'date', ''), NULLIF(payload::jsonb->>'data', '')) AS data_vencimento,
        COALESCE(NULLIF(payload::jsonb->>'dataPagamento', ''), NULLIF(payload::jsonb->>'paymentDate', ''), NULLIF(payload::jsonb->>'dataRecebimento', '')) AS data_pagamento,
        COALESCE(NULLIF(payload::jsonb->>'rawValue', ''), NULLIF(payload::jsonb->>'value', ''), NULLIF(payload::jsonb->>'valorSaldo', ''), NULLIF(payload::jsonb->>'totalInvoiceAmount', ''), NULLIF(payload::jsonb->>'amount', ''), NULLIF(payload::jsonb->>'valor', ''))::numeric AS valor,
        COALESCE(NULLIF(payload::jsonb->>'descricao', ''), NULLIF(payload::jsonb->>'historico', ''), NULLIF(payload::jsonb->>'notes', ''), NULLIF(payload::jsonb->>'observacao', '')) AS descricao,
        COALESCE(NULLIF(payload::jsonb->>'situacao', ''), NULLIF(payload::jsonb->>'status', '')) AS situacao,
        COALESCE(NULLIF(payload::jsonb->>'nomeCliente', ''), NULLIF(payload::jsonb->>'cliente', ''), NULLIF(payload::jsonb->>'clientName', '')) AS client_name,
        payload::jsonb->>'nomeObra' AS nome_obra,
        COALESCE(NULLIF(payload::jsonb->>'documentNumber', ''), NULLIF(payload::jsonb->>'numeroDocumento', '')) AS document_number
    FROM sienge_raw_records
    WHERE dataset = 'accounts-statements';
    """

    with engine.connect() as conn:
        conn.execute(text(sql_pedidos))
        conn.execute(text(sql_financeiro))
        conn.execute(text(sql_receber))
        conn.commit()
    print("Views criadas com sucesso!")

if __name__ == "__main__":
    create_views()
