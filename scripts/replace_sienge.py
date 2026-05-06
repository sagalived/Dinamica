import os

with open('backend/routers/sienge.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start_idx = -1
end_idx = -1
for i, line in enumerate(lines):
    if line.startswith('def _legacy_bootstrap_payload('):
        start_idx = i
    if start_idx != -1 and line.startswith('    return {') and i+1 < len(lines) and 'obras' in lines[i+1]:
        end_idx = i + 10
        break

if start_idx != -1 and end_idx != -1:
    new_func = '''def _legacy_bootstrap_payload(db: Session, include_transactions: bool = True) -> dict[str, Any]:
    obras = [
        {
            "id": b.id, "name": b.name, "code": b.id, "codigoVisivel": b.id,
            "address": b.address, "companyId": b.company_id, "cnpj": b.cnpj,
            "engineer": "Aguardando Avaliação",
        }
        for b in db.scalars(select(Building)).all()
    ]
    companies = [
        {
            "id": c.id, "name": c.name, "tradeName": c.trade_name,
            "companyName": c.name, "cnpj": c.cnpj,
        }
        for c in db.scalars(select(Company)).all()
    ]
    credores = [
        {
            "id": c.id, "name": c.name, "tradeName": c.trade_name,
            "cnpj": c.cnpj, "city": c.city, "state": c.state, "active": c.active,
        }
        for c in db.scalars(select(Creditor)).all()
    ]
    usuarios = [
        {
            "id": row.id, "name": row.name, "nome": row.name,
            "email": row.email, "active": row.active,
        }
        for row in db.scalars(select(DirectoryUser)).all()
    ]

    pedidos = []
    financeiro = []
    receber = []

    if include_transactions:
        try:
            for row in db.execute(text("SELECT * FROM vw_pedidos")).mappings():
                pedidos.append({
                    "id": row["id"] or 0,
                    "buildingId": row["building_id"] or 0,
                    "idObra": row["building_id"] or 0,
                    "codigoVisivelObra": str(row["building_id"] or ""),
                    "companyId": row["company_id"],
                    "buyerId": row["buyer_id"] or "",
                    "idComprador": row["buyer_id"] or "",
                    "codigoComprador": row["buyer_id"] or "",
                    "supplierId": row["supplier_id"] or 0,
                    "codigoFornecedor": row["supplier_id"] or 0,
                    "date": row["data_emissao"] or "",
                    "dataEmissao": row["data_emissao"] or "",
                    "totalAmount": float(row["total_amount"] or 0),
                    "valorTotal": float(row["total_amount"] or 0),
                    "status": row["status"] or "N/A",
                    "situacao": row["status"] or "N/A",
                    "paymentCondition": row["payment_condition"] or "A Prazo",
                    "condicaoPagamento": row["payment_condition"] or "A Prazo",
                    "deliveryDate": row["delivery_date"] or "",
                    "dataEntrega": row["delivery_date"] or "",
                    "internalNotes": row["internal_notes"] or "",
                    "observacao": row["internal_notes"] or "",
                    "nomeObra": row["nome_obra"] or f"Obra {row['building_id']}",
                    "nomeFornecedor": row["nome_fornecedor"] or f"Credor {row['supplier_id']}",
                    "nomeComprador": row["nome_comprador"] or row["buyer_id"],
                    "solicitante": row["solicitante"] or row["buyer_id"],
                    "requesterId": row["solicitante"] or row["buyer_id"],
                    "createdBy": row["solicitante"] or row["buyer_id"],
                })

            for row in db.execute(text("SELECT * FROM vw_financeiro")).mappings():
                financeiro.append({
                    "id": row["id"] or 0,
                    "numero": row["id"] or 0,
                    "codigoTitulo": row["id"] or 0,
                    "companyId": row["company_id"],
                    "creditorId": row["creditor_id"] or "",
                    "idCredor": row["creditor_id"] or "",
                    "buildingId": row["building_id"] or 0,
                    "idObra": row["building_id"] or 0,
                    "codigoObra": str(row["building_id"] or ""),
                    "dataVencimento": row["data_vencimento"] or "",
                    "descricao": row["descricao"] or "Título a Pagar",
                    "valor": float(row["valor"] or 0),
                    "situacao": row["situacao"] or "Pendente",
                    "status": row["situacao"] or "Pendente",
                    "creditorName": row["creditor_name"] or f"Credor {row['creditor_id']}",
                    "nomeCredor": row["creditor_name"] or f"Credor {row['creditor_id']}",
                    "nomeObra": row["nome_obra"] or f"Obra {row['building_id']}",
                    "documentNumber": row["document_number"] or "",
                })

            for row in db.execute(text("SELECT * FROM vw_receber")).mappings():
                receber.append({
                    "id": row["id"] or 0,
                    "numero": row["id"] or 0,
                    "numeroTitulo": row["id"] or 0,
                    "codigoTitulo": row["id"] or 0,
                    "companyId": row["company_id"],
                    "clientId": row["client_id"] or "",
                    "buildingId": row["building_id"] or 0,
                    "idObra": row["building_id"] or 0,
                    "codigoObra": str(row["building_id"] or ""),
                    "dataVencimento": row["data_vencimento"] or "",
                    "dataPagamento": row["data_pagamento"] or "",
                    "valor": float(row["valor"] or 0),
                    "descricao": row["descricao"] or "Título a Receber",
                    "situacao": row["situacao"] or "Pendente",
                    "clientName": row["client_name"] or f"Cliente {row['client_id']}",
                    "nomeCliente": row["client_name"] or f"Cliente {row['client_id']}",
                    "nomeObra": row["nome_obra"] or f"Obra {row['building_id']}",
                    "documentNumber": row["document_number"] or "",
                })
        except Exception as e:
            pass

    return {
        "obras": obras,
        "usuarios": usuarios,
        "credores": credores,
        "empresas": companies,
        "pedidos": pedidos,
        "financeiro": financeiro,
        "receber": receber,
        "itens_pedidos": {},
    }
'''
    new_lines = lines[:start_idx] + [new_func] + lines[end_idx+1:]
    with open('backend/routers/sienge.py', 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print('Function replaced successfully')
else:
    print('Could not find the function boundaries')
