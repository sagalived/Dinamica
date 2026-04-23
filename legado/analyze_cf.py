import json
from collections import defaultdict

with open('data/receber.json', 'r', encoding='utf-8') as f:
    raw = json.load(f)
items = raw['results'] if isinstance(raw, dict) else raw

# Lançamentos empresa 1 março 2026 com conta bancária
mar_emp1 = []
for x in items:
    d = x.get('date') or x.get('data') or ''
    if not d.startswith('2026-03'):
        continue
    company_id = None
    bank_account = None
    for lnk in (x.get('links') or []):
        if lnk.get('rel') == 'company':
            try:
                company_id = int(lnk['href'].rstrip('/').split('/')[-1])
            except:
                pass
        if lnk.get('rel') == 'bank-account':
            bank_account = lnk.get('href', '').rstrip('/').split('/')[-1]
    if company_id == 1:
        mar_emp1.append({'item': x, 'bank': bank_account})

# PDF Total: Income=2029313.12, Expense=2012460.49
# 
# Abordagem: o PDF do Sienge "Fluxo de Caixa" mostra lançamentos de TODAS as contas
# mas a coluna "Entradas" e "Saídas" no PDF SÃO OS VALORES BRUTOS de cada linha
# (incluindo valores negativos na coluna Entradas para estornos/reversões)
# 
# O PROBLEMA NO NOSSO SISTEMA:
# Estamos somando abs(value) para Income e abs(value) para Expense
# Mas o PDF soma value (com sinal) para Entradas e abs(value) para Saídas
# 
# No PDF, uma "Entrada negativa" significa que houve um estorno de entrada
# (dinheiro saiu da conta, é computado como entrada negativa)
# 
# Vamos verificar: quantos Income têm value negativo?

income_neg = [(r['item'], r['bank']) for r in mar_emp1 if r['item'].get('type') == 'Income' and float(r['item'].get('value') or 0) < 0]
income_pos = [(r['item'], r['bank']) for r in mar_emp1 if r['item'].get('type') == 'Income' and float(r['item'].get('value') or 0) >= 0]
expense_neg = [(r['item'], r['bank']) for r in mar_emp1 if r['item'].get('type') == 'Expense' and float(r['item'].get('value') or 0) > 0]
expense_pos = [(r['item'], r['bank']) for r in mar_emp1 if r['item'].get('type') == 'Expense' and float(r['item'].get('value') or 0) <= 0]

print("=== ANÁLISE DE SINAIS ===")
print(f"Income positivos: {len(income_pos)} = {sum(float(x.get('value') or 0) for x,_ in income_pos):,.2f}")
print(f"Income negativos: {len(income_neg)} = {sum(float(x.get('value') or 0) for x,_ in income_neg):,.2f}")
print(f"Expense positivos (value>0): {len(expense_neg)} = {sum(float(x.get('value') or 0) for x,_ in expense_neg):,.2f}")  
print(f"Expense negativos (value<0): {len(expense_pos)} = {sum(float(x.get('value') or 0) for x,_ in expense_pos):,.2f}")
print()

# NOVA HIPÓTESE: O PDF tem registros de múltiplas empresas do GRUPO
# "Total do período 2.029.313,12 / 2.012.460,49" pode ser o GRUPO, não só empresa 1
# "Grupo de empresa 4 - POTENCIAL, Empresa 1 - DINAMICA"
# Isso significa que é filtrado apenas pela Empresa 1!

# Vamos calcular empresa 1 EXATAMENTE como o PDF faz:
# Entradas = soma(value) para Income (pode ser negativo = estorno)
# Saídas   = soma(abs(value)) para Expense onde value > 0 (saída real)
#           MENOS soma(abs(value)) para Expense onde value < 0 (estorno de saída)

total_entradas_pdf_style = sum(float(r['item'].get('value') or 0) for r in mar_emp1 if r['item'].get('type') == 'Income')
total_saidas_pdf_style = sum(abs(float(r['item'].get('value') or 0)) for r in mar_emp1 if r['item'].get('type') == 'Expense' and float(r['item'].get('value') or 0) > 0)
total_saidas_pdf_neg = sum(abs(float(r['item'].get('value') or 0)) for r in mar_emp1 if r['item'].get('type') == 'Expense' and float(r['item'].get('value') or 0) < 0)

print("=== CÁLCULO ESTILO PDF ===")
print(f"Entradas (soma Income com sinal): {total_entradas_pdf_style:,.2f}")
print(f"Saídas (soma Expense positivos):  {total_saidas_pdf_style:,.2f}")
print(f"Saídas negativas (estornos):      -{total_saidas_pdf_neg:,.2f}")
print(f"Saídas líquidas:                  {total_saidas_pdf_style - total_saidas_pdf_neg:,.2f}")
print()
print(f"PDF: Entradas=2.029.313,12  Saídas=2.012.460,49")
print()

# INVESTIGAR ITAUPJ - por que tem 408.000 de Income?
itau = [r for r in mar_emp1 if r['bank'] == 'ITAUPJ']
print("=== ITAUPJ (conta Itaú PJ) ===")
print(f"Total registros: {len(itau)}")
for r in itau:
    x = r['item']
    print(f"  {x.get('date')} value={x.get('value')} type={x.get('type')} origin={x.get('statementOrigin')} billId={x.get('billId')} desc={str(x.get('description',''))[:60]}")
