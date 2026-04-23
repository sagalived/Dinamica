import json

with open('data/receber.json', 'r', encoding='utf-8') as f:
    rec = json.load(f)
if isinstance(rec, dict) and 'data' in rec: rec = rec['data'].get('results', [])
elif isinstance(rec, dict) and 'results' in rec: rec = rec['results']

print("--- ACCOUNTS STATEMENTS BIGGEST ITEMS ---")
incomes = []
expenses = []
for r_item in rec:
    if not isinstance(r_item, dict): continue
    date_val = r_item.get('date', '')
    if '2026-03' in date_val:
        links = r_item.get('links', [])
        if not any(link.get('rel') == 'company' and link.get('href', '').endswith('/companies/1') for link in links): continue
        
        stype = r_item.get('statementType', '')
        if 'Transf' in stype or stype == 'Saque': continue
        
        val = r_item.get('value', 0)
        desc = r_item.get('description', '')
        if r_item.get('type') == 'Income': incomes.append((val, desc))
        else: expenses.append((val, desc))

incomes.sort(key=lambda x: x[0], reverse=True)
expenses.sort(key=lambda x: x[0], reverse=True)

print("TOP INCOMES:")
for v, d in incomes[:10]: print(f"{v}: {d}")
print("TOP EXPENSES:")
for v, d in expenses[:10]: print(f"{v}: {d}")
