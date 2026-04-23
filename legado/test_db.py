import sqlite3, json
conn = sqlite3.connect('data/dinamica.db')
cur = conn.cursor()
cur.execute('SELECT payload FROM dataset_cache WHERE key = ?', ('financeiro',))
row = cur.fetchone()
if row:
    data = json.loads(row[0])
    results = data.get('results', [])
    print(f'Total financeiro items: {len(results)}')
    for r in results[:5]:
        print(f"ID: {r.get('id')}, Amount: {r.get('totalInvoiceAmount')}, Budget Categories: {r.get('budgetCategories')}")

cur.execute('SELECT payload FROM dataset_cache WHERE key = ?', ('receber',))
row = cur.fetchone()
if row:
    data = json.loads(row[0])
    results = data.get('results', [])
    print(f'\nTotal receber items: {len(results)}')
    for r in results[:5]:
        print(f"ID: {r.get('id')}, Amount: {r.get('value')}, Type: {r.get('type')}, StatementType: {r.get('statementType')}, Budget Categories: {r.get('budgetCategories')}")
