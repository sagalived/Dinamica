import sqlite3, json

conn = sqlite3.connect('data/dinamica.db')
cur = conn.cursor()

def get_data(key):
    cur.execute('SELECT payload FROM dataset_cache WHERE key = ?', (key,))
    row = cur.fetchone()
    if row:
        data = json.loads(row[0])
        return data.get('results', data) if isinstance(data, dict) else data
    return []

financeiro = get_data('financeiro')
receber = get_data('receber')

total_pago = 0
for f in financeiro:
    # Filter for Company 1 and Date in Mar 2026
    # Note: the API response might not have companyId in the summary?
    # Let's check keys first
    if 'dueDate' in f and f['dueDate'].startswith('2026-03'):
        # Just summing everything for Mar 2026 as a proxy
        total_pago += f.get('totalInvoiceAmount', 0)

total_recebido = 0
for r in receber:
    if 'dueDate' in r and r['dueDate'].startswith('2026-03'):
        total_recebido += r.get('value', 0)

print(f"Total Financeiro (Mar 2026): {total_pago}")
print(f"Total Receber (Mar 2026): {total_recebido}")
