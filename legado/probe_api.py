import sys
import json
from app import sienge_get

endpoints = [
    '/public/api/v1/dre',
    '/public/api/v1/accounting/dre',
    '/public/api/v1/financial-reports/dre',
    '/public/api/v1/budget-categories',
    '/public/api/v1/accounting/ledger-accounts'
]

for ep in endpoints:
    print(f"\nProbing {ep}...")
    try:
        r = sienge_get(ep)
        print(f"Status: {r.status_code}")
        if r.status_code < 400:
            data = r.json()
            if isinstance(data, dict):
                print(f"Keys: {list(data.keys())}")
                results = data.get('results', [])
                if results and len(results) > 0:
                    print(f"Sample: {json.dumps(results[0], ensure_ascii=False)[:300]}")
            elif isinstance(data, list):
                print(f"List length: {len(data)}")
                if data:
                    print(f"Sample: {json.dumps(data[0], ensure_ascii=False)[:300]}")
    except Exception as e:
        print(f"Error: {e}")
