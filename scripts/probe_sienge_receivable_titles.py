from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient


def compact(value: Any, max_len: int = 900) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = str(value)
    text = " ".join(text.split())
    return text[:max_len]


async def main() -> None:
    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    endpoints = [
        "/accounts-receivable",
        "/accounts-receivable/receivable-bills",
        "/accounts-receivable/bills",
        "/receivable-bills",
        "/accounts-receivable/titles",
        "/accounts-receivable/installments",
    ]
    param_sets = [
        {"startDate": "2025-11-01", "endDate": "2025-11-30", "limit": 3, "offset": 0},
        {"limit": 3, "offset": 0},
        {"issueDateStart": "2025-11-01", "issueDateEnd": "2025-11-30", "limit": 3, "offset": 0},
        {"emissionStartDate": "2025-11-01", "emissionEndDate": "2025-11-30", "limit": 3, "offset": 0},
    ]

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as http:
        for endpoint in endpoints:
            print(f"\n### {endpoint}", flush=True)
            for params in param_sets:
                url = client._candidate_urls(endpoint)[0]
                headers, auth = client._auth_variants()[0]
                try:
                    response = await http.get(url, headers=headers, auth=auth, params=params)
                    body: Any
                    try:
                        body = response.json()
                    except Exception:
                        body = response.text
                    print(f"{response.status_code} params={params} body={compact(body)}", flush=True)
                    if 200 <= response.status_code < 300:
                        rows = []
                        if isinstance(body, dict):
                            data = body.get("data")
                            if isinstance(data, list):
                                rows = data
                            elif isinstance(body.get("results"), list):
                                rows = body["results"]
                        if rows:
                            print("keys=" + ", ".join(rows[0].keys()), flush=True)
                            bill_id = rows[0].get("receivableBillId")
                            if bill_id:
                                detail_paths = [
                                    f"/accounts-receivable/receivable-bills/{bill_id}",
                                    f"/accounts-receivable/receivable-bills/{bill_id}/installments",
                                    f"/accounts-receivable/receivable-bills/{bill_id}/receipts",
                                ]
                                for detail_path in detail_paths:
                                    detail_url = client._candidate_urls(detail_path)[0]
                                    detail_resp = await http.get(detail_url, headers=headers, auth=auth)
                                    try:
                                        detail_body = detail_resp.json()
                                    except Exception:
                                        detail_body = detail_resp.text
                                    print(f"DETAIL {detail_path} {detail_resp.status_code} {compact(detail_body, 1600)}", flush=True)
                            return
                except Exception as exc:
                    print(f"ERR params={params} {exc}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
