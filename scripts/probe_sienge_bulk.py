from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient


async def main() -> None:
    client = SiengeClient()
    endpoints = [
        "invoice-items",
        "invoices",
        "taxes",
        "accounts-payable",
        "cost-distributions",
        "chart-of-accounts",
        "cost-centers",
        "outcome",
        "account-cost-center-balance",
        "account-company-balance",
    ]
    params = {"startDate": "2026-05-01", "endDate": "2026-05-06"}
    async with httpx.AsyncClient(timeout=60) as http:
        for endpoint in endpoints:
            url = f"{client.base_url.rstrip('/')}/public/api/bulk-data/v1/{endpoint}"
            for headers, auth in client._auth_variants()[:1]:
                response = await http.get(url, headers=headers, auth=auth, params=params)
                body = response.text[:220].replace("\n", " ")
                print(f"{endpoint};{response.status_code};{body}")


if __name__ == "__main__":
    asyncio.run(main())
