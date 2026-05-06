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
    companies = await client.fetch_empresas()
    print("companies", [(c.get("id"), c.get("name") or c.get("nome") or c.get("tradeName")) for c in companies[:20]])

    base = client.base_url.rstrip("/") + "/public/api/bulk-data/v1"
    headers, auth = client._auth_variants()[0]

    async with httpx.AsyncClient(timeout=120) as http:
        for company in companies[:5]:
            company_id = company.get("id")
            response = await http.get(
                base + "/invoice-itens",
                headers=headers,
                auth=auth,
                params={
                    "companyId": company_id,
                    "startDate": "2026-05-01",
                    "endDate": "2026-05-06",
                    "showCostCenterId": "S",
                    "_async": "true",
                    "_asyncChunkMaxSize": 4096,
                },
            )
            print("invoice-itens", company_id, response.status_code, response.text[:300])

        for indexer_id in range(0, 10):
            response = await http.get(
                base + "/outcome",
                headers=headers,
                auth=auth,
                params={
                    "startDate": "2026-05-01",
                    "endDate": "2026-05-06",
                    "selectionType": "P",
                    "correctionIndexerId": indexer_id,
                    "correctionDate": "2026-05-06",
                    "_async": "true",
                    "_asyncChunkMaxSize": 4096,
                },
            )
            print("outcome", indexer_id, response.status_code, response.text[:300])


if __name__ == "__main__":
    asyncio.run(main())
