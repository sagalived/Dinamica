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
    base = client.base_url.rstrip("/") + "/public/api/bulk-data/v1"
    headers, auth = client._auth_variants()[0]
    async with httpx.AsyncClient(timeout=180) as http:
        inv = await http.get(
            base + "/invoice-itens",
            headers=headers,
            auth=auth,
            params={"companyId": 1, "startDate": "2026-05-01", "endDate": "2026-05-06", "showCostCenterId": "S"},
        )
        print("invoice", inv.status_code, inv.text[:500])
        out = await http.get(
            base + "/outcome",
            headers=headers,
            auth=auth,
            params={
                "startDate": "2026-05-01",
                "endDate": "2026-05-06",
                "selectionType": "P",
                "correctionIndexerId": 0,
                "correctionDate": "2026-05-06",
                "withBankMovements": "true",
            },
        )
        print("outcome", out.status_code, out.text[:500])


if __name__ == "__main__":
    asyncio.run(main())
