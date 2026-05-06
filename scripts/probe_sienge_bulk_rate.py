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
    headers, auth = client._auth_variants()[0]
    url = client.base_url.rstrip("/") + "/public/api/bulk-data/v1/outcome"
    params = {
        "startDate": "2026-05-01",
        "endDate": "2026-05-06",
        "selectionType": "P",
        "correctionIndexerId": 0,
        "correctionDate": "2026-05-06",
    }
    async with httpx.AsyncClient(timeout=60) as http:
        response = await http.get(url, headers=headers, auth=auth, params=params)
        print(response.status_code)
        print(dict(response.headers))
        print(response.text[:500])


if __name__ == "__main__":
    asyncio.run(main())
