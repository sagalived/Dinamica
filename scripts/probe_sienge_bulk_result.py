from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient


async def wait_result(http: httpx.AsyncClient, base: str, identifier: str, headers: dict, auth: tuple[str, str] | None) -> list[dict]:
    for _ in range(90):
        status = await http.get(f"{base}/async/{identifier}", headers=headers, auth=auth)
        print("status", identifier, status.status_code, status.text[:200])
        status.raise_for_status()
        payload = status.json()
        if payload.get("status") == "Finished":
            rows: list[dict] = []
            for chunk in range(1, int(payload.get("chunks") or 1) + 1):
                result = await http.get(f"{base}/async/{identifier}/result/{chunk}", headers=headers, auth=auth)
                print("result", result.status_code, result.text[:300])
                result.raise_for_status()
                data = result.json().get("data") or []
                rows.extend(x for x in data if isinstance(x, dict))
            return rows
        if payload.get("status") == "Failed":
            return [{"_failed": payload}]
        await asyncio.sleep(2)
    return []


async def main() -> None:
    client = SiengeClient()
    base = client.base_url.rstrip("/") + "/public/api/bulk-data/v1"
    headers, auth = client._auth_variants()[0]
    async with httpx.AsyncClient(timeout=180) as http:
        inv = await http.get(
            base + "/invoice-itens",
            headers=headers,
            auth=auth,
            params={
                "companyId": 1,
                "startDate": "2026-05-01",
                "endDate": "2026-05-06",
                "showCostCenterId": "S",
                "_async": "true",
                "_asyncChunkMaxSize": 4096,
            },
        )
        print("invoice start", inv.status_code, inv.text)
        inv_id = inv.json().get("identifier")
        if inv_id:
            rows = await wait_result(http, base, inv_id, headers, auth)
            print("invoice rows", len(rows), rows[:2])

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
                "_async": "true",
                "_asyncChunkMaxSize": 4096,
            },
        )
        print("outcome start", out.status_code, out.text)
        out_id = out.json().get("identifier")
        if out_id:
            rows = await wait_result(http, base, out_id, headers, auth)
            print("outcome rows", len(rows), rows[:2])


if __name__ == "__main__":
    asyncio.run(main())
