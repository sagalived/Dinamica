import os
import logging
import asyncio
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class SiengeClient:
    def __init__(self):
        instance = os.getenv("SIENGE_INSTANCE", "").strip().split(".")[0]
        raw_base_url = os.getenv("SIENGE_BASE_URL", "").strip()
        if raw_base_url and "api.sienge.com.br" in raw_base_url:
            self.base_url = raw_base_url.rstrip("/")
        elif instance:
            self.base_url = f"https://api.sienge.com.br/{instance}"
        else:
            self.base_url = raw_base_url.rstrip("/") or "https://api.sienge.com.br/dinamicaempreendimentos"
        self.username = os.getenv("SIENGE_USERNAME", "").strip()
        self.password = os.getenv("SIENGE_PASSWORD", "").strip()
        self.access_name = os.getenv("SIENGE_ACCESS_NAME", "")
        self.token = os.getenv("SIENGE_TOKEN", "")
        self.timeout = 30

        self.basic_auth_user = self.access_name or self.username
        self.basic_auth_password = self.token or self.password

        self.use_basic_auth = bool(self.basic_auth_user and self.basic_auth_password)
        self.use_bearer_auth = bool(self.access_name and self.token)

        if not self.use_basic_auth and not self.use_bearer_auth:
            logger.warning("Sienge credentials not configured. Will return empty data.")
            self.is_configured = False
        else:
            self.is_configured = True

    def _get_headers(self) -> dict:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        if self.use_bearer_auth and not self.use_basic_auth:
            headers["Authorization"] = f"Bearer {self.token}"
            headers["X-Access-Name"] = self.access_name
        return headers

    def _candidate_urls(self, endpoint: str) -> list[str]:
        normalized_base = self.base_url.rstrip("/")
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"

        if normalized_endpoint.startswith("/public/api/v1/") or normalized_endpoint.startswith("/api/v1/"):
            return [f"{normalized_base}{normalized_endpoint}"]

        suffix = normalized_endpoint.lstrip("/")
        return [
            f"{normalized_base}/public/api/v1/{suffix}",
            f"{normalized_base}/api/v1/{suffix}",
        ]

    async def _get_json(self, endpoint: str) -> Any:
        if not self.is_configured:
            return None

        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for url in self._candidate_urls(endpoint):
                try:
                    response = await client.get(
                        url,
                        headers=self._get_headers(),
                        auth=(self.basic_auth_user, self.basic_auth_password) if self.use_basic_auth else None,
                    )
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    return response.json()
                except Exception as exc:
                    last_error = exc
                    logger.warning("Sienge request failed for %s: %s", url, exc)

        if last_error:
            logger.error("Sienge request exhausted all candidates for %s: %s", endpoint, last_error)
        return None

    async def _fetch_all_pages(self, endpoint: str, base_params: dict[str, Any] | None = None, limit: int = 200) -> list[dict]:
        params = dict(base_params or {})
        all_results: list[dict] = []
        offset = 0

        while True:
            current_params = dict(params)
            current_params["limit"] = limit
            current_params["offset"] = offset
            payload = await self._get_json_with_params(endpoint, current_params)
            results = self._extract_collection(payload)
            if not results:
                break

            all_results.extend(results)
            offset += len(results)

            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            if len(results) < limit or (isinstance(count, int) and offset >= count):
                break

            await asyncio.sleep(0.05)

        return all_results

    @staticmethod
    def _extract_collection(payload: Any) -> list[dict]:
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict) and isinstance(data.get("results"), list):
                return data["results"]
            if isinstance(data, list):
                return data
            if isinstance(payload.get("results"), list):
                return payload["results"]
            if isinstance(payload.get("data"), list):
                return payload["data"]
        if isinstance(payload, list):
            return payload
        return []

    async def fetch_users(self) -> list[dict]:
        return await self._fetch_all_pages("/users")

    async def test_connection(self) -> dict:
        if not self.is_configured:
            return {
                "ok": False,
                "live": {
                    "ok": False,
                    "status": "not_configured",
                    "message": "Sienge credentials not configured",
                },
                "cache": {"source": "fallback", "counts": {}},
            }

        try:
            payload = await self._get_json("/companies")
            live_ok = bool(self._extract_collection(payload))
            return {
                "ok": live_ok,
                "live": {
                    "ok": live_ok,
                    "status": "connected" if live_ok else "error",
                    "message": "Successfully connected to Sienge API" if live_ok else "Sienge API returned no data",
                },
                "cache": {"source": "sienge_live" if live_ok else "fallback", "counts": {}},
            }
        except Exception as e:
            logger.error(f"Sienge connection error: {str(e)}")
            return {
                "ok": False,
                "live": {
                    "ok": False,
                    "status": "error",
                    "message": str(e),
                },
                "cache": {"source": "fallback", "counts": {}},
            }

    async def fetch_obras(self) -> list[dict]:
        return await self._fetch_all_pages("/enterprises")

    async def fetch_empresas(self) -> list[dict]:
        return await self._fetch_all_pages("/companies")

    async def fetch_credores(self) -> list[dict]:
        return await self._fetch_all_pages("/creditors")

    async def fetch_pedidos(self) -> list[dict]:
        return await self._fetch_all_pages("/purchase-orders")

    async def fetch_financeiro(self) -> list[dict]:
        params = {
            "startDate": "1900-01-01",
            "endDate": "2030-12-31",
        }
        return await self._fetch_all_pages("/bills", params)

    async def fetch_receber(self) -> list[dict]:
        params = {
            "startDate": "1900-01-01",
            "endDate": "2030-12-31",
        }
        return await self._fetch_all_pages("/accounts-statements", params)

    async def _get_json_with_params(self, endpoint: str, params: dict[str, Any]) -> Any:
        if not self.is_configured:
            return None

        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for url in self._candidate_urls(endpoint):
                try:
                    response = await client.get(
                        url,
                        headers=self._get_headers(),
                        params=params,
                        auth=(self.basic_auth_user, self.basic_auth_password) if self.use_basic_auth else None,
                    )
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    return response.json()
                except Exception as exc:
                    last_error = exc
                    logger.warning("Sienge request failed for %s with params %s: %s", url, params, exc)

        if last_error:
            logger.error("Sienge request exhausted all candidates for %s with params %s: %s", endpoint, params, last_error)
        return None

    async def fetch_purchase_order_items(self, order_id: int) -> list[dict]:
        return self._extract_collection(await self._get_json(f"/purchase-orders/{order_id}/items"))

    async def fetch_purchase_quotation(self, quotation_id: int) -> dict[str, Any]:
        payload = await self._get_json(f"/purchase-quotations/{quotation_id}")
        return payload if isinstance(payload, dict) else {}

    async def fetch_itens_pedidos(self) -> dict[int, list[dict]]:
        if not self.is_configured:
            return {}

        pedidos = await self.fetch_pedidos()
        itens_by_pedido: dict[int, list[dict]] = {}
        for pedido in pedidos[:50]:
            order_id = pedido.get("id") or pedido.get("numero")
            if not order_id:
                continue
            try:
                order_id_int = int(order_id)
            except (TypeError, ValueError):
                continue
            items = await self.fetch_purchase_order_items(order_id_int)
            if items:
                itens_by_pedido[order_id_int] = items
        return itens_by_pedido

    async def fetch_saldo_bancario(self) -> float | None:
        receber = await self.fetch_receber()
        if not receber:
            return None

        income = 0.0
        expense = 0.0
        for item in receber:
            try:
                value = float(
                    item.get("value")
                    or item.get("valor")
                    or item.get("valorSaldo")
                    or item.get("totalInvoiceAmount")
                    or item.get("valorTotal")
                    or item.get("amount")
                    or 0
                )
            except (TypeError, ValueError):
                value = 0.0
            if str(item.get("type") or "").strip().lower() == "expense":
                expense += value
            else:
                income += value
        return income - expense


sienge_client = SiengeClient()
