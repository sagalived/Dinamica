import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Any
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

import httpx

logger = logging.getLogger(__name__)


# Garante que o .env do projeto seja carregado mesmo quando este módulo
# é importado diretamente (ex.: scripts/snippets), antes do backend.main.
_ROOT_ENV = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=_ROOT_ENV, override=False)


class SiengeClient:
    def __init__(self):
        self._env_path = _ROOT_ENV
        self._env_mtime: float | None = None
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
        self.access_name = os.getenv("SIENGE_ACCESS_NAME", "").strip()
        self.token = os.getenv("SIENGE_TOKEN", "").strip()
        self.timeout = 30
        self.last_error: dict[str, Any] | None = None

        # Regras de auth:
        # - Se houver username/password, usa Basic.
        # - Se houver access_name/token, alguns ambientes do Sienge aceitam Basic (access_name/token)
        #   e outros aceitam Bearer + X-Access-Name. Vamos suportar ambos como fallback.
        self.has_user_pass = bool(self.username and self.password)
        self.has_access_token = bool(self.access_name and self.token)

        self.use_basic_auth = self.has_user_pass or self.has_access_token
        self.use_bearer_auth = self.has_access_token

        if not self.use_basic_auth and not self.use_bearer_auth:
            logger.warning("Sienge credentials not configured. Will return empty data.")
            self.is_configured = False
        else:
            self.is_configured = True

    def _refresh_from_dotenv_if_changed(self) -> None:
        """Atualiza credenciais lendo o .env quando o arquivo mudar.

        Observação: não sobrescreve os.environ (evita colidir com variáveis de ambiente reais).
        Isso é útil quando o token é alterado durante o desenvolvimento.
        """
        try:
            if not self._env_path.exists():
                return
            mtime = self._env_path.stat().st_mtime
            if self._env_mtime is not None and mtime <= self._env_mtime:
                return

            values = dotenv_values(self._env_path)

            def _v(key: str) -> str | None:
                raw = values.get(key)
                if raw is None:
                    return None
                return str(raw).strip()

            instance = (_v("SIENGE_INSTANCE") or os.getenv("SIENGE_INSTANCE", "")).strip().split(".")[0]
            raw_base_url = _v("SIENGE_BASE_URL") or os.getenv("SIENGE_BASE_URL", "")
            raw_base_url = (raw_base_url or "").strip()
            if raw_base_url and "api.sienge.com.br" in raw_base_url:
                self.base_url = raw_base_url.rstrip("/")
            elif instance:
                self.base_url = f"https://api.sienge.com.br/{instance}"
            elif raw_base_url:
                self.base_url = raw_base_url.rstrip("/")

            self.username = _v("SIENGE_USERNAME") or self.username
            self.password = _v("SIENGE_PASSWORD") or self.password
            self.access_name = _v("SIENGE_ACCESS_NAME") or self.access_name
            self.token = _v("SIENGE_TOKEN") or self.token

            self.has_user_pass = bool(self.username and self.password)
            self.has_access_token = bool(self.access_name and self.token)
            self.use_basic_auth = self.has_user_pass or self.has_access_token
            self.use_bearer_auth = self.has_access_token
            self.is_configured = bool(self.use_basic_auth or self.use_bearer_auth)

            self._env_mtime = mtime
        except Exception as exc:
            # Não falha request por erro de refresh; só registra.
            logger.debug("Failed to refresh SIENGE env from .env: %s", exc)

    def _record_error(self, endpoint: str, url: str, exc: Exception, params: dict[str, Any] | None = None) -> None:
        status_code = None
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
        self.last_error = {
            "endpoint": endpoint,
            "url": url,
            "status_code": status_code,
            "message": str(exc),
            "params": params or {},
        }

    def _base_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
        }

    def _auth_variants(self) -> list[tuple[dict, tuple[str, str] | None]]:
        """Retorna combinações (headers, auth) a serem tentadas.

        Ordem:
        1) Basic com username/password (se existir)
        2) Basic com access_name/token (se existir)
        3) Bearer + X-Access-Name (se existir)
        """
        variants: list[tuple[dict, tuple[str, str] | None]] = []
        if self.has_user_pass:
            variants.append((self._base_headers(), (self.username, self.password)))
        if self.has_access_token:
            variants.append((self._base_headers(), (self.access_name, self.token)))
            bearer_headers = self._base_headers()
            bearer_headers["Authorization"] = f"Bearer {self.token}"
            bearer_headers["X-Access-Name"] = self.access_name
            variants.append((bearer_headers, None))
        # fallback: sem auth
        if not variants:
            variants.append((self._base_headers(), None))
        return variants

    async def _get_json_via_client(self, client: httpx.AsyncClient, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """Executa GET usando um AsyncClient já criado (útil para chamadas em lote)."""
        payload, err = await self._get_json_via_client_detailed(client, endpoint, params)
        self.last_error = err
        return payload

    async def _get_json_via_client_detailed(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any] | None]:
        """Executa GET e retorna (payload, last_error) de forma determinística.

        Útil para chamadas concorrentes: evita depender de self.last_error global.
        """
        self._refresh_from_dotenv_if_changed()
        if not self.is_configured:
            return None, {
                "endpoint": endpoint,
                "url": None,
                "status_code": None,
                "message": "Sienge credentials not configured",
                "params": params or {},
            }

        last_err: dict[str, Any] | None = None
        last_exc: Exception | None = None

        for url in self._candidate_urls(endpoint):
            for headers, auth in self._auth_variants():
                # retry leve para 429/5xx
                for attempt in range(3):
                    try:
                        response = await client.get(url, headers=headers, params=params, auth=auth)
                        if response.status_code == 404:
                            last_err = {
                                "endpoint": endpoint,
                                "url": url,
                                "status_code": 404,
                                "message": "404 Not Found",
                                "params": params or {},
                            }
                            break
                        if response.status_code == 401:
                            last_err = {
                                "endpoint": endpoint,
                                "url": url,
                                "status_code": 401,
                                "message": "401 Unauthorized",
                                "params": params or {},
                            }
                            break
                        if response.status_code == 429:
                            retry_after = response.headers.get("Retry-After")
                            last_err = {
                                "endpoint": endpoint,
                                "url": url,
                                "status_code": 429,
                                "message": f"429 Too Many Requests (Retry-After={retry_after})",
                                "params": params or {},
                            }
                            try:
                                wait_s = float(retry_after) if retry_after else (0.5 * (attempt + 1))
                            except ValueError:
                                wait_s = 0.5 * (attempt + 1)
                            await asyncio.sleep(min(wait_s, 2.0))
                            continue
                        if 500 <= response.status_code < 600 and attempt < 2:
                            last_err = {
                                "endpoint": endpoint,
                                "url": url,
                                "status_code": int(response.status_code),
                                "message": f"{response.status_code} Server Error",
                                "params": params or {},
                            }
                            await asyncio.sleep(0.3 * (attempt + 1))
                            continue

                        response.raise_for_status()
                        return response.json(), None
                    except Exception as exc:
                        last_exc = exc
                        status_code = None
                        if isinstance(exc, httpx.HTTPStatusError):
                            status_code = exc.response.status_code
                        last_err = {
                            "endpoint": endpoint,
                            "url": url,
                            "status_code": status_code,
                            "message": str(exc),
                            "params": params or {},
                        }
                        logger.warning("Sienge request failed for %s with params %s: %s", url, params or {}, exc)

        if last_exc:
            logger.error("Sienge request exhausted all candidates for %s with params %s: %s", endpoint, params or {}, last_exc)
        return None, last_err

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

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            return await self._get_json_via_client(client, endpoint, None)

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
            diagnostic = self.last_error or {}
            status_code = diagnostic.get("status_code")
            if status_code == 401:
                message = "Sienge returned 401: invalid or expired API credentials"
            elif status_code == 403:
                message = "Sienge returned 403: API user has no permission for this resource"
            else:
                message = "Successfully connected to Sienge API" if live_ok else "Sienge API returned no data"
            return {
                "ok": live_ok,
                "live": {
                    "ok": live_ok,
                    "status": "connected" if live_ok else "error",
                    "message": message,
                    "status_code": status_code,
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

    def _sync_date_range(self) -> tuple[str, str]:
        """Janela de datas usada no sync periódico.

        Por padrão, mantém uma janela operacional (histórico limitado + futuro).
        Para buscar histórico completo (ex.: totais all-time), configure:
                - SIENGE_SYNC_START_DATE=YYYY-MM-DD (ex.: 2000-01-01)
                    ou SIENGE_SYNC_HISTORY_DAYS (padrão 183)
        - SIENGE_SYNC_END_DATE=YYYY-MM-DD (opcional)
                    ou SIENGE_SYNC_FUTURE_DAYS (padrão 30)
        """

        # IMPORTANTE: defaults conservadores para não puxar anos de dados.
        # A tela trabalha com janela curta; histórico completo deve ser explícito via env.
        start_override = os.getenv("SIENGE_SYNC_START_DATE", "").strip()
        end_override = os.getenv("SIENGE_SYNC_END_DATE", "").strip()

        if start_override:
            start = start_override
        else:
            try:
                # Default: ~6 meses (aprox. 183 dias)
                history_days = int(os.getenv("SIENGE_SYNC_HISTORY_DAYS", "183") or "183")
            except ValueError:
                history_days = 183
            history_days = max(history_days, 1)
            start = (datetime.now() - timedelta(days=history_days)).strftime("%Y-%m-%d")

        if end_override:
            end = end_override
        else:
            try:
                # Default: pequeno horizonte futuro para não inflar dataset.
                future_days = int(os.getenv("SIENGE_SYNC_FUTURE_DAYS", "30") or "30")
            except ValueError:
                future_days = 30
            future_days = max(future_days, 0)
            end = (datetime.now() + timedelta(days=future_days)).strftime("%Y-%m-%d")

        return start, end

    async def fetch_pedidos(self) -> list[dict]:
        start, end = self._sync_date_range()
        params = {"startDate": start, "endDate": end}
        return await self._fetch_all_pages("/purchase-orders", params)

    async def fetch_pedidos_range(self, start: str, end: str) -> list[dict]:
        params = {"startDate": start, "endDate": end}
        return await self._fetch_all_pages("/purchase-orders", params)

    async def fetch_financeiro(self) -> list[dict]:
        """
        Busca titulos a pagar em uma janela operacional.

        Evita expandir installments de todos os bills no sync inicial: em bases
        grandes isso dispara dezenas de milhares de chamadas e impede a tela de
        atualizar. Detalhes pontuais continuam disponiveis por endpoints sob
        demanda.
        """
        start, end = self._sync_date_range()
        params = {"startDate": start, "endDate": end}
        return await self._fetch_all_pages("/bills", params)

    async def fetch_financeiro_range(self, start: str, end: str) -> list[dict]:
        params = {"startDate": start, "endDate": end}
        return await self._fetch_all_pages("/bills", params)

    async def fetch_receber(self) -> list[dict]:
        start, end = self._sync_date_range()
        params = {
            "startDate": start,
            "endDate": end,
        }
        return await self._fetch_all_pages("/accounts-statements", params)

    async def fetch_receber_range(self, start: str, end: str) -> list[dict]:
        params = {
            "startDate": start,
            "endDate": end,
        }
        return await self._fetch_all_pages("/accounts-statements", params)

    async def fetch_nfe_documents(
        self,
        *,
        startDate: str,
        endDate: str,
        limit: int = 100,
        offset: int = 0,
        companyId: int | None = None,
        supplierId: int | None = None,
        documentId: str | None = None,
        series: str | None = None,
        number: str | None = None,
    ) -> Any:
        """Busca documentos de NF-e emitidas (NFe).

        Endpoint esperado no Sienge: GET /nfe/documents
        Parametros principais: startDate/endDate (yyyy-MM-dd), limit/offset.
        """

        limit = int(limit or 100)
        offset = int(offset or 0)
        if limit < 1:
            limit = 1
        if limit > 200:
            limit = 200
        if offset < 0:
            offset = 0

        params: dict[str, Any] = {
            "startDate": startDate,
            "endDate": endDate,
            "limit": limit,
            "offset": offset,
        }
        if companyId is not None:
            params["companyId"] = int(companyId)
        if supplierId is not None:
            params["supplierId"] = int(supplierId)
        if documentId:
            params["documentId"] = str(documentId)
        if series:
            params["series"] = str(series)
        if number:
            params["number"] = str(number)

        return await self._get_json_with_params("/nfe/documents", params)

    async def _get_json_with_params(self, endpoint: str, params: dict[str, Any]) -> Any:
        if not self.is_configured:
            return None

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            return await self._get_json_via_client(client, endpoint, params)

    async def fetch_purchase_order_items(self, order_id: int) -> list[dict]:
        return self._extract_collection(await self._get_json(f"/purchase-orders/{order_id}/items"))

    async def fetch_purchase_quotation(self, quotation_id: int) -> dict[str, Any]:
        payload = await self._get_json(f"/purchase-quotations/{quotation_id}")
        return payload if isinstance(payload, dict) else {}

    async def fetch_bill_buildings_cost(self, bill_id: int | str) -> Any:
        """Busca o rateio por obra (buildings cost) de um título (bill).

        Endpoint esperado no Sienge: GET /bills/{billId}/buildings-cost
        """
        return await self._get_json(f"/bills/{bill_id}/buildings-cost")

    async def fetch_bill_buildings_cost_with_client(self, client: httpx.AsyncClient, bill_id: int | str) -> Any:
        """Versão otimizada para uso em lote com um AsyncClient reaproveitado."""
        return await self._get_json_via_client(client, f"/bills/{bill_id}/buildings-cost", None)

    async def fetch_bill_buildings_cost_with_client_detailed(
        self, client: httpx.AsyncClient, bill_id: int | str
    ) -> tuple[Any, dict[str, Any] | None]:
        """Versão otimizada com retorno de diagnóstico por chamada."""
        return await self._get_json_via_client_detailed(client, f"/bills/{bill_id}/buildings-cost", None)

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
