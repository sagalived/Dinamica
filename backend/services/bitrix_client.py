from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BitrixResponseError(Exception):
    status_code: int | None
    message: str
    payload: Any | None = None

    def __str__(self) -> str:  # pragma: no cover
        code = self.status_code if self.status_code is not None else "?"
        return f"Bitrix error (status={code}): {self.message}"


class BitrixClient:
    def __init__(self, *, base_url: str | None, timeout_s: float = 30.0) -> None:
        self._base_url = (base_url or "").strip()
        self._timeout_s = float(timeout_s)

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url)

    def _build_url(self, method: str) -> str:
        base = self._base_url
        if not base:
            raise BitrixResponseError(status_code=None, message="BITRIX24_WEBHOOK_BASE_URL nao configurado")

        normalized = base.rstrip("/") + "/"
        m = method.strip().lstrip("/")
        if m.endswith(".json"):
            return normalized + m
        return normalized + f"{m}.json"

    async def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        url = self._build_url(method)
        payload = params or {}

        def _flatten(prefix: str, value: Any, out: list[tuple[str, str]]) -> None:
            if value is None:
                return
            if isinstance(value, (str, int, float, bool)):
                out.append((prefix, "1" if value is True else "0" if value is False else str(value)))
                return
            if isinstance(value, dict):
                for k, v in value.items():
                    key = f"{prefix}[{k}]" if prefix else str(k)
                    _flatten(key, v, out)
                return
            if isinstance(value, (list, tuple)):
                for i, v in enumerate(value):
                    key = f"{prefix}[{i}]"
                    _flatten(key, v, out)
                return

            # fallback (ex.: datetime)
            out.append((prefix, str(value)))

        flat: list[tuple[str, str]] = []
        for k, v in payload.items():
            _flatten(str(k), v, flat)

        # Webhooks do Bitrix aceitam parametros via x-www-form-urlencoded.
        async with httpx.AsyncClient(timeout=self._timeout_s) as client:
            try:
                resp = await client.post(url, data=flat)
            except Exception as exc:  # network error
                logger.warning("Bitrix request failed: %s", exc)
                raise BitrixResponseError(status_code=None, message=str(exc)) from exc

        if resp.status_code >= 400:
            raise BitrixResponseError(status_code=resp.status_code, message=resp.text[:500])

        try:
            data = resp.json()
        except Exception as exc:
            raise BitrixResponseError(status_code=resp.status_code, message="Resposta nao-JSON do Bitrix") from exc

        # Bitrix padrao: {"result": ..., "error": ..., "error_description": ...}
        if isinstance(data, dict) and data.get("error"):
            raise BitrixResponseError(
                status_code=resp.status_code,
                message=str(data.get("error_description") or data.get("error")),
                payload=data,
            )

        return data
