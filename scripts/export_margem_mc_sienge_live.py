from __future__ import annotations

import argparse
import asyncio
import calendar
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient


@dataclass(frozen=True)
class MonthRange:
    start: date
    end: date


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        pass
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def add_months(d: date, delta: int) -> date:
    y = d.year + (d.month - 1 + delta) // 12
    m = (d.month - 1 + delta) % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def iter_chunks(start: date, end: date, chunk_months: int) -> list[MonthRange]:
    out: list[MonthRange] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        chunk_end = month_end(add_months(cursor, chunk_months - 1))
        if chunk_end > end:
            chunk_end = end
        out.append(MonthRange(start=cursor, end=chunk_end))
        cursor = add_months(cursor, chunk_months)
    return out


def iter_months(start: date, end: date) -> list[str]:
    cursor = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    months: list[str] = []
    while cursor <= last:
        months.append(month_key(cursor))
        cursor = add_months(cursor, 1)
    return months


def safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str):
        raw = value.strip().replace("R$", "").replace(" ", "")
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(",", ".")
        value = raw
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def pick_amount(item: dict[str, Any], names: list[str] | None = None) -> float:
    candidates = names or [
        "grossAmount",
        "grossValue",
        "invoiceAmount",
        "totalInvoiceAmount",
        "totalAmount",
        "totalValue",
        "amount",
        "value",
        "taxAmount",
        "paidAmount",
        "netAmount",
    ]
    for name in candidates:
        if name in item and item.get(name) not in (None, ""):
            return abs(safe_float(item.get(name)))
    return 0.0


def pick_date(item: dict[str, Any]) -> date | None:
    for name in [
        "issueDate",
        "emissionDate",
        "invoiceDate",
        "documentDate",
        "competenceDate",
        "paymentDate",
        "dueDate",
        "date",
        "createdDate",
    ]:
        parsed = parse_date(item.get(name))
        if parsed is not None:
            return parsed
    return None


def text_blob(item: dict[str, Any]) -> str:
    pieces: list[str] = []
    for key, value in item.items():
        if isinstance(value, (str, int, float)):
            pieces.append(f"{key}:{value}")
    return " ".join(pieces).lower()


def looks_variable_account(item: dict[str, Any], chart_accounts: dict[str, dict[str, Any]]) -> bool:
    blob = text_blob(item)
    for key in ["accountId", "accountCode", "chartOfAccountId", "financialCategoryId", "financialAccountId"]:
        account_id = str(item.get(key) or "").strip()
        if account_id and account_id in chart_accounts:
            blob += " " + text_blob(chart_accounts[account_id])

    include_terms = [
        "comissao",
        "comissão",
        "imposto",
        "tribut",
        "taxa",
        "custo direto",
        "custos diretos",
        "custo de obra",
        "custos de obra",
        "insumo",
        "material",
        "servico",
        "serviço",
        "empreiteir",
        "mao de obra",
        "mão de obra",
    ]
    exclude_terms = ["administrativ", "despesa fixa", "fixa", "salario administrativo", "salário administrativo"]
    return any(term in blob for term in include_terms) and not any(term in blob for term in exclude_terms)


def extract_collection(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return [x for x in data["results"] if isinstance(x, dict)]
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


class BulkSienge:
    def __init__(self) -> None:
        self.client = SiengeClient()
        if not getattr(self.client, "is_configured", False):
            raise SystemExit("SIENGE nao esta configurado no .env.")
        self.timeout = httpx.Timeout(180.0)

    def urls(self, endpoint: str) -> list[str]:
        base = self.client.base_url.rstrip("/")
        endpoint = endpoint.strip("/")
        return [
            f"{base}/public/api/bulk-data/v1/{endpoint}",
            f"{base}/bulk-data/v1/{endpoint}",
        ]

    async def get(self, endpoint: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
        async with httpx.AsyncClient(timeout=self.timeout) as http:
            last_error: str | None = None
            for url in self.urls(endpoint):
                for headers, auth in self.client._auth_variants():
                    try:
                        resp = await http.get(url, headers=headers, auth=auth, params=params)
                        if resp.status_code == 404:
                            last_error = f"404 {url}"
                            break
                        if resp.status_code == 429:
                            retry = resp.headers.get("Retry-After")
                            await asyncio.sleep(float(retry or 4))
                            resp = await http.get(url, headers=headers, auth=auth, params=params)
                        if resp.status_code >= 400:
                            last_error = f"{resp.status_code} {url}: {resp.text[:300]}"
                            continue
                        payload = resp.json()
                        if isinstance(payload, dict) and payload.get("identifier"):
                            payload = await self._wait_async(http, payload["identifier"], headers, auth)
                        return extract_collection(payload), None
                    except Exception as exc:
                        last_error = f"{url}: {exc}"
            return [], last_error

    async def _wait_async(self, http: httpx.AsyncClient, identifier: str, headers: dict, auth: Any) -> dict[str, Any]:
        status_url = self.urls(f"async/{identifier}")[0]
        for _ in range(90):
            status_resp = await http.get(status_url, headers=headers, auth=auth)
            status_resp.raise_for_status()
            status = status_resp.json()
            if status.get("status") == "Finished":
                chunks = int(status.get("chunks") or 1)
                data: list[dict[str, Any]] = []
                for chunk in range(1, chunks + 1):
                    result_url = self.urls(f"async/{identifier}/result/{chunk}")[0]
                    result_resp = await http.get(result_url, headers=headers, auth=auth)
                    result_resp.raise_for_status()
                    data.extend(extract_collection(result_resp.json()))
                return {"data": data}
            if status.get("status") == "Failed":
                raise RuntimeError(json.dumps(status, ensure_ascii=False))
            await asyncio.sleep(2)
        raise TimeoutError(f"Bulk async timeout: {identifier}")


async def fetch_endpoint(
    bulk: BulkSienge,
    endpoint: str,
    start: date,
    end: date,
    date_param_sets: list[tuple[str, str]],
    verbose: bool,
) -> list[dict[str, Any]]:
    for start_key, end_key in date_param_sets:
        rows, err = await bulk.get(endpoint, {start_key: start.isoformat(), end_key: end.isoformat()})
        await asyncio.sleep(3.2)
        if rows:
            if verbose:
                print(f"  {endpoint} ({start_key}/{end_key}): {len(rows)}")
            return rows
        if verbose:
            print(f"  {endpoint} ({start_key}/{end_key}): vazio/erro {err or ''}".strip())
    return []


def build_chart_account_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in ["id", "accountId", "accountCode", "code", "financialCategoryId", "financialAccountId"]:
            value = str(row.get(key) or "").strip()
            if value:
                out[value] = row
    return out


async def run(args: argparse.Namespace) -> None:
    today = parse_date(args.today) if args.today else date.today()
    start = parse_date(args.start)
    if today is None or start is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    bulk = BulkSienge()
    date_params = [
        ("startDate", "endDate"),
        ("startIssueDate", "endIssueDate"),
        ("startEmissionDate", "endEmissionDate"),
        ("startDueDate", "endDueDate"),
        ("startCompetenceDate", "endCompetenceDate"),
    ]

    all_invoices: list[dict[str, Any]] = []
    all_invoice_items: list[dict[str, Any]] = []
    all_taxes: list[dict[str, Any]] = []
    all_cost_distributions: list[dict[str, Any]] = []
    all_accounts_payable: list[dict[str, Any]] = []

    chart_accounts, chart_err = await bulk.get("chart-of-accounts", {})
    await asyncio.sleep(3.2)
    if args.verbose:
        print(f"chart-of-accounts: {len(chart_accounts)} {chart_err or ''}".strip())
    chart_map = build_chart_account_map(chart_accounts)

    cost_centers, cc_err = await bulk.get("cost-centers", {})
    await asyncio.sleep(3.2)
    if args.verbose:
        print(f"cost-centers: {len(cost_centers)} {cc_err or ''}".strip())

    for chunk in iter_chunks(start, today, args.chunk_months):
        if args.verbose:
            print(f"[bulk] {chunk.start.isoformat()} -> {chunk.end.isoformat()}")
        all_invoices.extend(await fetch_endpoint(bulk, "invoices", chunk.start, chunk.end, date_params, args.verbose))
        all_invoice_items.extend(await fetch_endpoint(bulk, "invoice-items", chunk.start, chunk.end, date_params, args.verbose))
        all_taxes.extend(await fetch_endpoint(bulk, "taxes", chunk.start, chunk.end, date_params, args.verbose))
        all_cost_distributions.extend(await fetch_endpoint(bulk, "cost-distributions", chunk.start, chunk.end, date_params, args.verbose))
        all_accounts_payable.extend(await fetch_endpoint(bulk, "accounts-payable", chunk.start, chunk.end, date_params, args.verbose))

    receita: dict[str, float] = defaultdict(float)
    taxes: dict[str, float] = defaultdict(float)
    custos: dict[str, float] = defaultdict(float)
    despesas_variaveis: dict[str, float] = defaultdict(float)

    receita_source = all_invoices if all_invoices else all_invoice_items
    if not receita_source:
        raise SystemExit(
            "Nenhuma receita retornou dos endpoints Bulk Data testados "
            "(invoices/invoice-items). Arquivos nao foram gerados."
        )

    for item in receita_source:
        d = pick_date(item)
        amount = pick_amount(item)
        if d and amount > 0:
            receita[month_key(d)] += amount

    for item in all_taxes:
        d = pick_date(item)
        amount = pick_amount(item, ["taxAmount", "amount", "value", "totalAmount", "totalValue"])
        if d and amount > 0:
            taxes[month_key(d)] += amount

    for item in all_cost_distributions:
        d = pick_date(item)
        amount = pick_amount(item, ["appropriatedAmount", "distributedAmount", "amount", "value", "totalAmount", "totalValue"])
        if d and amount > 0 and (looks_variable_account(item, chart_map) or not chart_map):
            custos[month_key(d)] += amount

    for item in all_accounts_payable:
        d = pick_date(item)
        amount = pick_amount(item, ["paidAmount", "amount", "value", "totalAmount", "totalValue"])
        if d and amount > 0 and looks_variable_account(item, chart_map):
            despesas_variaveis[month_key(d)] += amount

    months = iter_months(start, today)
    write_outputs(
        Path(args.output_dir),
        months,
        receita,
        taxes,
        custos,
        despesas_variaveis,
        today,
        {
            "invoices": len(all_invoices),
            "invoice-items": len(all_invoice_items),
            "taxes": len(all_taxes),
            "cost-distributions": len(all_cost_distributions),
            "accounts-payable": len(all_accounts_payable),
            "chart-of-accounts": len(chart_accounts),
            "cost-centers": len(cost_centers),
            "receita_source": "invoices" if all_invoices else "invoice-items",
        },
    )


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def format_percent(value: float) -> str:
    return f"{value:.2f}".replace(".", ",") + "%"


def write_outputs(
    output_dir: Path,
    months: list[str],
    receita: dict[str, float],
    taxes: dict[str, float],
    custos: dict[str, float],
    despesas_variaveis: dict[str, float],
    generated_at: date,
    counts: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    mc_by_month: dict[str, float] = {}
    margem_by_month: dict[str, float] = {}
    variaveis_by_month: dict[str, float] = {}

    for month in months:
        variaveis = taxes.get(month, 0.0) + custos.get(month, 0.0) + despesas_variaveis.get(month, 0.0)
        mc = receita.get(month, 0.0) - variaveis
        variaveis_by_month[month] = variaveis
        mc_by_month[month] = mc
        margem_by_month[month] = (mc / receita[month] * 100.0) if receita.get(month, 0.0) > 0 else 0.0

    total_receita = sum(receita.get(m, 0.0) for m in months)
    total_taxes = sum(taxes.get(m, 0.0) for m in months)
    total_custos = sum(custos.get(m, 0.0) for m in months)
    total_despesas = sum(despesas_variaveis.get(m, 0.0) for m in months)
    total_variaveis = total_taxes + total_custos + total_despesas
    total_mc = total_receita - total_variaveis
    total_margem = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

    header_note = (
        "Fonte: Sienge Bulk Data direto da API, sem uso do banco local. "
        f"Contagens: {json.dumps(counts, ensure_ascii=False)}"
    )

    margem_lines = [
        "MARGEM DE CONTRIBUICAO (%) (SIENGE BULK DATA)",
        "Formula: Margem = MC / Receita Bruta",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        header_note,
        "",
        "MES;MARGEM",
    ]
    margem_lines.extend(f"{m};{format_percent(margem_by_month[m])}" for m in months)
    margem_lines.extend(["", f"TOTAL;{format_percent(total_margem)}"])

    mc_lines = [
        "MC GERAL (SIENGE BULK DATA)",
        "Formula: MC = Receita Bruta - (Custos Variaveis + Despesas Variaveis)",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        header_note,
        "",
        "MES;RECEITA_BRUTA;IMPOSTOS_DEDUCOES;CUSTOS_VARIAVEIS;DESPESAS_VARIAVEIS;TOTAL_VARIAVEIS;MC",
    ]
    mc_lines.extend(
        (
            f"{m};{format_brl(receita.get(m, 0.0))};{format_brl(taxes.get(m, 0.0))};"
            f"{format_brl(custos.get(m, 0.0))};{format_brl(despesas_variaveis.get(m, 0.0))};"
            f"{format_brl(variaveis_by_month[m])};{format_brl(mc_by_month[m])}"
        )
        for m in months
    )
    mc_lines.extend(
        [
            "",
            (
                f"TOTAL;{format_brl(total_receita)};{format_brl(total_taxes)};"
                f"{format_brl(total_custos)};{format_brl(total_despesas)};"
                f"{format_brl(total_variaveis)};{format_brl(total_mc)}"
            ),
        ]
    )

    (output_dir / "Margem.txt").write_text("\n".join(margem_lines) + "\n", encoding="utf-8")
    (output_dir / "MC_geral.txt").write_text("\n".join(mc_lines) + "\n", encoding="utf-8")

    print(f"Arquivos gerados: {output_dir / 'Margem.txt'} | {output_dir / 'MC_geral.txt'}")
    print(json.dumps(counts, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta Margem.txt e MC_geral.txt via Sienge Bulk Data, sem banco.")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--chunk-months", type=int, default=12)
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
