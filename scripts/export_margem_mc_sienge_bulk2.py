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


@dataclass
class BulkResult:
    endpoint: str
    rows: list[dict[str, Any]]
    identifier: str | None = None


class RateGate:
    def __init__(self, min_interval: float = 3.4) -> None:
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            delay = self.min_interval - (now - self._last)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = loop.time()


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


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def add_months(d: date, delta: int) -> date:
    y = d.year + (d.month - 1 + delta) // 12
    m = (d.month - 1 + delta) % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


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


def extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


class BulkClient:
    def __init__(self, gate: RateGate) -> None:
        self.sienge = SiengeClient()
        if not getattr(self.sienge, "is_configured", False):
            raise SystemExit("SIENGE nao esta configurado no .env.")
        self.base = self.sienge.base_url.rstrip("/") + "/public/api/bulk-data/v1"
        self.gate = gate
        self.headers, self.auth = self.sienge._auth_variants()[0]

    async def get_json(self, http: httpx.AsyncClient, path: str, params: dict[str, Any] | None = None) -> Any:
        url = self.base + path
        for attempt in range(8):
            await self.gate.wait()
            response = await http.get(url, headers=self.headers, auth=self.auth, params=params or {})
            if response.status_code == 429:
                remaining_day = response.headers.get("x-ratelimit-remaining-day") or response.headers.get("ratelimit-remaining")
                retry_after = response.headers.get("retry-after") or response.headers.get("ratelimit-reset") or "desconhecido"
                if str(remaining_day).strip() == "0":
                    raise RuntimeError(f"Bulk Data sem limite diario restante. retry-after={retry_after}s")
                wait_s = 75 + (attempt * 10)
                print(f"[rate-limit] aguardando {wait_s}s...")
                await asyncio.sleep(wait_s)
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"{path} HTTP {response.status_code}: {response.text[:500]}")
            return response.json()
        raise RuntimeError(f"{path} excedeu tentativas por rate limit.")

    async def fetch_async(self, http: httpx.AsyncClient, path: str, params: dict[str, Any]) -> BulkResult:
        start_payload = await self.get_json(
            http,
            path,
            {
                **params,
                "_async": "true",
                "_asyncChunkMaxSize": 4096,
            },
        )
        identifier = start_payload.get("identifier") if isinstance(start_payload, dict) else None
        if not identifier:
            return BulkResult(path, extract_rows(start_payload))

        for _ in range(180):
            status = await self.get_json(http, f"/async/{identifier}")
            state = str(status.get("status") or "")
            if state == "Finished":
                rows: list[dict[str, Any]] = []
                chunks = int(status.get("chunks") or 1)
                for chunk in range(1, chunks + 1):
                    result = await self.get_json(http, f"/async/{identifier}/result/{chunk}")
                    rows.extend(extract_rows(result))
                return BulkResult(path, rows, identifier)
            if state == "Failed":
                raise RuntimeError(f"{path} async failed: {status}")
            await asyncio.sleep(5)
        raise TimeoutError(f"{path} async timeout: {identifier}")


def cache_file(cache_dir: Path, name: str) -> Path:
    return cache_dir / f"{name}.json"


def load_cache(cache_dir: Path, name: str) -> list[dict[str, Any]] | None:
    path = cache_file(cache_dir, name)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return [x for x in payload["data"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return None


def save_cache(cache_dir: Path, name: str, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_file(cache_dir, name)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"meta": meta, "data": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def load_all_cached(cache_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    invoices: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    if not cache_dir.exists():
        return invoices, outcomes
    for path in sorted(cache_dir.glob("invoice_company_*.json")):
        rows = load_cache(cache_dir, path.stem)
        if rows:
            invoices.extend(rows)
    rows = load_cache(cache_dir, "outcome_all")
    if rows:
        outcomes.extend(rows)
    return invoices, outcomes


def write_partial_from_cache(cache_dir: Path, output_dir: Path, months: list[str], generated_at: date, correction_indexer_id: int, errors: list[dict[str, Any]] | None = None) -> None:
    invoices, outcomes = load_all_cached(cache_dir)
    receita, impostos, custos_despesas = aggregate(invoices, outcomes)
    counts = {
        "invoice-itens": len(invoices),
        "outcome": len(outcomes),
        "errors": len(errors or []),
        "correctionIndexerId": correction_indexer_id,
        "partial": True,
        "cacheDir": str(cache_dir),
    }
    write_files(output_dir, months, receita, impostos, custos_despesas, generated_at, counts)


async def fetch_all(
    start: date,
    end: date,
    correction_indexer_id: int,
    verbose: bool,
    cache_dir: Path,
    output_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    gate = RateGate()
    bulk = BulkClient(gate)
    companies = await bulk.sienge.fetch_empresas()
    company_ids = [c.get("id") for c in companies if c.get("id") is not None]
    months = iter_months(start, end)
    if verbose:
        print(f"Empresas: {company_ids}")

    timeout = httpx.Timeout(600.0)
    invoice_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=timeout) as http:
        for company_id in company_ids:
            cache_name = f"invoice_company_{company_id}"
            cached = load_cache(cache_dir, cache_name)
            if cached is not None:
                invoice_rows.extend(cached)
                if verbose:
                    print(f"[cache] {cache_name}: {len(cached)} linhas")
                write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)
                continue

            try:
                if verbose:
                    print(f"[bulk] invoice-itens companyId={company_id}")
                result = await bulk.fetch_async(
                    http,
                    "/invoice-itens",
                    {
                        "companyId": company_id,
                        "startDate": start.isoformat(),
                        "endDate": end.isoformat(),
                        "showCostCenterId": "S",
                    },
                )
                invoice_rows.extend(result.rows)
                save_cache(
                    cache_dir,
                    cache_name,
                    result.rows,
                    {
                        "endpoint": "invoice-itens",
                        "companyId": company_id,
                        "identifier": result.identifier,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    },
                )
                write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)
                if verbose:
                    print(f"  linhas: {len(result.rows)}")
            except Exception as exc:
                errors.append({"endpoint": "invoice-itens", "companyId": company_id, "error": str(exc)})
                print(f"[erro] invoice-itens companyId={company_id}: {exc}")
                write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)

        cached_outcome = load_cache(cache_dir, "outcome_all")
        if cached_outcome is not None:
            outcome_rows.extend(cached_outcome)
            if verbose:
                print(f"[cache] outcome_all: {len(cached_outcome)} linhas")
            write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)
            return invoice_rows, outcome_rows, errors

        try:
            if verbose:
                print("[bulk] outcome")
            result = await bulk.fetch_async(
                http,
                "/outcome",
                {
                    "startDate": start.isoformat(),
                    "endDate": end.isoformat(),
                    "selectionType": "P",
                    "correctionIndexerId": correction_indexer_id,
                    "correctionDate": end.isoformat(),
                    "withBankMovements": "true",
                },
            )
            outcome_rows.extend(result.rows)
            save_cache(
                cache_dir,
                "outcome_all",
                result.rows,
                {
                    "endpoint": "outcome",
                    "identifier": result.identifier,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "selectionType": "P",
                    "correctionIndexerId": correction_indexer_id,
                },
            )
            write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)
            if verbose:
                print(f"  linhas: {len(result.rows)}")
        except Exception as exc:
            errors.append({"endpoint": "outcome", "error": str(exc)})
            print(f"[erro] outcome: {exc}")
            write_partial_from_cache(cache_dir, output_dir, months, end, correction_indexer_id, errors)

    return invoice_rows, outcome_rows, errors


def invoice_tax_value(row: dict[str, Any]) -> float:
    fields = [
        "pisValue",
        "cofinsValue",
        "icmsValue",
        "icmsStValue",
        "ipiValue",
        "issValue",
        "irValue",
        "csllValue",
        "inssValue",
    ]
    return sum(abs(safe_float(row.get(f))) for f in fields)


def aggregate(invoices: list[dict[str, Any]], outcomes: list[dict[str, Any]]) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    receita: dict[str, float] = defaultdict(float)
    impostos: dict[str, float] = defaultdict(float)
    custos_despesas: dict[str, float] = defaultdict(float)

    for row in invoices:
        d = parse_date(row.get("entryExitDate") or row.get("issueDate"))
        if d is None:
            continue
        entry_exit_type = row.get("entryExitType")
        # 0 = saida, 1 = entrada, segundo YAML do Sienge.
        if str(entry_exit_type) not in {"0", "0.0", ""} and entry_exit_type is not None:
            continue
        month = month_key(d)
        receita[month] += abs(safe_float(row.get("totalItemValue")))
        impostos[month] += invoice_tax_value(row)

    for row in outcomes:
        payments = row.get("payments")
        if isinstance(payments, list) and payments:
            for payment in payments:
                if not isinstance(payment, dict):
                    continue
                d = parse_date(payment.get("paymentDate") or payment.get("calculationDate"))
                if d is None:
                    d = parse_date(row.get("dueDate") or row.get("billDate") or row.get("issueDate"))
                if d is None:
                    continue
                amount = safe_float(payment.get("netAmount") or payment.get("grossAmount"))
                custos_despesas[month_key(d)] += abs(amount)
        else:
            d = parse_date(row.get("dueDate") or row.get("billDate") or row.get("issueDate"))
            if d is None:
                continue
            amount = safe_float(row.get("originalAmount") or row.get("correctedBalanceAmount") or row.get("balanceAmount"))
            custos_despesas[month_key(d)] += abs(amount)

    return dict(receita), dict(impostos), dict(custos_despesas)


def format_brl(value: float) -> str:
    return "R$ " + f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_percent(value: float) -> str:
    return f"{value:.2f}".replace(".", ",") + "%"


def write_files(output_dir: Path, months: list[str], receita: dict[str, float], impostos: dict[str, float], custos_despesas: dict[str, float], generated_at: date, counts: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    mc: dict[str, float] = {}
    margem: dict[str, float] = {}
    variaveis: dict[str, float] = {}
    for m in months:
        variaveis[m] = impostos.get(m, 0.0) + custos_despesas.get(m, 0.0)
        mc[m] = receita.get(m, 0.0) - variaveis[m]
        margem[m] = (mc[m] / receita[m] * 100.0) if receita.get(m, 0.0) > 0 else 0.0

    total_receita = sum(receita.get(m, 0.0) for m in months)
    total_impostos = sum(impostos.get(m, 0.0) for m in months)
    total_custos = sum(custos_despesas.get(m, 0.0) for m in months)
    total_variaveis = total_impostos + total_custos
    total_mc = total_receita - total_variaveis
    total_margem = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

    source = (
        "Fonte: Sienge Bulk Data direto da API, sem uso do banco local. "
        "Receita Bruta: /invoice-itens totalItemValue de notas de saida. "
        "Impostos/Deducoes: campos tributarios do /invoice-itens. "
        "Custos/Despesas Variaveis: /outcome selectionType=P, valores pagos. "
        f"Contagens: {counts}."
    )

    margem_lines = [
        "MARGEM DE CONTRIBUICAO 2 (%) (SIENGE BULK DATA)",
        "Formula: Margem = MC / Receita Bruta",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        source,
        "",
        "MES;MARGEM",
    ]
    margem_lines.extend(f"{m};{format_percent(margem[m])}" for m in months)
    margem_lines.extend(["", f"TOTAL;{format_percent(total_margem)}"])

    mc_lines = [
        "MC GERAL 2 (SIENGE BULK DATA)",
        "Formula: MC = Receita Bruta - (Custos Variaveis + Despesas Variaveis)",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        source,
        "",
        "MES;RECEITA_BRUTA;IMPOSTOS_DEDUCOES;CUSTOS_DESPESAS_VARIAVEIS;TOTAL_VARIAVEIS;MC",
    ]
    mc_lines.extend(
        f"{m};{format_brl(receita.get(m, 0.0))};{format_brl(impostos.get(m, 0.0))};{format_brl(custos_despesas.get(m, 0.0))};{format_brl(variaveis[m])};{format_brl(mc[m])}"
        for m in months
    )
    mc_lines.extend(
        [
            "",
            f"TOTAL;{format_brl(total_receita)};{format_brl(total_impostos)};{format_brl(total_custos)};{format_brl(total_variaveis)};{format_brl(total_mc)}",
        ]
    )

    (output_dir / "Margem2.txt").write_text("\n".join(margem_lines) + "\n", encoding="utf-8")
    (output_dir / "MC_geral2.txt").write_text("\n".join(mc_lines) + "\n", encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")
    output_dir = Path(args.output_dir)
    cache_dir = Path(args.cache_dir) if args.cache_dir else output_dir / ".bulk2_cache"
    invoices, outcomes, errors = await fetch_all(
        start,
        end,
        args.correction_indexer_id,
        args.verbose,
        cache_dir,
        output_dir,
    )
    receita, impostos, custos_despesas = aggregate(invoices, outcomes)
    months = iter_months(start, end)
    counts = {
        "invoice-itens": len(invoices),
        "outcome": len(outcomes),
        "errors": len(errors),
        "correctionIndexerId": args.correction_indexer_id,
    }
    write_files(output_dir, months, receita, impostos, custos_despesas, end, counts)
    print(f"Arquivos gerados: {output_dir / 'Margem2.txt'} | {output_dir / 'MC_geral2.txt'}")
    print(f"Cache incremental: {cache_dir}")
    print(counts)
    if errors:
        print("Erros:", errors[:10])


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera Margem2.txt e MC_geral2.txt via Sienge Bulk Data.")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--correction-indexer-id", type=int, default=0)
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt"))
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--verbose", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
