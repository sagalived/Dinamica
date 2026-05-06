from __future__ import annotations

import argparse
import asyncio
import calendar
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

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
    chunks: list[MonthRange] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        end_month = month_end(add_months(cursor, chunk_months - 1))
        if end_month > end:
            end_month = end
        chunks.append(MonthRange(cursor, end_month))
        cursor = add_months(cursor, chunk_months)
    return chunks


def iter_months(start: date, end: date) -> list[str]:
    months: list[str] = []
    cursor = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
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


def amount_abs(item: dict[str, Any]) -> float:
    value = (
        item.get("rawValue")
        or item.get("amount")
        or item.get("valor")
        or item.get("value")
        or item.get("totalInvoiceAmount")
        or item.get("totalAmount")
        or 0
    )
    return abs(safe_float(value))


def item_date(item: dict[str, Any]) -> date | None:
    return parse_date(
        item.get("dataVencimento")
        or item.get("dueDate")
        or item.get("data")
        or item.get("date")
        or item.get("operationDate")
        or item.get("paymentDate")
        or item.get("issueDate")
        or item.get("dataEmissao")
    )


def should_ignore_statement(item: dict[str, Any]) -> bool:
    statement_type = str(item.get("statementType") or item.get("operationType") or "").strip().lower()
    origin = str(item.get("statementOrigin") or item.get("origin") or "").strip().lower()
    return "transf" in statement_type or "transfer" in statement_type or "saque" in statement_type or origin == "bc"


def is_expense_statement(item: dict[str, Any]) -> bool:
    typ = str(item.get("type") or "").strip().lower()
    if typ == "expense":
        return True
    if typ == "income":
        return False
    raw_value = item.get("rawValue")
    return raw_value is not None and safe_float(raw_value) < 0


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def format_percent(value: float) -> str:
    return f"{value:.2f}".replace(".", ",") + "%"


async def fetch_history(start: date, end: date, chunk_months: int, verbose: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    statements: list[dict[str, Any]] = []
    bills: list[dict[str, Any]] = []
    for chunk in iter_chunks(start, end, chunk_months):
        if verbose:
            print(f"[sienge-rest] {chunk.start.isoformat()} -> {chunk.end.isoformat()}")
        chunk_statements, chunk_bills = await asyncio.gather(
            client.fetch_receber_range(chunk.start.isoformat(), chunk.end.isoformat()),
            client.fetch_financeiro_range(chunk.start.isoformat(), chunk.end.isoformat()),
        )
        statements.extend(x for x in chunk_statements if isinstance(x, dict))
        bills.extend(x for x in chunk_bills if isinstance(x, dict))
        if verbose:
            print(f"  accounts-statements: {len(chunk_statements)} | bills: {len(chunk_bills)}")
    return statements, bills


def aggregate(statements: list[dict[str, Any]], bills: list[dict[str, Any]]) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    receita: dict[str, float] = defaultdict(float)
    despesas_variaveis: dict[str, float] = defaultdict(float)
    custos_variaveis: dict[str, float] = defaultdict(float)

    for item in statements:
        if should_ignore_statement(item):
            continue
        d = item_date(item)
        amount = amount_abs(item)
        if d is None or amount <= 0:
            continue
        if is_expense_statement(item):
            despesas_variaveis[month_key(d)] += amount
        else:
            receita[month_key(d)] += amount

    for item in bills:
        if should_ignore_statement(item):
            continue
        d = item_date(item)
        amount = amount_abs(item)
        if d is None or amount <= 0:
            continue
        custos_variaveis[month_key(d)] += amount

    return receita, custos_variaveis, despesas_variaveis


def write_outputs(
    output_dir: Path,
    months: list[str],
    receita: dict[str, float],
    custos_variaveis: dict[str, float],
    despesas_variaveis: dict[str, float],
    generated_at: date,
    counts: dict[str, int],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    mc_by_month: dict[str, float] = {}
    margem_by_month: dict[str, float] = {}
    total_variaveis_by_month: dict[str, float] = {}
    for m in months:
        total_variaveis = custos_variaveis.get(m, 0.0) + despesas_variaveis.get(m, 0.0)
        mc = receita.get(m, 0.0) - total_variaveis
        total_variaveis_by_month[m] = total_variaveis
        mc_by_month[m] = mc
        margem_by_month[m] = (mc / receita[m] * 100.0) if receita.get(m, 0.0) > 0 else 0.0

    total_receita = sum(receita.get(m, 0.0) for m in months)
    total_custos = sum(custos_variaveis.get(m, 0.0) for m in months)
    total_despesas = sum(despesas_variaveis.get(m, 0.0) for m in months)
    total_variaveis = total_custos + total_despesas
    total_mc = total_receita - total_variaveis
    total_margem = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

    source_note = (
        "Fonte: Sienge REST direto da API, sem uso do banco local. "
        "Receita Bruta aproximada por /accounts-statements tipo receita; "
        "Custos Variaveis por /bills; Despesas Variaveis por /accounts-statements tipo expense. "
        f"Registros: accounts-statements={counts['statements']}; bills={counts['bills']}."
    )

    margem_lines = [
        "MARGEM DE CONTRIBUICAO (%) (SIENGE REST)",
        "Formula: Margem = MC / Receita Bruta",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        source_note,
        "",
        "MES;MARGEM",
    ]
    margem_lines.extend(f"{m};{format_percent(margem_by_month[m])}" for m in months)
    margem_lines.extend(["", f"TOTAL;{format_percent(total_margem)}"])

    mc_lines = [
        "MC GERAL (SIENGE REST)",
        "Formula: MC = Receita Bruta - (Custos Variaveis + Despesas Variaveis)",
        f"Periodo: {months[0]} ate {months[-1]}",
        f"Gerado em: {generated_at.isoformat()}",
        source_note,
        "",
        "MES;RECEITA_BRUTA;CUSTOS_VARIAVEIS;DESPESAS_VARIAVEIS;TOTAL_VARIAVEIS;MC",
    ]
    mc_lines.extend(
        (
            f"{m};{format_brl(receita.get(m, 0.0))};{format_brl(custos_variaveis.get(m, 0.0))};"
            f"{format_brl(despesas_variaveis.get(m, 0.0))};{format_brl(total_variaveis_by_month[m])};"
            f"{format_brl(mc_by_month[m])}"
        )
        for m in months
    )
    mc_lines.extend(
        [
            "",
            (
                f"TOTAL;{format_brl(total_receita)};{format_brl(total_custos)};"
                f"{format_brl(total_despesas)};{format_brl(total_variaveis)};{format_brl(total_mc)}"
            ),
        ]
    )

    (output_dir / "Margem.txt").write_text("\n".join(margem_lines) + "\n", encoding="utf-8")
    (output_dir / "MC_geral.txt").write_text("\n".join(mc_lines) + "\n", encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")
    statements, bills = await fetch_history(start, end, args.chunk_months, args.verbose)
    receita, custos_variaveis, despesas_variaveis = aggregate(statements, bills)
    months = iter_months(start, end)
    write_outputs(
        Path(args.output_dir),
        months,
        receita,
        custos_variaveis,
        despesas_variaveis,
        end,
        {"statements": len(statements), "bills": len(bills)},
    )
    print(f"Arquivos gerados: {Path(args.output_dir) / 'Margem.txt'} | {Path(args.output_dir) / 'MC_geral.txt'}")
    print(f"Registros: accounts-statements={len(statements)} bills={len(bills)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta Margem.txt e MC_geral.txt direto do Sienge REST, sem banco.")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--chunk-months", type=int, default=18)
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt"))
    parser.add_argument("--verbose", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
