from __future__ import annotations

import argparse
import asyncio
import calendar
import csv
import re
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


def iter_chunks(start: date, end: date, chunk_months: int) -> list[MonthRange]:
    chunks: list[MonthRange] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        finish = month_end(add_months(cursor, chunk_months - 1))
        if finish > end:
            finish = end
        chunks.append(MonthRange(cursor, finish))
        cursor = add_months(cursor, chunk_months)
    return chunks


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


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def format_percent(value: float) -> str:
    return f"{value:.2f}".replace(".", ",") + "%"


def fix_mojibake(value: Any) -> str:
    text = str(value or "").strip()
    for _ in range(2):
        if not any(marker in text for marker in ("Ã", "Â", "�")):
            break
        try:
            repaired = text.encode("latin1").decode("utf-8")
        except UnicodeError:
            break
        if repaired == text:
            break
        text = repaired
    return text


def amount_abs(item: dict[str, Any]) -> float:
    return abs(
        safe_float(
            item.get("rawValue")
            or item.get("amount")
            or item.get("valor")
            or item.get("value")
            or item.get("totalInvoiceAmount")
            or item.get("totalAmount")
            or 0
        )
    )


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


def first_text(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = fix_mojibake(item.get(key))
        if value and value.lower() not in {"none", "null", "undefined"}:
            return value
    return ""


def company_id(item: dict[str, Any]) -> str:
    return first_text(item, ["companyId", "company_id", "idCompany", "empresaId"])


def building_id(item: dict[str, Any]) -> str:
    return first_text(
        item,
        [
            "buildingId",
            "building_id",
            "buildingCode",
            "building_code",
            "enterpriseId",
            "enterprise_id",
            "idObra",
            "codigoObra",
            "codigoVisivelObra",
            "codigoVisivel",
            "obraId",
            "obra_id",
        ],
    )


def normalize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "-", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:140] or "SEM_NOME"


def convert_txt_to_csv(txt_dir: Path, names: list[str]) -> list[Path]:
    written: list[Path] = []
    for name in names:
        src = txt_dir / name
        if not src.exists():
            continue
        dst = src.with_suffix(".csv")
        lines = src.read_text(encoding="utf-8-sig").splitlines()
        table_lines = [line for line in lines if ";" in line]
        with dst.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            for line in table_lines:
                writer.writerow(line.split(";"))
        written.append(dst)
    return written


async def fetch_live_history(start: date, end: date, chunk_months: int, verbose: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    companies, buildings = await asyncio.gather(client.fetch_empresas(), client.fetch_obras())
    statements: list[dict[str, Any]] = []
    bills: list[dict[str, Any]] = []
    for chunk in iter_chunks(start, end, chunk_months):
        if verbose:
            print(f"[sienge-live] {chunk.start.isoformat()} -> {chunk.end.isoformat()}")
        chunk_statements, chunk_bills = await asyncio.gather(
            client.fetch_receber_range(chunk.start.isoformat(), chunk.end.isoformat()),
            client.fetch_financeiro_range(chunk.start.isoformat(), chunk.end.isoformat()),
        )
        statements.extend(x for x in chunk_statements if isinstance(x, dict))
        bills.extend(x for x in chunk_bills if isinstance(x, dict))
        if verbose:
            print(f"  accounts-statements: {len(chunk_statements)} | bills: {len(chunk_bills)}")
    return companies, buildings, statements, bills


def build_maps(companies: list[dict[str, Any]], buildings: list[dict[str, Any]]) -> tuple[dict[str, str], dict[str, dict[str, str]], dict[str, str]]:
    company_names: dict[str, str] = {}
    for company in companies:
        cid = first_text(company, ["id", "companyId", "code"])
        name = first_text(company, ["name", "nome", "tradeName", "fantasyName"]) or f"Empresa {cid}"
        if cid:
            company_names[cid] = name

    building_info: dict[str, dict[str, str]] = {}
    alias_to_building: dict[str, str] = {}
    for building in buildings:
        bid = first_text(building, ["id", "code", "codigoVisivel", "codigo"])
        name = first_text(building, ["name", "nome", "enterpriseName"]) or f"Obra {bid}"
        cid = company_id(building)
        if not bid:
            continue
        building_info[bid] = {"id": bid, "name": name, "company_id": cid}
        for alias_key in ["id", "code", "codigoVisivel", "codigo", "buildingId", "enterpriseId"]:
            alias = first_text(building, [alias_key])
            if alias:
                alias_to_building[alias] = bid
    return company_names, building_info, alias_to_building


def resolve_building(raw: str, alias_to_building: dict[str, str]) -> str:
    if not raw:
        return "0"
    return alias_to_building.get(raw) or raw


def aggregate_by_building(
    statements: list[dict[str, Any]],
    bills: list[dict[str, Any]],
    building_info: dict[str, dict[str, str]],
    alias_to_building: dict[str, str],
) -> dict[str, dict[str, float]]:
    totals: dict[str, dict[str, float]] = defaultdict(lambda: {"receita": 0.0, "custos": 0.0, "despesas": 0.0})

    for item in statements:
        if should_ignore_statement(item):
            continue
        if item_date(item) is None:
            continue
        amount = amount_abs(item)
        if amount <= 0:
            continue
        bid = resolve_building(building_id(item), alias_to_building)
        if is_expense_statement(item):
            totals[bid]["despesas"] += amount
        else:
            totals[bid]["receita"] += amount

    for item in bills:
        if should_ignore_statement(item):
            continue
        if item_date(item) is None:
            continue
        amount = amount_abs(item)
        if amount <= 0:
            continue
        bid = resolve_building(building_id(item), alias_to_building)
        totals[bid]["custos"] += amount

    for bid in building_info:
        totals.setdefault(bid, {"receita": 0.0, "custos": 0.0, "despesas": 0.0})
    return totals


def write_company_files(
    out_dir: Path,
    company_names: dict[str, str],
    building_info: dict[str, dict[str, str]],
    totals: dict[str, dict[str, float]],
    generated_at: date,
) -> list[Path]:
    company_dir = out_dir / "empresas"
    company_dir.mkdir(parents=True, exist_ok=True)

    buildings_by_company: dict[str, list[dict[str, str]]] = defaultdict(list)
    for bid, info in building_info.items():
        buildings_by_company[info.get("company_id") or "0"].append(info)

    # Inclui registros que vieram dos lancamentos mas nao existem em /enterprises.
    for bid in totals:
        if bid not in building_info:
            building_info[bid] = {"id": bid, "name": "Sem obra" if bid == "0" else f"Obra {bid}", "company_id": "0"}
            buildings_by_company["0"].append(building_info[bid])

    written: list[Path] = []
    for cid, buildings in sorted(buildings_by_company.items(), key=lambda kv: company_names.get(kv[0], kv[0])):
        company_name = company_names.get(cid) or ("Sem empresa identificada" if cid == "0" else f"Empresa {cid}")
        rows = []
        for building in buildings:
            bid = building["id"]
            row = totals.get(bid, {"receita": 0.0, "custos": 0.0, "despesas": 0.0})
            receita = row["receita"]
            variaveis = row["custos"] + row["despesas"]
            mc = receita - variaveis
            margem = (mc / receita * 100.0) if receita > 0 else 0.0
            rows.append((building["name"], bid, receita, row["custos"], row["despesas"], variaveis, mc, margem))
        rows.sort(key=lambda r: (-r[2], r[0]))

        total_receita = sum(r[2] for r in rows)
        total_custos = sum(r[3] for r in rows)
        total_despesas = sum(r[4] for r in rows)
        total_variaveis = total_custos + total_despesas
        total_mc = total_receita - total_variaveis
        total_margem = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

        lines = [
            f"EMPRESA: {company_name}",
            f"ID_EMPRESA: {cid}",
            "Fonte: Sienge API ao vivo, sem uso do banco local.",
            "Valores em Real (BRL). Margem = MC / Receita. MC = Receita - (Custos + Despesas).",
            f"Gerado em: {generated_at.isoformat()}",
            "",
            "OBRA;ID_OBRA;RECEITA;CUSTOS;DESPESAS;TOTAL_VARIAVEIS;MC;MARGEM",
        ]
        lines.extend(
            f"{name};{bid};{format_brl(receita)};{format_brl(custos)};{format_brl(despesas)};{format_brl(variaveis)};{format_brl(mc)};{format_percent(margem)}"
            for name, bid, receita, custos, despesas, variaveis, mc, margem in rows
        )
        lines.extend(
            [
                "",
                f"TOTAL;{cid};{format_brl(total_receita)};{format_brl(total_custos)};{format_brl(total_despesas)};{format_brl(total_variaveis)};{format_brl(total_mc)};{format_percent(total_margem)}",
            ]
        )
        path = company_dir / f"{normalize_filename(company_name)}.txt"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        written.append(path)

    return written


async def run(args: argparse.Namespace) -> None:
    txt_dir = Path(args.txt_dir)
    csv_written = convert_txt_to_csv(
        txt_dir,
        ["Margem.txt", "MC_geral.txt", "receita_total.txt", "Margem2.txt", "MC_geral2.txt"],
    )

    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    companies, buildings, statements, bills = await fetch_live_history(start, end, args.chunk_months, args.verbose)
    company_names, building_info, alias_to_building = build_maps(companies, buildings)
    totals = aggregate_by_building(statements, bills, building_info, alias_to_building)
    company_files = write_company_files(txt_dir, company_names, building_info, totals, end)

    print("CSV gerados:")
    for path in csv_written:
        print(f"  {path}")
    print("Arquivos de empresas gerados:")
    for path in company_files:
        print(f"  {path}")
    print(f"Registros Sienge ao vivo: empresas={len(companies)} obras={len(buildings)} accounts-statements={len(statements)} bills={len(bills)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Converte TXT para CSV e gera arquivos por empresa/obra direto do Sienge.")
    parser.add_argument("--txt-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt"))
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--chunk-months", type=int, default=18)
    parser.add_argument("--verbose", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
