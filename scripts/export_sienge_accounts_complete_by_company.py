from __future__ import annotations

import argparse
import asyncio
import csv
import re
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
class WorkInfo:
    id: str
    name: str
    company_id: str


@dataclass(frozen=True)
class AccountRow:
    company_id: str
    company_name: str
    person: str
    document: str
    launch_date: date | None
    quantity: str
    index: str
    days: str
    due_value: float
    addition: float
    discount: float
    total: float


HEADER = [
    "Credor",
    "Documento",
    "Lançamento",
    "Qt.",
    "Ind.",
    "Dias",
    "Valor no vencto",
    "Acréscimo",
    "Desconto",
    "Total",
]


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
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
    return "R$ " + f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_date_br(value: date | None) -> str:
    return value.strftime("%d/%m/%Y") if value else ""


def fix_mojibake(value: Any) -> str:
    text = str(value or "").strip()
    for _ in range(3):
        if not any(marker in text for marker in ("Ãƒ", "Ã‚", "ï¿½", "Ã¯")):
            break
        repaired = None
        for encoding in ("cp1252", "latin1"):
            try:
                repaired = text.encode(encoding).decode("utf-8")
                break
            except UnicodeError:
                continue
        if not repaired or repaired == text:
            break
        text = repaired
    return text


def id_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    raw = str(value).strip()
    if raw.endswith(".0") and raw[:-2].isdigit():
        return raw[:-2]
    return raw


def clean_cell(value: Any) -> str:
    return re.sub(r"\s+", " ", fix_mojibake(value)).strip()


def first_text(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = clean_cell(item.get(key))
        if value and value.lower() not in {"none", "null", "undefined"}:
            return value
    return ""


def normalize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "-", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:140] or "SEM_NOME"


def extract_rows(payload: Any) -> list[dict[str, Any]]:
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


async def fetch_pages(client: SiengeClient, endpoint: str, params: dict[str, Any] | None = None, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as http:
        while True:
            page_params = {**(params or {}), "limit": limit, "offset": offset}
            payload = await client._get_json_via_client(http, endpoint, page_params)
            page = extract_rows(payload)
            if not page:
                break
            rows.extend(page)
            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            print(f"[sienge] {endpoint} offset={offset} +{len(page)} total={len(rows)}", flush=True)
            offset += len(page)
            if len(page) < limit or (isinstance(count, int) and offset >= count):
                break
    return rows


def build_name_map(items: list[dict[str, Any]], id_keys: list[str], name_keys: list[str], fallback: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        item_id = ""
        for key in id_keys:
            item_id = id_text(item.get(key))
            if item_id:
                break
        name = first_text(item, name_keys) or f"{fallback} {item_id}"
        if item_id:
            out[item_id] = name
    return out


def build_works_by_company(cost_centers: list[dict[str, Any]], enterprises: list[dict[str, Any]]) -> dict[str, list[WorkInfo]]:
    grouped: dict[str, dict[str, WorkInfo]] = defaultdict(dict)
    for item in [*cost_centers, *enterprises]:
        work_id = id_text(item.get("id") or item.get("costCenterId") or item.get("code") or item.get("codigo"))
        company_id = id_text(item.get("idCompany") or item.get("companyId"))
        name = first_text(item, ["name", "costCenterName", "nome", "enterpriseName"]) or f"Obra {work_id}"
        if work_id and company_id:
            grouped[company_id][work_id] = WorkInfo(work_id, name, company_id)
    return {company_id: sorted(values.values(), key=lambda work: (work.name, work.id)) for company_id, values in grouped.items()}


def document_from_parts(*parts: Any) -> str:
    return " ".join(part for part in (clean_cell(value) for value in parts) if part).strip()


def payable_rows(
    bills: list[dict[str, Any]],
    company_names: dict[str, str],
    creditor_names: dict[str, str],
) -> dict[str, list[AccountRow]]:
    grouped: dict[str, list[AccountRow]] = defaultdict(list)
    for bill in bills:
        company_id = id_text(bill.get("debtorId") or bill.get("companyId") or bill.get("idCompany")) or "0"
        creditor_id = id_text(bill.get("creditorId") or bill.get("supplierId") or bill.get("idCredor"))
        due_value = safe_float(bill.get("totalInvoiceAmount") or bill.get("amount") or bill.get("value"))
        discount = safe_float(bill.get("discount") or bill.get("discountAmount"))
        addition = safe_float(bill.get("addition") or bill.get("additionAmount"))
        grouped[company_id].append(
            AccountRow(
                company_id=company_id,
                company_name=company_names.get(company_id, f"Empresa {company_id}" if company_id != "0" else "Sem empresa identificada"),
                person=creditor_names.get(creditor_id)
                or first_text(bill, ["creditorName", "nomeCredor", "supplierName", "nomeFornecedor"])
                or (f"Credor {creditor_id}" if creditor_id else "Credor sem nome"),
                document=document_from_parts(bill.get("documentIdentificationId"), bill.get("documentNumber")) or id_text(bill.get("id")),
                launch_date=parse_date(bill.get("issueDate") or bill.get("billDate") or bill.get("registeredDate")),
                quantity=id_text(bill.get("installmentsNumber")) or "1",
                index="",
                days="",
                due_value=due_value,
                addition=addition,
                discount=discount,
                total=due_value + addition - discount,
            )
        )
    return grouped


def receivable_rows(
    titles: list[dict[str, Any]],
    company_names: dict[str, str],
    customer_names: dict[str, str],
) -> dict[str, list[AccountRow]]:
    grouped: dict[str, list[AccountRow]] = defaultdict(list)
    for title in titles:
        company_id = id_text(title.get("companyId") or title.get("idCompany")) or "0"
        customer_id = id_text(title.get("customerId") or title.get("clientId"))
        due_value = safe_float(title.get("receivableBillValue") or title.get("value") or title.get("totalValue"))
        discount = safe_float(title.get("discount") or title.get("discountAmount"))
        addition = safe_float(title.get("addition") or title.get("additionAmount"))
        grouped[company_id].append(
            AccountRow(
                company_id=company_id,
                company_name=company_names.get(company_id, f"Empresa {company_id}" if company_id != "0" else "Sem empresa identificada"),
                person=customer_names.get(customer_id)
                or first_text(title, ["customerName", "clientName", "nomeCliente", "name"])
                or (f"Cliente {customer_id}" if customer_id else "Cliente sem nome"),
                document=document_from_parts(title.get("documentId"), title.get("documentNumber")) or id_text(title.get("receivableBillId")),
                launch_date=parse_date(title.get("issueDate") or title.get("billDate")),
                quantity=id_text(title.get("installmentsNumber")) or "1",
                index="",
                days="",
                due_value=due_value,
                addition=addition,
                discount=discount,
                total=due_value + addition - discount,
            )
        )
    return grouped


def row_to_values(row: AccountRow) -> list[str]:
    return [
        row.person,
        row.document,
        format_date_br(row.launch_date),
        row.quantity,
        row.index,
        row.days,
        format_brl(row.due_value),
        format_brl(row.addition),
        format_brl(row.discount),
        format_brl(row.total),
    ]


def write_company_files(
    output_dir: Path,
    section_name: str,
    source: str,
    company_names: dict[str, str],
    works_by_company: dict[str, list[WorkInfo]],
    rows_by_company: dict[str, list[AccountRow]],
    counts: dict[str, int],
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    company_ids = sorted(set(company_names) | set(works_by_company) | set(rows_by_company), key=lambda cid: company_names.get(cid, cid))
    dates = [row.launch_date for rows in rows_by_company.values() for row in rows if row.launch_date]
    period = f"{min(dates).isoformat()} ate {max(dates).isoformat()}" if dates else "Sem datas retornadas"
    for company_id in company_ids:
        company_name = company_names.get(company_id, f"Empresa {company_id}" if company_id != "0" else "Sem empresa identificada")
        rows = sorted(rows_by_company.get(company_id, []), key=lambda row: (row.launch_date or date.min, row.document, row.person))
        works = works_by_company.get(company_id, [])
        total_due = sum(row.due_value for row in rows)
        total_addition = sum(row.addition for row in rows)
        total_discount = sum(row.discount for row in rows)
        total = sum(row.total for row in rows)

        stem = normalize_filename(company_name)
        txt_path = output_dir / f"{stem}.txt"
        csv_path = output_dir / f"{stem}.csv"
        lines = [
            f"EMPRESA: {company_name}",
            f"ID_EMPRESA: {company_id}",
            f"PERIODO: {period}",
            source,
            "Observacao: Ind., Dias, Acrescimo e Desconto ficam em branco/R$ 0,00 quando o endpoint principal do Sienge nao retorna esses campos.",
            "Registros: " + "; ".join(f"{key}={value}" for key, value in counts.items()) + ".",
            "",
            "OBRAS ADMINISTRADAS",
            "ID_OBRA;OBRA",
        ]
        if works:
            lines.extend(f"{work.id};{work.name}" for work in works)
        else:
            lines.append("0;Sem obra identificada")
        lines.extend(["", section_name, ";".join(HEADER)])
        lines.extend(";".join(row_to_values(row)) for row in rows)
        lines.append("")
        lines.append("TOTAL;;;;;" + ";".join([format_brl(total_due), format_brl(total_addition), format_brl(total_discount), format_brl(total)]))
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(HEADER)
            for row in rows:
                writer.writerow(row_to_values(row))
            writer.writerow(["TOTAL", "", "", "", "", "", format_brl(total_due), format_brl(total_addition), format_brl(total_discount), format_brl(total)])
        written.extend([txt_path, csv_path])
    return written


async def run(args: argparse.Namespace) -> None:
    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    base_dir = Path(args.output_base_dir)
    print("[sienge] buscando catalogos", flush=True)
    companies, creditors, customers, cost_centers, enterprises = await asyncio.gather(
        client.fetch_empresas(),
        client.fetch_credores(),
        client._fetch_all_pages("/customers"),
        client._fetch_all_pages("/cost-centers"),
        client.fetch_obras(),
    )
    company_names = build_name_map(companies, ["id", "companyId", "code"], ["name", "nome", "tradeName", "fantasyName"], "Empresa")
    creditor_names = build_name_map(creditors, ["id", "creditorId", "code"], ["name", "creditorName", "nome", "tradeName", "fantasyName"], "Credor")
    customer_names = build_name_map(customers, ["id", "customerId", "clientId", "code"], ["name", "customerName", "nomeCliente", "nome", "tradeName", "fantasyName"], "Cliente")
    works_by_company = build_works_by_company(cost_centers, enterprises)
    print(
        f"[sienge] empresas={len(company_names)} credores={len(creditor_names)} clientes={len(customer_names)} obras={sum(len(v) for v in works_by_company.values())}",
        flush=True,
    )

    print("[sienge] buscando contas a pagar completas", flush=True)
    bills = await fetch_pages(
        client,
        "/bills",
        {"startDate": args.start_date, "endDate": args.end_date},
        limit=args.page_size,
    )
    payable_by_company = payable_rows(bills, company_names, creditor_names)
    payable_written = write_company_files(
        base_dir / "contas a pagar",
        "CONTAS_A_PAGAR",
        "Fonte: Sienge API REST ao vivo (/bills), sem uso do banco local.",
        company_names,
        works_by_company,
        payable_by_company,
        {"companies": len(company_names), "works": sum(len(v) for v in works_by_company.values()), "bills": len(bills)},
    )

    print("[sienge] buscando contas a receber completas", flush=True)
    receivable = await fetch_pages(client, "/accounts-receivable/receivable-bills", limit=args.page_size)
    receivable_by_company = receivable_rows(receivable, company_names, customer_names)
    receivable_written = write_company_files(
        base_dir / "contas a receber",
        "CONTAS_A_RECEBER",
        "Fonte: Sienge API REST ao vivo (/accounts-receivable/receivable-bills), sem uso do banco local.",
        company_names,
        works_by_company,
        receivable_by_company,
        {"companies": len(company_names), "works": sum(len(v) for v in works_by_company.values()), "titles": len(receivable)},
    )

    print("Arquivos gerados:")
    print(f"  contas a pagar: {len(payable_written)} arquivos")
    print(f"  contas a receber: {len(receivable_written)} arquivos")
    print(f"  pasta base: {base_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta contas a pagar e a receber por empresa direto do Sienge, sem banco.")
    parser.add_argument("--output-base-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt" / "empresas"))
    parser.add_argument("--start-date", default="1900-01-01")
    parser.add_argument("--end-date", default="2100-12-31")
    parser.add_argument("--page-size", type=int, default=200)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
