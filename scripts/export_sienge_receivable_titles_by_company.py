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

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient


@dataclass(frozen=True)
class MonthRange:
    start: date
    end: date


@dataclass(frozen=True)
class WorkInfo:
    id: str
    name: str
    company_id: str


@dataclass(frozen=True)
class ReceivableRow:
    title_id: str
    company_id: str
    company_name: str
    document: str
    document_number: str
    issue_date: date | None
    customer_id: str
    customer_name: str
    origin: str
    defaulting: str
    spc: str
    serasa: str
    subjudice: str
    admin_fee: float
    insurance: float
    receivable_value: float
    paid_value: float
    net_value: float
    total_value: float
    note: str


class RateGate:
    def __init__(self, min_interval: float = 0.32) -> None:
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


def add_months(d: date, delta: int) -> date:
    y = d.year + (d.month - 1 + delta) // 12
    m = (d.month - 1 + delta) % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def month_end(d: date) -> date:
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def iter_months(start: date, end: date) -> list[MonthRange]:
    months: list[MonthRange] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        finish = month_end(cursor)
        if finish > end:
            finish = end
        months.append(MonthRange(cursor, finish))
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


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def format_date_br(value: date | None) -> str:
    return value.strftime("%d/%m/%Y") if value else ""


def fix_mojibake(value: Any) -> str:
    text = str(value or "").strip()
    for _ in range(3):
        if not any(marker in text for marker in ("Ãƒ", "Ã‚", "Ã¢â‚¬", "Ã¯Â¿Â½", "ï¿½")):
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


def first_text(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = fix_mojibake(item.get(key))
        if value and value.lower() not in {"none", "null", "undefined"}:
            return value
    return ""


def normalize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "-", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:140] or "SEM_NOME"


def yes_no(value: Any) -> str:
    if isinstance(value, bool):
        return "Sim" if value else "Não"
    raw = str(value or "").strip().lower()
    if raw in {"true", "1", "s", "sim", "yes", "y"}:
        return "Sim"
    return "Não"


def clean_cell(value: Any) -> str:
    return re.sub(r"\s+", " ", fix_mojibake(value)).strip()


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


async def fetch_pages(
    client: SiengeClient,
    endpoint: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int = 200,
    gate: RateGate | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as http:
        while True:
            page_params = {**(params or {}), "limit": limit, "offset": offset}
            if gate:
                await gate.wait()
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


async def fetch_installments(
    client: SiengeClient,
    bill_id: str,
    gate: RateGate,
) -> list[dict[str, Any]]:
    if not bill_id:
        return []
    return await fetch_pages(
        client,
        f"/accounts-receivable/receivable-bills/{bill_id}/installments",
        limit=100,
        gate=gate,
    )


async def enrich_titles(
    client: SiengeClient,
    titles: list[dict[str, Any]],
    company_names: dict[str, str],
    customer_names: dict[str, str],
    gate: RateGate,
    concurrency: int,
) -> list[ReceivableRow]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    done = 0

    async def one(title: dict[str, Any]) -> ReceivableRow:
        nonlocal done
        async with semaphore:
            bill_id = id_text(title.get("receivableBillId") or title.get("id"))
            installments = await fetch_installments(client, bill_id, gate)
            total = safe_float(title.get("receivableBillValue") or title.get("value") or title.get("totalValue"))
            receivable = sum(safe_float(item.get("balanceDue")) for item in installments)
            if not installments:
                receivable = 0.0 if title.get("payOffDate") else total
            paid = max(total - receivable, 0.0)
            admin_fee = safe_float(
                title.get("administrationFee")
                or title.get("administrativeFee")
                or title.get("administrativeTax")
                or title.get("adminTax")
            )
            insurance = safe_float(title.get("insurance") or title.get("insuranceValue"))
            net = safe_float(title.get("netValue") or title.get("liquidValue"))
            if net <= 0 and total > 0:
                net = max(total - admin_fee - insurance, 0.0)
            company_id = id_text(title.get("companyId"))
            customer_id = id_text(title.get("customerId"))
            done += 1
            if done == 1 or done % 100 == 0 or done == len(titles):
                print(f"[sienge] parcelas titulos: {done}/{len(titles)}", flush=True)
            return ReceivableRow(
                title_id=bill_id,
                company_id=company_id,
                company_name=company_names.get(company_id, f"Empresa {company_id}" if company_id else ""),
                document=clean_cell(title.get("documentId")),
                document_number=clean_cell(title.get("documentNumber")),
                issue_date=parse_date(title.get("issueDate")),
                customer_id=customer_id,
                customer_name=customer_names.get(customer_id, f"Cliente {customer_id}" if customer_id else ""),
                origin=clean_cell(title.get("origin") or "CR"),
                defaulting=yes_no(title.get("defaulting")),
                spc=yes_no(title.get("spc")),
                serasa=yes_no(title.get("serasa")),
                subjudice=yes_no(title.get("subjudice")),
                admin_fee=admin_fee,
                insurance=insurance,
                receivable_value=receivable,
                paid_value=paid,
                net_value=net,
                total_value=total,
                note=clean_cell(title.get("note")),
            )

    return await asyncio.gather(*(one(title) for title in titles))


def build_company_names(companies: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for company in companies:
        company_id = id_text(company.get("id") or company.get("companyId") or company.get("code"))
        name = first_text(company, ["name", "nome", "tradeName", "fantasyName"]) or f"Empresa {company_id}"
        if company_id:
            out[company_id] = name
    return out


def build_customer_names(customers: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for customer in customers:
        customer_id = id_text(customer.get("id") or customer.get("customerId") or customer.get("codigoCliente") or customer.get("code"))
        name = first_text(customer, ["name", "customerName", "nomeCliente", "nome", "tradeName", "fantasyName"]) or f"Cliente {customer_id}"
        if customer_id:
            out[customer_id] = name
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


HEADER = [
    "Título",
    "Cód. empresa",
    "Empresa",
    "Documento",
    "Nº documento",
    "Data emissão",
    "Cód. cliente",
    "Cliente",
    "Origem",
    "Inadimplente",
    "SPC",
    "Serasa",
    "Subjudice",
    "Taxa administrativa",
    "Seguro",
    "Valor a receber",
    "Valor baixado",
    "Valor líquido",
    "Valor total",
    "Observação",
]


def row_to_values(row: ReceivableRow) -> list[str]:
    return [
        row.title_id,
        row.company_id,
        row.company_name,
        row.document,
        row.document_number,
        format_date_br(row.issue_date),
        row.customer_id,
        row.customer_name,
        row.origin,
        row.defaulting,
        row.spc,
        row.serasa,
        row.subjudice,
        format_brl(row.admin_fee),
        format_brl(row.insurance),
        format_brl(row.receivable_value),
        format_brl(row.paid_value),
        format_brl(row.net_value),
        format_brl(row.total_value),
        row.note,
    ]


def write_company_files(
    output_dir: Path,
    company_names: dict[str, str],
    works_by_company: dict[str, list[WorkInfo]],
    rows_by_company: dict[str, list[ReceivableRow]],
    start: date,
    end: date,
    processed_until: str,
    counts: dict[str, int],
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    all_company_ids = sorted(set(company_names) | set(works_by_company) | set(rows_by_company), key=lambda cid: company_names.get(cid, cid))
    for company_id in all_company_ids:
        company_name = company_names.get(company_id, f"Empresa {company_id}" if company_id else "Sem empresa identificada")
        rows = sorted(rows_by_company.get(company_id, []), key=lambda row: (row.issue_date or date.min, int(row.title_id or "0")))
        works = works_by_company.get(company_id, [])
        stem = normalize_filename(company_name)
        txt_path = output_dir / f"{stem}.txt"
        csv_path = output_dir / f"{stem}.csv"

        total_admin = sum(row.admin_fee for row in rows)
        total_insurance = sum(row.insurance for row in rows)
        total_receivable = sum(row.receivable_value for row in rows)
        total_paid = sum(row.paid_value for row in rows)
        total_net = sum(row.net_value for row in rows)
        total_value = sum(row.total_value for row in rows)

        lines = [
            f"EMPRESA: {company_name}",
            f"ID_EMPRESA: {company_id}",
            f"PERIODO: {start.isoformat()} ate {end.isoformat()}",
            "Fonte: Sienge API REST ao vivo (/accounts-receivable/receivable-bills e installments), sem uso do banco local.",
            "Observacao: SPC, Serasa, taxa administrativa e seguro ficam como Nao/R$ 0,00 quando o endpoint do Sienge nao retorna esses campos.",
            f"Atualizado ate o mes processado: {processed_until}",
            "Registros: " + "; ".join(f"{key}={value}" for key, value in counts.items()) + ".",
            "",
            "OBRAS ADMINISTRADAS",
            "ID_OBRA;OBRA",
        ]
        if works:
            lines.extend(f"{work.id};{work.name}" for work in works)
        else:
            lines.append("0;Sem obra identificada")
        lines.extend(["", "TITULOS_A_RECEBER", ";".join(HEADER)])
        lines.extend(";".join(row_to_values(row)) for row in rows)
        lines.extend(
            [
                "",
                "TOTAL;"
                + ";".join(
                    [
                        company_id,
                        company_name,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        format_brl(total_admin),
                        format_brl(total_insurance),
                        format_brl(total_receivable),
                        format_brl(total_paid),
                        format_brl(total_net),
                        format_brl(total_value),
                        "",
                    ]
                ),
            ]
        )
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(HEADER)
            for row in rows:
                writer.writerow(row_to_values(row))
            writer.writerow(
                [
                    "TOTAL",
                    company_id,
                    company_name,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    format_brl(total_admin),
                    format_brl(total_insurance),
                    format_brl(total_receivable),
                    format_brl(total_paid),
                    format_brl(total_net),
                    format_brl(total_value),
                    "",
                ]
            )
        written.extend([txt_path, csv_path])
    return written


async def run(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    requested_end = parse_date(args.today) if args.today else None
    if start is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    output_dir = Path(args.output_dir)
    print("[sienge] buscando empresas, clientes e obras", flush=True)
    companies, customers, cost_centers, enterprises = await asyncio.gather(
        client.fetch_empresas(),
        client._fetch_all_pages("/customers"),
        client._fetch_all_pages("/cost-centers"),
        client.fetch_obras(),
    )
    company_names = build_company_names(companies)
    customer_names = build_customer_names(customers)
    works_by_company = build_works_by_company(cost_centers, enterprises)
    print(
        f"[sienge] empresas={len(company_names)} clientes={len(customer_names)} obras={sum(len(v) for v in works_by_company.values())}",
        flush=True,
    )

    print("[sienge] buscando titulos a receber paginados", flush=True)
    title_rows = await fetch_pages(client, "/accounts-receivable/receivable-bills", limit=args.page_size)
    unique: dict[str, dict[str, Any]] = {}
    max_issue_date: date | None = None
    for title in title_rows:
        title_id = id_text(title.get("receivableBillId") or title.get("id"))
        issue = parse_date(title.get("issueDate"))
        if issue is not None and (max_issue_date is None or issue > max_issue_date):
            max_issue_date = issue
        if not title_id or issue is None or issue < start:
            continue
        if requested_end is not None and issue > requested_end:
            continue
        unique[title_id] = title
    end = requested_end or max_issue_date or date.today()
    titles = sorted(unique.values(), key=lambda item: (parse_date(item.get("issueDate")) or date.min, int(id_text(item.get("receivableBillId")) or "0")))
    print(
        f"[sienge] maior data de emissao retornada pela API: {(max_issue_date or date.today()).isoformat()}",
        flush=True,
    )
    print(f"[sienge] titulos filtrados por emissao {start.isoformat()}..{end.isoformat()}: {len(titles)}", flush=True)

    titles_by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for title in titles:
        issue = parse_date(title.get("issueDate"))
        if issue:
            titles_by_month[f"{issue.year:04d}-{issue.month:02d}"].append(title)

    rows_by_company: dict[str, list[ReceivableRow]] = defaultdict(list)
    counts = {
        "companies": len(company_names),
        "customers": len(customer_names),
        "works": sum(len(v) for v in works_by_company.values()),
        "titles": 0,
        "installments": 0,
        "months-written": 0,
    }
    gate = RateGate(args.min_request_interval)
    for month in iter_months(start, end):
        month_key = f"{month.start.year:04d}-{month.start.month:02d}"
        month_titles = titles_by_month.get(month_key, [])
        if month_titles:
            print(f"[mes] {month_key}: titulos={len(month_titles)}", flush=True)
            enriched = await enrich_titles(
                client,
                month_titles,
                company_names,
                customer_names,
                gate,
                args.detail_concurrency,
            )
            counts["titles"] += len(enriched)
            counts["installments"] += len(enriched)  # cada titulo consulta o endpoint de parcelas uma vez.
            for row in enriched:
                rows_by_company[row.company_id or "0"].append(row)
        counts["months-written"] += 1
        write_company_files(output_dir, company_names, works_by_company, rows_by_company, start, end, month_key, counts)
        if month_titles or args.verbose:
            print(f"[salvo] mes={month_key} arquivos em {output_dir}", flush=True)

    written = write_company_files(output_dir, company_names, works_by_company, rows_by_company, start, end, end.strftime("%Y-%m"), counts)
    print("Arquivos gerados:")
    for path in written:
        print(f"  {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta Titulos a Receber por empresa direto do Sienge, sem banco local.")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None, help="Data final opcional. Quando omitida, usa a maior data de emissao retornada pela API do Sienge.")
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt" / "empresas" / "titulos_a_receber"))
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--detail-concurrency", type=int, default=4)
    parser.add_argument("--min-request-interval", type=float, default=0.32)
    parser.add_argument("--verbose", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
