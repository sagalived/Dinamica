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
class PayableRow:
    bill_id: str
    company_id: str
    company_name: str
    creditor: str
    document: str
    issue_date: date | None
    installment_quantity: str
    installment_index: str
    days: str
    due_value: float
    addition: float
    discount: float
    total: float


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


def amount_from_keys(item: dict[str, Any], keys: list[str]) -> float:
    return sum(safe_float(item.get(key)) for key in keys)


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


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


async def fetch_bill_installments(
    client: SiengeClient,
    bill_id: str,
    gate: RateGate,
) -> list[dict[str, Any]]:
    if not bill_id:
        return []
    return await fetch_pages(client, f"/bills/{bill_id}/installments", limit=100, gate=gate)


def build_company_names(companies: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for company in companies:
        company_id = id_text(company.get("id") or company.get("companyId") or company.get("code"))
        name = first_text(company, ["name", "nome", "tradeName", "fantasyName"]) or f"Empresa {company_id}"
        if company_id:
            out[company_id] = name
    return out


def build_creditor_names(creditors: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for creditor in creditors:
        creditor_id = id_text(creditor.get("id") or creditor.get("creditorId") or creditor.get("code"))
        name = first_text(creditor, ["name", "creditorName", "nome", "tradeName", "fantasyName"]) or f"Credor {creditor_id}"
        if creditor_id:
            out[creditor_id] = name
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


def bill_document(bill: dict[str, Any]) -> str:
    parts = [
        clean_cell(bill.get("documentIdentificationId")),
        clean_cell(bill.get("documentNumber")),
    ]
    return " ".join(part for part in parts if part).strip()


def addition_value(item: dict[str, Any]) -> float:
    return amount_from_keys(
        item,
        [
            "addition",
            "additionAmount",
            "accrual",
            "accrualAmount",
            "interest",
            "interestAmount",
            "fine",
            "fineAmount",
            "correctionAmount",
            "monetaryCorrectionAmount",
        ],
    )


def discount_value(item: dict[str, Any]) -> float:
    return amount_from_keys(item, ["discount", "discountAmount", "abatement", "abatementAmount"])


async def enrich_bills(
    client: SiengeClient,
    bills: list[dict[str, Any]],
    company_names: dict[str, str],
    creditor_names: dict[str, str],
    gate: RateGate,
    concurrency: int,
) -> list[PayableRow]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    done = 0

    async def one(bill: dict[str, Any]) -> list[PayableRow]:
        nonlocal done
        async with semaphore:
            bill_id = id_text(bill.get("id") or bill.get("billId"))
            installments = await fetch_bill_installments(client, bill_id, gate)
            if not installments:
                installments = [{}]

            company_id = id_text(bill.get("debtorId") or bill.get("companyId") or bill.get("idCompany"))
            creditor_id = id_text(bill.get("creditorId") or bill.get("supplierId") or bill.get("idCredor"))
            issue = parse_date(bill.get("issueDate") or bill.get("billDate") or bill.get("registeredDate"))
            document = bill_document(bill) or bill_id
            quantity = id_text(bill.get("installmentsNumber")) or str(len(installments))
            bill_discount = discount_value(bill)
            rows: list[PayableRow] = []
            for installment in installments:
                due = parse_date(installment.get("dueDate"))
                launch = parse_date(installment.get("billDate") or installment.get("baseDate")) or issue
                days = ""
                if launch and due:
                    days = str((due - launch).days)

                due_value = safe_float(
                    installment.get("amount")
                    or installment.get("originalAmount")
                    or installment.get("correctedBalanceAmount")
                    or installment.get("balanceAmount")
                    or bill.get("totalInvoiceAmount")
                )
                addition = addition_value(installment)
                discount = discount_value(installment)
                if bill_discount and len(installments) == 1 and not discount:
                    discount = bill_discount
                total = due_value + addition - discount
                rows.append(
                    PayableRow(
                        bill_id=bill_id,
                        company_id=company_id,
                        company_name=company_names.get(company_id, f"Empresa {company_id}" if company_id else ""),
                        creditor=creditor_names.get(creditor_id)
                        or first_text(bill, ["creditorName", "nomeCredor", "supplierName", "nomeFornecedor"])
                        or (f"Credor {creditor_id}" if creditor_id else "Credor sem nome"),
                        document=document,
                        issue_date=issue,
                        installment_quantity=quantity,
                        installment_index=id_text(installment.get("installmentNumber") or installment.get("indexId")),
                        days=days,
                        due_value=due_value,
                        addition=addition,
                        discount=discount,
                        total=total,
                    )
                )

            done += 1
            if done == 1 or done % 100 == 0 or done == len(bills):
                print(f"[sienge] parcelas contas a pagar: {done}/{len(bills)}", flush=True)
            return rows

    nested = await asyncio.gather(*(one(bill) for bill in bills))
    return [row for rows in nested for row in rows]


HEADER = [
    "Credor",
    "Documento",
    "Lancamento",
    "Qt.",
    "Ind.",
    "Dias",
    "Valor no vencto",
    "Acrescimo",
    "Desconto",
    "Total",
]


def row_to_values(row: PayableRow) -> list[str]:
    return [
        row.creditor,
        row.document,
        format_date_br(row.issue_date),
        row.installment_quantity,
        row.installment_index,
        row.days,
        format_brl(row.due_value),
        format_brl(row.addition),
        format_brl(row.discount),
        format_brl(row.total),
    ]


def write_company_files(
    output_dir: Path,
    company_names: dict[str, str],
    works_by_company: dict[str, list[WorkInfo]],
    rows_by_company: dict[str, list[PayableRow]],
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
        rows = sorted(rows_by_company.get(company_id, []), key=lambda row: (row.issue_date or date.min, row.document, row.installment_index))
        works = works_by_company.get(company_id, [])
        stem = normalize_filename(company_name)
        txt_path = output_dir / f"{stem}.txt"
        csv_path = output_dir / f"{stem}.csv"

        total_due = sum(row.due_value for row in rows)
        total_addition = sum(row.addition for row in rows)
        total_discount = sum(row.discount for row in rows)
        total = sum(row.total for row in rows)

        lines = [
            f"EMPRESA: {company_name}",
            f"ID_EMPRESA: {company_id}",
            f"PERIODO: {start.isoformat()} ate {end.isoformat()}",
            "Fonte: Sienge API REST ao vivo (/bills e /bills/{id}/installments), sem uso do banco local.",
            "Observacao: Acrescimo e desconto ficam como R$ 0,00 quando o endpoint do Sienge nao retorna esses campos.",
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
        lines.extend(["", "CONTAS_A_PAGAR", ";".join(HEADER)])
        lines.extend(";".join(row_to_values(row)) for row in rows)
        lines.extend(
            [
                "",
                "TOTAL;;;;;;"
                + ";".join(
                    [
                        format_brl(total_due),
                        format_brl(total_addition),
                        format_brl(total_discount),
                        format_brl(total),
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
            writer.writerow(["TOTAL", "", "", "", "", "", format_brl(total_due), format_brl(total_addition), format_brl(total_discount), format_brl(total)])
        written.extend([txt_path, csv_path])
    return written


async def run(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    output_dir = Path(args.output_dir)
    print("[sienge] buscando empresas, credores e obras", flush=True)
    companies, creditors, cost_centers, enterprises = await asyncio.gather(
        client.fetch_empresas(),
        client.fetch_credores(),
        client._fetch_all_pages("/cost-centers"),
        client.fetch_obras(),
    )
    company_names = build_company_names(companies)
    creditor_names = build_creditor_names(creditors)
    works_by_company = build_works_by_company(cost_centers, enterprises)
    print(
        f"[sienge] empresas={len(company_names)} credores={len(creditor_names)} obras={sum(len(v) for v in works_by_company.values())}",
        flush=True,
    )

    rows_by_company: dict[str, list[PayableRow]] = defaultdict(list)
    seen_bill_ids: set[str] = set()
    counts = {
        "companies": len(company_names),
        "creditors": len(creditor_names),
        "works": sum(len(v) for v in works_by_company.values()),
        "bills": 0,
        "installment-rows": 0,
        "months-written": 0,
    }
    gate = RateGate(args.min_request_interval)
    for month in iter_months(start, end):
        month_key = f"{month.start.year:04d}-{month.start.month:02d}"
        print(f"[mes] {month_key}: buscando contas a pagar", flush=True)
        bills = await fetch_pages(
            client,
            "/bills",
            {"startDate": month.start.isoformat(), "endDate": month.end.isoformat()},
            limit=args.page_size,
            gate=gate,
        )
        unique_bills: list[dict[str, Any]] = []
        for bill in bills:
            bill_id = id_text(bill.get("id") or bill.get("billId"))
            if not bill_id or bill_id in seen_bill_ids:
                continue
            seen_bill_ids.add(bill_id)
            unique_bills.append(bill)

        if unique_bills:
            enriched = await enrich_bills(
                client,
                unique_bills,
                company_names,
                creditor_names,
                gate,
                args.detail_concurrency,
            )
            counts["bills"] += len(unique_bills)
            counts["installment-rows"] += len(enriched)
            for row in enriched:
                rows_by_company[row.company_id or "0"].append(row)

        counts["months-written"] += 1
        write_company_files(output_dir, company_names, works_by_company, rows_by_company, start, end, month_key, counts)
        if unique_bills or args.verbose:
            print(f"[salvo] mes={month_key} arquivos em {output_dir}", flush=True)

    written = write_company_files(output_dir, company_names, works_by_company, rows_by_company, start, end, end.strftime("%Y-%m"), counts)
    print("Arquivos gerados:")
    for path in written:
        print(f"  {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta Contas a Pagar por empresa direto do Sienge, sem banco local.")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt" / "empresas" / "contas a pagar"))
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--detail-concurrency", type=int, default=4)
    parser.add_argument("--min-request-interval", type=float, default=0.32)
    parser.add_argument("--verbose", action="store_true")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
