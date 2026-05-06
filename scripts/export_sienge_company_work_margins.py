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


@dataclass
class WorkTotals:
    receita: float = 0.0
    custos_despesas: float = 0.0


@dataclass(frozen=True)
class Allocation:
    company_id: str
    work_id: str
    work_name: str
    rate: float


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


class BulkSiengeClient:
    def __init__(self) -> None:
        self.sienge = SiengeClient()
        if not getattr(self.sienge, "is_configured", False):
            raise SystemExit("SIENGE nao esta configurado no .env.")
        self.base = self.sienge.base_url.rstrip("/") + "/public/api/bulk-data/v1"
        self.headers, self.auth = self.sienge._auth_variants()[0]
        self.gate = RateGate()

    async def get_json(self, http: httpx.AsyncClient, path: str, params: dict[str, Any] | None = None) -> Any:
        url = self.base + path
        last_error = ""
        for attempt in range(8):
            await self.gate.wait()
            response = await http.get(url, headers=self.headers, auth=self.auth, params=params or {})
            if response.status_code == 429:
                retry_header = response.headers.get("Retry-After") or response.headers.get("retry-after")
                try:
                    wait_s = float(retry_header) if retry_header else 75.0 + (attempt * 20.0)
                except ValueError:
                    wait_s = 75.0 + (attempt * 20.0)
                print(f"[rate-limit] {path}: aguardando {wait_s:.0f}s antes de tentar novamente", flush=True)
                await asyncio.sleep(wait_s)
                last_error = response.text[:500]
                continue
            if response.status_code == 404:
                return {"data": []}
            if response.status_code >= 400:
                raise RuntimeError(f"{path} HTTP {response.status_code}: {response.text[:700]}")
            return response.json()
        raise RuntimeError(f"{path} excedeu tentativas por rate limit: {last_error}")

    async def fetch_async(self, http: httpx.AsyncClient, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        payload = await self.get_json(
            http,
            path,
            {
                **params,
                "_async": "true",
                "_asyncChunkMaxSize": 4096,
            },
        )
        identifier = payload.get("identifier") if isinstance(payload, dict) else None
        if not identifier:
            return extract_rows(payload)

        for poll in range(180):
            status = await self.get_json(http, f"/async/{identifier}")
            state = str(status.get("status") or "")
            if state == "Finished":
                rows: list[dict[str, Any]] = []
                chunks = int(status.get("chunks") or 1)
                for chunk in range(1, chunks + 1):
                    result = await self.get_json(http, f"/async/{identifier}/result/{chunk}")
                    rows.extend(extract_rows(result))
                return rows
            if state == "Failed":
                raise RuntimeError(f"{path} async failed: {status}")
            if poll == 0 or poll % 6 == 0:
                print(f"[sienge] {path} async {identifier}: status={state or 'aguardando'}", flush=True)
            await asyncio.sleep(5)
        raise TimeoutError(f"{path} async timeout: {identifier}")


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
        finish = month_end(add_months(cursor, max(chunk_months, 1) - 1))
        if finish > end:
            finish = end
        chunks.append(MonthRange(cursor, finish))
        cursor = add_months(cursor, max(chunk_months, 1))
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


def extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    return []


def first_text(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = fix_mojibake(item.get(key))
        if value and value.lower() not in {"none", "null", "undefined"}:
            return value
    return ""


def fix_mojibake(value: Any) -> str:
    text = str(value or "").strip()
    for _ in range(3):
        if not any(marker in text for marker in ("Ã", "Â", "â€", "ï¿½")):
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


def format_brl(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def format_percent(value: float) -> str:
    return f"{value:.2f}".replace(".", ",") + "%"


def normalize_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "-", name).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:140] or "SEM_NOME"


def amount_from_keys(item: dict[str, Any], keys: list[str]) -> float:
    for key in keys:
        if item.get(key) not in (None, ""):
            return abs(safe_float(item.get(key)))
    return 0.0


def row_chunk(row: dict[str, Any]) -> tuple[date | None, date | None, str]:
    start = parse_date(row.get("_chunkStart"))
    end = parse_date(row.get("_chunkEnd"))
    selection_type = str(row.get("_selectionType") or "").upper()
    return start, end, selection_type


def payment_in_row_chunk(item: dict[str, Any], row: dict[str, Any]) -> bool:
    start, end, selection_type = row_chunk(row)
    if selection_type != "P" or start is None or end is None:
        return True
    payment_date = parse_date(item.get("paymentDate") or item.get("calculationDate"))
    return payment_date is not None and start <= payment_date <= end


def income_amount(row: dict[str, Any]) -> float:
    receipts = row.get("receipts")
    if isinstance(receipts, list) and receipts:
        total = sum(
            amount_from_keys(receipt, ["grossAmount", "netAmount", "correctedAmount", "amount"])
            for receipt in receipts
            if isinstance(receipt, dict) and payment_in_row_chunk(receipt, row)
        )
        if total > 0:
            return total
    return amount_from_keys(row, ["originalAmount", "correctedBalanceAmount", "balanceAmount"])


def outcome_amount(row: dict[str, Any]) -> float:
    payments = row.get("payments")
    if isinstance(payments, list) and payments:
        total = sum(
            amount_from_keys(payment, ["netAmount", "grossAmount", "correctedNetAmount", "amount"])
            for payment in payments
            if isinstance(payment, dict) and payment_in_row_chunk(payment, row)
        )
        if total > 0:
            return total
    return amount_from_keys(row, ["originalAmount", "correctedBalanceAmount", "balanceAmount"])


def categories_from_bank_movements(row: dict[str, Any], movement_key: str, category_key: str) -> list[dict[str, Any]]:
    categories: list[dict[str, Any]] = []
    movements_parent = row.get(movement_key)
    if not isinstance(movements_parent, list):
        return categories
    for movement in movements_parent:
        if not isinstance(movement, dict):
            continue
        nested = movement.get("bankMovements")
        if not isinstance(nested, list):
            continue
        for bank_movement in nested:
            if not isinstance(bank_movement, dict):
                continue
            movement_categories = bank_movement.get(category_key)
            if isinstance(movement_categories, list):
                categories.extend(x for x in movement_categories if isinstance(x, dict))
    return categories


def build_allocations(
    row: dict[str, Any],
    category_rows: list[dict[str, Any]],
    fallback_company_id: str,
    work_by_id: dict[str, WorkInfo],
) -> list[Allocation]:
    grouped: dict[tuple[str, str, str], float] = defaultdict(float)
    for category in category_rows:
        work_id = id_text(
            category.get("costCenterId")
            or category.get("buildingId")
            or category.get("id")
        )
        if not work_id:
            continue
        known = work_by_id.get(work_id)
        company_id = id_text(category.get("companyId")) or (known.company_id if known else fallback_company_id)
        work_name = first_text(category, ["costCenterName", "buildingName", "name"]) or (known.name if known else f"Obra {work_id}")
        rate = safe_float(category.get("financialCategoryRate") or category.get("rate") or category.get("percentage"))
        grouped[(company_id or "0", work_id, work_name)] += rate

    if not grouped:
        return [
            Allocation(
                company_id=fallback_company_id or "0",
                work_id="0",
                work_name="Sem obra identificada",
                rate=100.0,
            )
        ]

    total_rate = sum(max(rate, 0.0) for rate in grouped.values())
    if total_rate <= 0:
        equal = 100.0 / len(grouped)
        return [Allocation(company_id, work_id, work_name, equal) for (company_id, work_id, work_name) in grouped]
    if total_rate > 100.5:
        return [
            Allocation(company_id, work_id, work_name, (rate / total_rate) * 100.0)
            for (company_id, work_id, work_name), rate in grouped.items()
        ]
    return [Allocation(company_id, work_id, work_name, rate) for (company_id, work_id, work_name), rate in grouped.items()]


def income_allocations(row: dict[str, Any], work_by_id: dict[str, WorkInfo]) -> list[Allocation]:
    fallback_company_id = id_text(row.get("companyId"))
    categories = row.get("receiptsCategories")
    category_rows = [x for x in categories if isinstance(x, dict)] if isinstance(categories, list) else []
    if not category_rows:
        category_rows = categories_from_bank_movements(row, "receipts", "financialCategories")
    return build_allocations(row, category_rows, fallback_company_id, work_by_id)


def outcome_allocations(row: dict[str, Any], work_by_id: dict[str, WorkInfo]) -> list[Allocation]:
    fallback_company_id = id_text(row.get("companyId"))
    buildings = row.get("buildingsCosts")
    building_rows = [x for x in buildings if isinstance(x, dict)] if isinstance(buildings, list) else []
    if building_rows:
        return build_allocations(row, building_rows, fallback_company_id, work_by_id)

    categories = row.get("paymentsCategories")
    category_rows = [x for x in categories if isinstance(x, dict)] if isinstance(categories, list) else []
    if not category_rows:
        category_rows = categories_from_bank_movements(row, "payments", "paymentCategories")
    return build_allocations(row, category_rows, fallback_company_id, work_by_id)


def link_id(item: dict[str, Any], rel: str, pattern: str) -> str:
    links = item.get("links")
    if not isinstance(links, list):
        return ""
    for link in links:
        if not isinstance(link, dict):
            continue
        if str(link.get("rel") or "") != rel:
            continue
        href = str(link.get("href") or "")
        match = re.search(pattern, href)
        if match:
            return id_text(match.group(1))
    return ""


def company_id_from_links(item: dict[str, Any]) -> str:
    return link_id(item, "company", r"/companies/(\d+)")


def cost_center_id_from_links(item: dict[str, Any]) -> str:
    return link_id(item, "cost-center", r"/cost-centers?/(\d+)")


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


def statement_amount(item: dict[str, Any]) -> float:
    return amount_from_keys(item, ["rawValue", "value", "amount", "valor", "totalInvoiceAmount", "totalAmount"])


def statement_allocations(item: dict[str, Any], work_by_id: dict[str, WorkInfo]) -> list[Allocation]:
    fallback_company_id = id_text(item.get("companyId")) or company_id_from_links(item)
    categories = item.get("budgetCategories")
    category_rows: list[dict[str, Any]] = []
    if isinstance(categories, list):
        for category in categories:
            if not isinstance(category, dict):
                continue
            work_id = id_text(category.get("costCenterId")) or cost_center_id_from_links(category)
            known = work_by_id.get(work_id)
            category_rows.append(
                {
                    "costCenterId": work_id,
                    "costCenterName": known.name if known else "",
                    "companyId": known.company_id if known else fallback_company_id,
                    "percentage": category.get("percentage"),
                }
            )
    return build_allocations(item, category_rows, fallback_company_id, work_by_id)


def bill_company_id(item: dict[str, Any]) -> str:
    return id_text(item.get("companyId") or item.get("debtorId")) or company_id_from_links(item)


def bill_amount(item: dict[str, Any]) -> float:
    return amount_from_keys(item, ["totalInvoiceAmount", "amount", "value", "totalAmount", "rawValue"])


def bill_allocations(item: dict[str, Any], categories: list[dict[str, Any]], work_by_id: dict[str, WorkInfo]) -> list[Allocation]:
    fallback_company_id = bill_company_id(item)
    return build_allocations(item, categories, fallback_company_id, work_by_id)


def apply_amount(
    totals: dict[tuple[str, str], WorkTotals],
    work_info: dict[tuple[str, str], WorkInfo],
    company_names: dict[str, str],
    amount: float,
    allocations: list[Allocation],
    field: str,
) -> None:
    if amount <= 0:
        return
    for allocation in allocations:
        company_id = allocation.company_id or "0"
        work_id = allocation.work_id or "0"
        key = (company_id, work_id)
        work_info.setdefault(key, WorkInfo(id=work_id, name=allocation.work_name, company_id=company_id))
        if company_id not in company_names:
            company_names[company_id] = "Sem empresa identificada" if company_id == "0" else f"Empresa {company_id}"
        value = amount * (allocation.rate / 100.0)
        if field == "receita":
            totals[key].receita += value
        else:
            totals[key].custos_despesas += value


def build_company_maps(companies: list[dict[str, Any]], cost_centers: list[dict[str, Any]]) -> tuple[dict[str, str], dict[tuple[str, str], WorkInfo], dict[str, WorkInfo]]:
    company_names: dict[str, str] = {}
    for company in companies:
        company_id = id_text(company.get("id") or company.get("companyId") or company.get("code"))
        name = first_text(company, ["name", "nome", "tradeName", "fantasyName"]) or f"Empresa {company_id}"
        if company_id:
            company_names[company_id] = name

    work_info: dict[tuple[str, str], WorkInfo] = {}
    work_by_id: dict[str, WorkInfo] = {}
    for cost_center in cost_centers:
        work_id = id_text(cost_center.get("id") or cost_center.get("costCenterId"))
        company_id = id_text(cost_center.get("idCompany") or cost_center.get("companyId"))
        if not work_id:
            continue
        name = first_text(cost_center, ["name", "costCenterName"]) or f"Obra {work_id}"
        info = WorkInfo(id=work_id, name=name, company_id=company_id or "0")
        work_info[(info.company_id, info.id)] = info
        work_by_id.setdefault(info.id, info)
        if info.company_id and info.company_id not in company_names:
            company_names[info.company_id] = f"Empresa {info.company_id}"
    return company_names, work_info, work_by_id


def aggregate(
    income_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    work_by_id: dict[str, WorkInfo],
) -> dict[tuple[str, str], WorkTotals]:
    totals: dict[tuple[str, str], WorkTotals] = defaultdict(WorkTotals)
    for row in income_rows:
        if not isinstance(row, dict):
            continue
        amount = income_amount(row)
        apply_amount(totals, work_info, company_names, amount, income_allocations(row, work_by_id), "receita")

    for row in outcome_rows:
        if not isinstance(row, dict):
            continue
        amount = outcome_amount(row)
        apply_amount(totals, work_info, company_names, amount, outcome_allocations(row, work_by_id), "custos_despesas")

    for key in work_info:
        totals.setdefault(key, WorkTotals())
    return totals


def aggregate_rest(
    statements: list[dict[str, Any]],
    bills: list[dict[str, Any]],
    bill_categories: dict[str, list[dict[str, Any]]],
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    work_by_id: dict[str, WorkInfo],
) -> dict[tuple[str, str], WorkTotals]:
    totals: dict[tuple[str, str], WorkTotals] = defaultdict(WorkTotals)
    for item in statements:
        if not isinstance(item, dict) or should_ignore_statement(item):
            continue
        amount = statement_amount(item)
        if amount <= 0:
            continue
        if is_expense_statement(item):
            # Titulo a pagar e contado por /bills para preservar a apropriacao por centro de custo.
            if item.get("billId"):
                continue
            apply_amount(totals, work_info, company_names, amount, statement_allocations(item, work_by_id), "custos_despesas")
        else:
            apply_amount(totals, work_info, company_names, amount, statement_allocations(item, work_by_id), "receita")

    for bill in bills:
        if not isinstance(bill, dict):
            continue
        amount = bill_amount(bill)
        if amount <= 0:
            continue
        bill_id = id_text(bill.get("id"))
        apply_amount(totals, work_info, company_names, amount, bill_allocations(bill, bill_categories.get(bill_id, []), work_by_id), "custos_despesas")

    for key in work_info:
        totals.setdefault(key, WorkTotals())
    return totals


def apply_rest_statement(
    item: dict[str, Any],
    totals: dict[tuple[str, str], WorkTotals],
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    work_by_id: dict[str, WorkInfo],
) -> bool:
    if not isinstance(item, dict) or should_ignore_statement(item):
        return False
    amount = statement_amount(item)
    if amount <= 0:
        return False
    if is_expense_statement(item):
        # Titulo a pagar e contado por /bills para preservar a apropriacao por centro de custo.
        if item.get("billId"):
            return False
        apply_amount(totals, work_info, company_names, amount, statement_allocations(item, work_by_id), "custos_despesas")
    else:
        apply_amount(totals, work_info, company_names, amount, statement_allocations(item, work_by_id), "receita")
    return True


def apply_rest_bill(
    bill: dict[str, Any],
    categories: list[dict[str, Any]],
    totals: dict[tuple[str, str], WorkTotals],
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    work_by_id: dict[str, WorkInfo],
) -> bool:
    if not isinstance(bill, dict):
        return False
    amount = bill_amount(bill)
    if amount <= 0:
        return False
    apply_amount(totals, work_info, company_names, amount, bill_allocations(bill, categories, work_by_id), "custos_despesas")
    return True


def write_company_files(
    output_dir: Path,
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    totals: dict[tuple[str, str], WorkTotals],
    start: date,
    end: date,
    counts: dict[str, int],
    source_note: str,
) -> list[Path]:
    company_dir = output_dir / "empresas"
    company_dir.mkdir(parents=True, exist_ok=True)

    works_by_company: dict[str, list[WorkInfo]] = defaultdict(list)
    for info in work_info.values():
        works_by_company[info.company_id or "0"].append(info)
    for company_id in company_names:
        works_by_company.setdefault(company_id, [])

    written: list[Path] = []
    for company_id, works in sorted(works_by_company.items(), key=lambda kv: company_names.get(kv[0], kv[0])):
        company_name = company_names.get(company_id) or ("Sem empresa identificada" if company_id == "0" else f"Empresa {company_id}")
        unique_works = {(work.id, work.name): work for work in works}.values()
        rows = []
        for work in unique_works:
            total = totals.get((company_id, work.id), WorkTotals())
            mc = total.receita - total.custos_despesas
            margem = (mc / total.receita * 100.0) if total.receita > 0 else 0.0
            rows.append((work.name, work.id, total.receita, total.custos_despesas, mc, margem))
        rows.sort(key=lambda row: (-row[2], row[0]))

        total_receita = sum(row[2] for row in rows)
        total_custos = sum(row[3] for row in rows)
        total_mc = total_receita - total_custos
        total_margem = (total_mc / total_receita * 100.0) if total_receita > 0 else 0.0

        stem = normalize_filename(company_name)
        txt_path = company_dir / f"{stem}.txt"
        csv_path = company_dir / f"{stem}.csv"

        lines = [
            f"EMPRESA: {company_name}",
            f"ID_EMPRESA: {company_id}",
            f"PERIODO: {start.isoformat()} ate {end.isoformat()}",
            source_note,
            "Formula: Margem = MC / Receita. MC = Receita - Custos/Despesas.",
            "Registros: " + "; ".join(f"{key}={value}" for key, value in counts.items()) + ".",
            "",
            "OBRA;ID_OBRA;RECEITA;CUSTOS_DESPESAS;MC;MARGEM",
        ]
        lines.extend(
            f"{name};{work_id};{format_brl(receita)};{format_brl(custos)};{format_brl(mc)};{format_percent(margem)}"
            for name, work_id, receita, custos, mc, margem in rows
        )
        lines.extend(
            [
                "",
                f"TOTAL;{company_id};{format_brl(total_receita)};{format_brl(total_custos)};{format_brl(total_mc)};{format_percent(total_margem)}",
            ]
        )
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["EMPRESA", "ID_EMPRESA", "OBRA", "ID_OBRA", "RECEITA", "CUSTOS_DESPESAS", "MC", "MARGEM"])
            for name, work_id, receita, custos, mc, margem in rows:
                writer.writerow(
                    [
                        company_name,
                        company_id,
                        name,
                        work_id,
                        format_brl(receita),
                        format_brl(custos),
                        format_brl(mc),
                        format_percent(margem),
                    ]
                )
            writer.writerow([company_name, company_id, "TOTAL", company_id, format_brl(total_receita), format_brl(total_custos), format_brl(total_mc), format_percent(total_margem)])

        written.extend([txt_path, csv_path])
    return written


def write_single_company_file(
    output_dir: Path,
    company_id: str,
    company_names: dict[str, str],
    work_info: dict[tuple[str, str], WorkInfo],
    totals: dict[tuple[str, str], WorkTotals],
    start: date,
    end: date,
    counts: dict[str, int],
    source_note: str,
) -> list[Path]:
    scoped_company_names = {company_id: company_names.get(company_id, f"Empresa {company_id}")}
    scoped_work_info = {
        key: info
        for key, info in work_info.items()
        if key[0] == company_id
    }
    scoped_totals = {
        key: value
        for key, value in totals.items()
        if key[0] == company_id
    }
    return write_company_files(output_dir, scoped_company_names, scoped_work_info, scoped_totals, start, end, counts, source_note)


def annotate_chunk(rows: list[dict[str, Any]], chunk: MonthRange, selection_type: str) -> list[dict[str, Any]]:
    for row in rows:
        row["_chunkStart"] = chunk.start.isoformat()
        row["_chunkEnd"] = chunk.end.isoformat()
        row["_selectionType"] = selection_type
    return rows


async def fetch_pages(client: SiengeClient, endpoint: str, params: dict[str, Any], limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    async with httpx.AsyncClient(timeout=client.timeout) as http:
        while True:
            page_params = {**params, "limit": limit, "offset": offset}
            payload = await client._get_json_via_client(http, endpoint, page_params)
            page = extract_rows(payload)
            if not page:
                break
            rows.extend(page)
            offset += len(page)
            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            if len(page) < limit or (isinstance(count, int) and offset >= count):
                break
            await asyncio.sleep(0.05)
    return rows


async def iter_pages(client: SiengeClient, endpoint: str, params: dict[str, Any], limit: int) -> Any:
    offset = 0
    async with httpx.AsyncClient(timeout=client.timeout) as http:
        while True:
            page_params = {**params, "limit": limit, "offset": offset}
            payload = await client._get_json_via_client(http, endpoint, page_params)
            page = extract_rows(payload)
            if not page:
                break
            yield page
            offset += len(page)
            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            if len(page) < limit or (isinstance(count, int) and offset >= count):
                break
            await asyncio.sleep(0.05)


async def fetch_rest_history(client: SiengeClient, start: date, end: date, chunk_months: int, company_id: str | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    statements_by_key: dict[str, dict[str, Any]] = {}
    bills_by_key: dict[str, dict[str, Any]] = {}
    chunks = iter_chunks(start, end, chunk_months)
    for index, chunk in enumerate(chunks, start=1):
        print(f"[sienge-rest] bloco {index}/{len(chunks)}: {chunk.start.isoformat()} ate {chunk.end.isoformat()}", flush=True)
        statement_params: dict[str, Any] = {"startDate": chunk.start.isoformat(), "endDate": chunk.end.isoformat()}
        bill_params: dict[str, Any] = {"startDate": chunk.start.isoformat(), "endDate": chunk.end.isoformat()}
        if company_id:
            statement_params["companyId"] = int(company_id)
            bill_params["debtorId"] = int(company_id)
        chunk_statements, chunk_bills = await asyncio.gather(
            fetch_pages(client, "/accounts-statements", statement_params),
            fetch_pages(client, "/bills", bill_params),
        )
        for item in chunk_statements:
            if not isinstance(item, dict):
                continue
            key = id_text(item.get("id")) or f"{item.get('date')}|{item.get('documentNumber')}|{item.get('value')}"
            statements_by_key[key] = item
        for item in chunk_bills:
            if not isinstance(item, dict):
                continue
            key = id_text(item.get("id")) or f"{item.get('issueDate')}|{item.get('documentNumber')}|{item.get('totalInvoiceAmount')}"
            bills_by_key[key] = item
        print(f"[sienge-rest] acumulado: statements={len(statements_by_key)} bills={len(bills_by_key)}", flush=True)
    return list(statements_by_key.values()), list(bills_by_key.values())


async def fetch_bill_budget_categories(client: SiengeClient, bills: list[dict[str, Any]], concurrency: int) -> dict[str, list[dict[str, Any]]]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    results: dict[str, list[dict[str, Any]]] = {}
    done = 0

    async def fetch_one(http: httpx.AsyncClient, bill: dict[str, Any]) -> None:
        nonlocal done
        bill_id = id_text(bill.get("id"))
        if not bill_id:
            return
        async with semaphore:
            rows: list[dict[str, Any]] = []
            for attempt in range(5):
                payload, err = await client._get_json_via_client_detailed(http, f"/bills/{bill_id}/budget-categories", None)
                if err and err.get("status_code") == 429:
                    await asyncio.sleep(2.0 + attempt)
                    continue
                rows = extract_rows(payload)
                break
            results[bill_id] = rows
            done += 1
            if done == 1 or done % 250 == 0 or done == len(bills):
                print(f"[sienge-rest] categorias de bills: {done}/{len(bills)}", flush=True)

    timeout = httpx.Timeout(120.0)
    async with httpx.AsyncClient(timeout=timeout) as http:
        batch_size = max(25, concurrency * 20)
        for start in range(0, len(bills), batch_size):
            batch = bills[start : start + batch_size]
            await asyncio.gather(*(fetch_one(http, bill) for bill in batch))
    return results


def row_company_id(item: dict[str, Any]) -> str:
    return id_text(item.get("companyId")) or company_id_from_links(item)


async def run_bulk(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    bulk = BulkSiengeClient()
    timeout = httpx.Timeout(900.0)

    print("[sienge] buscando empresas e centros de custo", flush=True)
    companies, cost_centers = await asyncio.gather(
        bulk.sienge.fetch_empresas(),
        bulk.sienge._fetch_all_pages("/cost-centers"),
    )
    if args.company_id:
        companies = [company for company in companies if id_text(company.get("id")) == args.company_id]
        cost_centers = [
            cost_center
            for cost_center in cost_centers
            if id_text(cost_center.get("idCompany") or cost_center.get("companyId")) == args.company_id
        ]
    company_names, work_info, work_by_id = build_company_maps(companies, cost_centers)

    income_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    chunks = iter_chunks(start, end, args.chunk_months)
    async with httpx.AsyncClient(timeout=timeout) as http:
        for index, chunk in enumerate(chunks, start=1):
            print(f"[sienge] bloco {index}/{len(chunks)}: {chunk.start.isoformat()} ate {chunk.end.isoformat()}", flush=True)
            chunk_income = await bulk.fetch_async(
                http,
                "/income",
                {
                    "startDate": chunk.start.isoformat(),
                    "endDate": chunk.end.isoformat(),
                    "selectionType": args.selection_type,
                    "correctionIndexerId": args.correction_indexer_id,
                    "correctionDate": end.isoformat(),
                },
            )
            income_rows.extend(annotate_chunk(chunk_income, chunk, args.selection_type))
            print(f"[sienge] receitas acumuladas: {len(income_rows)} linhas", flush=True)

            chunk_outcome = await bulk.fetch_async(
                http,
                "/outcome",
                {
                    "startDate": chunk.start.isoformat(),
                    "endDate": chunk.end.isoformat(),
                    "selectionType": args.selection_type,
                    "correctionIndexerId": args.correction_indexer_id,
                    "correctionDate": end.isoformat(),
                    "withBankMovements": "true",
                },
            )
            outcome_rows.extend(annotate_chunk(chunk_outcome, chunk, args.selection_type))
            print(f"[sienge] custos/despesas acumulados: {len(outcome_rows)} linhas", flush=True)

    totals = aggregate(income_rows, outcome_rows, company_names, work_info, work_by_id)
    written = write_company_files(
        Path(args.output_dir),
        company_names,
        work_info,
        totals,
        start,
        end,
        {
            "income": len(income_rows),
            "outcome": len(outcome_rows),
            "cost_centers": len(cost_centers),
        },
        "Fonte: Sienge API ao vivo (Bulk Data income/outcome + cost-centers), sem uso do banco local.",
    )

    print("Arquivos gerados:")
    for path in written:
        print(f"  {path}")


async def run_rest(args: argparse.Namespace) -> None:
    if args.company_id and args.batch_size:
        await run_rest_incremental(args)
        return

    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")

    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    print("[sienge-rest] buscando empresas e centros de custo", flush=True)
    companies, cost_centers = await asyncio.gather(
        client.fetch_empresas(),
        client._fetch_all_pages("/cost-centers"),
    )
    if args.company_id:
        companies = [company for company in companies if id_text(company.get("id")) == args.company_id]
        cost_centers = [
            cost_center
            for cost_center in cost_centers
            if id_text(cost_center.get("idCompany") or cost_center.get("companyId")) == args.company_id
        ]
    company_names, work_info, work_by_id = build_company_maps(companies, cost_centers)
    statements, bills = await fetch_rest_history(client, start, end, args.chunk_months, args.company_id)
    if args.company_id:
        statements = [statement for statement in statements if row_company_id(statement) == args.company_id]
        bills = [bill for bill in bills if bill_company_id(bill) == args.company_id]
        print(f"[sienge-rest] filtro companyId={args.company_id}: statements={len(statements)} bills={len(bills)}", flush=True)
    bill_categories = await fetch_bill_budget_categories(client, bills, args.detail_concurrency)

    totals = aggregate_rest(statements, bills, bill_categories, company_names, work_info, work_by_id)
    written = write_company_files(
        Path(args.output_dir),
        company_names,
        work_info,
        totals,
        start,
        end,
        {
            "accounts-statements": len(statements),
            "bills": len(bills),
            "bill-categories": sum(len(v) for v in bill_categories.values()),
            "cost-centers": len(cost_centers),
        },
        "Fonte: Sienge API REST ao vivo (accounts-statements, bills, bill budget-categories e cost-centers), sem uso do banco local.",
    )
    print("Arquivos gerados:")
    for path in written:
        print(f"  {path}")


async def run_rest_incremental(args: argparse.Namespace) -> None:
    start = parse_date(args.start)
    end = parse_date(args.today) if args.today else date.today()
    if start is None or end is None:
        raise SystemExit("Datas invalidas. Use YYYY-MM-DD.")
    company_id = str(args.company_id)

    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE nao esta configurado no .env.")

    print("[sienge-rest] buscando empresa e centros de custo", flush=True)
    companies, cost_centers = await asyncio.gather(
        client.fetch_empresas(),
        client._fetch_all_pages("/cost-centers"),
    )
    companies = [company for company in companies if id_text(company.get("id")) == company_id]
    cost_centers = [
        cost_center
        for cost_center in cost_centers
        if id_text(cost_center.get("idCompany") or cost_center.get("companyId")) == company_id
    ]
    company_names, work_info, work_by_id = build_company_maps(companies, cost_centers)
    totals: dict[tuple[str, str], WorkTotals] = defaultdict(WorkTotals)
    counts = {
        "accounts-statements": 0,
        "statements-applied": 0,
        "bills": 0,
        "bills-applied": 0,
        "bill-categories": 0,
        "cost-centers": len(cost_centers),
        "batches-written": 0,
    }
    source_note = (
        "Fonte: Sienge API REST ao vivo (accounts-statements, bills, bill budget-categories e cost-centers), "
        "sem uso do banco local. Arquivo atualizado incrementalmente de 20 em 20 registros."
    )
    seen_statements: set[str] = set()
    seen_bills: set[str] = set()

    chunks = iter_chunks(start, end, args.chunk_months)
    for index, chunk in enumerate(chunks, start=1):
        print(f"[sienge-rest] bloco {index}/{len(chunks)}: {chunk.start.isoformat()} ate {chunk.end.isoformat()}", flush=True)
        statement_params: dict[str, Any] = {
            "startDate": chunk.start.isoformat(),
            "endDate": chunk.end.isoformat(),
            "companyId": int(company_id),
        }
        async for page in iter_pages(client, "/accounts-statements", statement_params, args.batch_size):
            fresh: list[dict[str, Any]] = []
            for item in page:
                key = id_text(item.get("id")) or f"{item.get('date')}|{item.get('documentNumber')}|{item.get('value')}"
                if key in seen_statements:
                    continue
                seen_statements.add(key)
                fresh.append(item)
            applied = 0
            for item in fresh:
                if apply_rest_statement(item, totals, company_names, work_info, work_by_id):
                    applied += 1
            counts["accounts-statements"] += len(fresh)
            counts["statements-applied"] += applied
            counts["batches-written"] += 1
            written = write_single_company_file(Path(args.output_dir), company_id, company_names, work_info, totals, start, end, counts, source_note)
            print(
                f"[salvo] statements +{len(fresh)} (aplicados {applied}) -> {written[0]}",
                flush=True,
            )

        bill_params: dict[str, Any] = {
            "startDate": chunk.start.isoformat(),
            "endDate": chunk.end.isoformat(),
            "debtorId": int(company_id),
        }
        async for page in iter_pages(client, "/bills", bill_params, args.batch_size):
            fresh_bills: list[dict[str, Any]] = []
            for bill in page:
                key = id_text(bill.get("id")) or f"{bill.get('issueDate')}|{bill.get('documentNumber')}|{bill.get('totalInvoiceAmount')}"
                if key in seen_bills:
                    continue
                seen_bills.add(key)
                fresh_bills.append(bill)
            bill_categories = await fetch_bill_budget_categories(client, fresh_bills, args.detail_concurrency)
            applied = 0
            for bill in fresh_bills:
                bill_id = id_text(bill.get("id"))
                if apply_rest_bill(bill, bill_categories.get(bill_id, []), totals, company_names, work_info, work_by_id):
                    applied += 1
            counts["bills"] += len(fresh_bills)
            counts["bills-applied"] += applied
            counts["bill-categories"] += sum(len(v) for v in bill_categories.values())
            counts["batches-written"] += 1
            written = write_single_company_file(Path(args.output_dir), company_id, company_names, work_info, totals, start, end, counts, source_note)
            print(
                f"[salvo] bills +{len(fresh_bills)} (aplicados {applied}) -> {written[0]}",
                flush=True,
            )

    written = write_single_company_file(Path(args.output_dir), company_id, company_names, work_info, totals, start, end, counts, source_note)
    print("Arquivos gerados:")
    for path in written:
        print(f"  {path}")


async def run(args: argparse.Namespace) -> None:
    if args.source == "bulk":
        await run_bulk(args)
    else:
        await run_rest(args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera TXT e CSV por empresa/obra direto do Sienge, sem banco local.")
    parser.add_argument("--source", default="rest", choices=["rest", "bulk"])
    parser.add_argument("--company-id", default=None)
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--today", default=None)
    parser.add_argument("--selection-type", default="P", choices=["I", "D", "P", "B"], help="Tipo de data do Bulk Data: I emissao, D vencimento, P pagamento, B competencia.")
    parser.add_argument("--correction-indexer-id", type=int, default=0)
    parser.add_argument("--chunk-months", type=int, default=12)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--detail-concurrency", type=int, default=8)
    parser.add_argument("--output-dir", default=str(ROOT / "assets" / "camada APiteste" / "txt"))
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
