from __future__ import annotations

import asyncio
import unicodedata
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backend.config import BASE_DIR
from backend.services.sienge_cache import utc_now_iso
from backend.services.sienge_client import SiengeClient, sienge_client
from backend.services.sienge_storage import read_snapshot, write_snapshot


TXT_ROOT = BASE_DIR / "assets" / "camada APiteste" / "txt"
OPEN_ACCOUNTS_TXT = TXT_ROOT / "contasabertas_sienge.txt"
SETTLED_ACCOUNTS_TXT = TXT_ROOT / "contasbaixadas_sienge.txt"
DIVERGENCES_TXT = TXT_ROOT / "divergencias_contas_sienge_db.txt"


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    return SiengeClient._extract_collection(payload)


async def _fetch_pages(
    client: SiengeClient,
    endpoint: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as http:
        while True:
            page_params = {**(params or {}), "limit": limit, "offset": offset}
            payload = await client._get_json_via_client(http, endpoint, page_params)
            page = _extract_rows(payload)
            if not page:
                break
            rows.extend(page)
            metadata = payload.get("resultSetMetadata") if isinstance(payload, dict) else None
            count = metadata.get("count") if isinstance(metadata, dict) else None
            offset += len(page)
            if len(page) < limit or (isinstance(count, int) and offset >= count):
                break
            await asyncio.sleep(0.03)
    return rows


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _is_payable_bill_open(bill: dict[str, Any]) -> bool:
    status = _text(bill.get("status") or bill.get("situacao")).upper()
    return status not in {"S", "BAIXADO", "BAIXADA", "PAGO", "PAGA", "LIQUIDADO", "LIQUIDADA", "QUITADO", "QUITADA"}


def _is_payable_installment_open(installment: dict[str, Any]) -> bool:
    situation = _text(installment.get("situation") or installment.get("situacao")).upper()
    normalized = (
        situation.replace("Ã", "A")
        .replace("Õ", "O")
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
    )
    if normalized.startswith("NAO "):
        return True
    if normalized in {"PAGA", "PAGO", "LIQUIDADA", "LIQUIDADO", "BAIXADA", "BAIXADO", "QUITADA", "QUITADO"}:
        return False
    return True


def _first(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _normalize_status(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    for encoding in ("latin1", "cp1252"):
        try:
            repaired = text.encode(encoding).decode("utf-8")
            if repaired != text:
                text = repaired
                break
        except UnicodeError:
            pass
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.replace("_", " ").upper().split())


SETTLED_STATUS = {
    "S",
    "BAIXADO",
    "BAIXADA",
    "PAGO",
    "PAGA",
    "LIQUIDADO",
    "LIQUIDADA",
    "QUITADO",
    "QUITADA",
    "RECEBIDO",
    "RECEBIDA",
    "SETTLED",
    "PAID",
}
CANCELED_STATUS = {"CANCELADO", "CANCELADA", "CANCELED", "CANCELLED", "ANULADO", "ANULADA"}


def _is_settled_status(*values: Any) -> bool:
    for value in values:
        status = _normalize_status(value)
        if not status or status.startswith("NAO "):
            continue
        if status in SETTLED_STATUS:
            return True
        if any(marker in status for marker in ("BAIXAD", "LIQUID", "QUITAD", "SETTLED", "PAID")):
            return True
        if "PAGO" in status or "PAGA" in status or "RECEBID" in status:
            return True
    return False


def _is_canceled_status(*values: Any) -> bool:
    for value in values:
        status = _normalize_status(value)
        if status in CANCELED_STATUS or any(marker in status for marker in ("CANCEL", "ANULAD")):
            return True
    return False


def _payment_date(*items: dict[str, Any]) -> str:
    keys = (
        "paymentDate",
        "payOffDate",
        "settlementDate",
        "paidDate",
        "liquidationDate",
        "baixaDate",
        "dataBaixa",
        "dataPagamento",
    )
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in keys:
            value = _text(item.get(key))
            if value:
                return value
    return ""


def _is_payable_bill_open(bill: dict[str, Any]) -> bool:
    return not _is_canceled_status(bill.get("status"), bill.get("situacao")) and not _is_settled_status(
        bill.get("status"),
        bill.get("situacao"),
    )


def _is_payable_installment_open(installment: dict[str, Any]) -> bool:
    status = _normalize_status(_first(installment.get("situation"), installment.get("situacao"), installment.get("status")))
    if status.startswith("NAO "):
        return True
    return not _is_canceled_status(status) and not _is_settled_status(status) and not _payment_date(installment)


def _month_chunks(start_year: int, end: date) -> list[tuple[str, str]]:
    chunks: list[tuple[str, str]] = []
    year = start_year
    while year <= end.year:
        start = date(year, 1, 1)
        finish = date(year, 12, 31)
        if finish > end:
            finish = end
        chunks.append((start.isoformat(), finish.isoformat()))
        year += 1
    return chunks


async def _fetch_open_payables(client: SiengeClient, *, start_year: int, today: date) -> list[dict[str, Any]]:
    bills: list[dict[str, Any]] = []
    for start, end in _month_chunks(start_year, today):
        bills.extend(await _fetch_pages(client, "/bills", {"startDate": start, "endDate": end}))

    open_bills = [bill for bill in bills if _is_payable_bill_open(bill)]
    semaphore = asyncio.Semaphore(8)
    out: list[dict[str, Any]] = []

    async def consume_bill(bill: dict[str, Any]) -> None:
        bill_id = _text(bill.get("id") or bill.get("billId"))
        if not bill_id:
            return
        async with semaphore:
            installments = await _fetch_pages(client, f"/bills/{bill_id}/installments", limit=100)
        if not installments:
            installments = [{}]

        for installment in installments:
            if installment and not _is_payable_installment_open(installment):
                continue
            due_date = _text(installment.get("dueDate") or bill.get("dueDate") or bill.get("issueDate"))
            installment_number = installment.get("installmentNumber") or installment.get("indexId") or ""
            amount = _safe_float(
                installment.get("amount")
                or installment.get("originalAmount")
                or installment.get("correctedBalanceAmount")
                or installment.get("balanceAmount")
                or bill.get("totalInvoiceAmount")
            )
            if amount <= 0:
                continue
            row_id = f"{bill_id}-{installment_number or '1'}"
            out.append(
                {
                    "id": row_id,
                    "billId": bill_id,
                    "numero": row_id,
                    "codigoTitulo": row_id,
                    "companyId": bill.get("debtorId") or bill.get("companyId") or bill.get("idCompany"),
                    "creditorId": bill.get("creditorId") or bill.get("supplierId"),
                    "documentNumber": _text(bill.get("documentNumber") or bill.get("documentIdentificationId") or bill_id),
                    "description": _text(bill.get("notes") or bill.get("documentIdentificationId") or "Titulo a pagar"),
                    "descricao": _text(bill.get("notes") or bill.get("documentIdentificationId") or "Titulo a pagar"),
                    "dueDate": due_date,
                    "dataVencimento": due_date,
                    "issueDate": _text(bill.get("issueDate")),
                    "dataEmissao": _text(bill.get("issueDate")),
                    "amount": amount,
                    "valor": amount,
                    "status": "Pendente",
                    "situacao": "Pendente",
                    "installmentNumber": installment_number,
                    "source": "sienge_open_accounts",
                }
            )

    await asyncio.gather(*(consume_bill(bill) for bill in open_bills))
    out.sort(key=lambda item: (_text(item.get("dueDate")), _text(item.get("id"))))
    return out


async def _fetch_open_receivables(client: SiengeClient, *, enrich_installments: bool = False) -> list[dict[str, Any]]:
    titles = await _fetch_pages(client, "/accounts-receivable/receivable-bills")
    open_titles = [title for title in titles if not _text(title.get("payOffDate"))]
    if not enrich_installments:
        out = []
        for title in open_titles:
            bill_id = _text(title.get("receivableBillId") or title.get("id"))
            amount = _safe_float(title.get("receivableBillValue") or title.get("value") or title.get("totalValue"))
            if not bill_id or amount <= 0:
                continue
            due_date = _text(title.get("dueDate") or title.get("issueDate"))
            out.append(
                {
                    "id": bill_id,
                    "billId": bill_id,
                    "numero": bill_id,
                    "numeroTitulo": bill_id,
                    "codigoTitulo": bill_id,
                    "companyId": title.get("companyId"),
                    "clientId": title.get("customerId"),
                    "customerId": title.get("customerId"),
                    "documentNumber": _text(title.get("documentNumber") or title.get("documentId") or bill_id),
                    "description": _text(title.get("note") or title.get("documentId") or "Titulo a receber"),
                    "descricao": _text(title.get("note") or title.get("documentId") or "Titulo a receber"),
                    "issueDate": _text(title.get("issueDate")),
                    "dataEmissao": _text(title.get("issueDate")),
                    "dueDate": due_date,
                    "dataVencimento": due_date,
                    "amount": amount,
                    "rawValue": amount,
                    "valor": amount,
                    "status": "Pendente",
                    "situacao": "Pendente",
                    "source": "sienge_open_accounts",
                }
            )
        out.sort(key=lambda item: (_text(item.get("dueDate")), _text(item.get("id"))))
        return out

    semaphore = asyncio.Semaphore(8)
    out: list[dict[str, Any]] = []

    async def consume_title(title: dict[str, Any]) -> None:
        bill_id = _text(title.get("receivableBillId") or title.get("id"))
        if not bill_id:
            return
        async with semaphore:
            installments = await _fetch_pages(client, f"/accounts-receivable/receivable-bills/{bill_id}/installments", limit=100)
        if not installments:
            installments = [{}]

        for installment in installments:
            balance_due = _safe_float(installment.get("balanceDue") or installment.get("amount") or title.get("receivableBillValue"))
            if balance_due <= 0:
                continue
            due_date = _text(installment.get("dueDate") or title.get("dueDate") or title.get("issueDate"))
            installment_number = installment.get("installmentNumber") or installment.get("indexId") or ""
            row_id = f"{bill_id}-{installment_number or '1'}"
            out.append(
                {
                    "id": row_id,
                    "billId": bill_id,
                    "numero": row_id,
                    "numeroTitulo": row_id,
                    "codigoTitulo": row_id,
                    "companyId": title.get("companyId"),
                    "clientId": title.get("customerId"),
                    "customerId": title.get("customerId"),
                    "documentNumber": _text(title.get("documentNumber") or title.get("documentId") or bill_id),
                    "description": _text(title.get("note") or title.get("documentId") or "Titulo a receber"),
                    "descricao": _text(title.get("note") or title.get("documentId") or "Titulo a receber"),
                    "issueDate": _text(title.get("issueDate")),
                    "dataEmissao": _text(title.get("issueDate")),
                    "dueDate": due_date,
                    "dataVencimento": due_date,
                    "amount": balance_due,
                    "rawValue": balance_due,
                    "valor": balance_due,
                    "status": "Pendente",
                    "situacao": "Pendente",
                    "installmentNumber": installment_number,
                    "source": "sienge_open_accounts",
                }
            )

    await asyncio.gather(*(consume_title(title) for title in open_titles))
    out.sort(key=lambda item: (_text(item.get("dueDate")), _text(item.get("id"))))
    return out


def _payable_amount(bill: dict[str, Any], installment: dict[str, Any]) -> float:
    return _safe_float(
        _first(
            installment.get("amount"),
            installment.get("originalAmount"),
            installment.get("correctedAmount"),
            installment.get("correctedBalanceAmount"),
            installment.get("balanceAmount"),
            bill.get("totalInvoiceAmount"),
            bill.get("amount"),
            bill.get("value"),
        )
    )


def _is_payable_installment_settled(bill: dict[str, Any], installment: dict[str, Any]) -> bool:
    installment_status = _first(installment.get("situation"), installment.get("situacao"), installment.get("status"))
    normalized = _normalize_status(installment_status)
    if normalized.startswith("NAO "):
        return False
    if _is_settled_status(installment_status):
        return True
    if _payment_date(installment, bill):
        return True
    if not normalized and _is_settled_status(bill.get("status"), bill.get("situacao")):
        return True
    return False


def _payable_row(bill: dict[str, Any], installment: dict[str, Any], *, settled: bool) -> dict[str, Any] | None:
    if _is_canceled_status(
        bill.get("status"),
        bill.get("situacao"),
        installment.get("situation"),
        installment.get("situacao"),
        installment.get("status"),
    ):
        return None
    bill_id = _text(_first(bill.get("id"), bill.get("billId")))
    if not bill_id:
        return None
    amount = _payable_amount(bill, installment)
    if amount <= 0:
        return None
    installment_number = _first(installment.get("installmentNumber"), installment.get("indexId"), "")
    row_id = f"{bill_id}-{installment_number or '1'}"
    due_date = _text(_first(installment.get("dueDate"), bill.get("dueDate"), bill.get("issueDate")))
    payment_date = _payment_date(installment, bill)
    status = "Baixado" if settled else "Pendente"
    row = {
        "id": row_id,
        "billId": bill_id,
        "numero": row_id,
        "codigoTitulo": row_id,
        "companyId": _first(bill.get("debtorId"), bill.get("companyId"), bill.get("idCompany")),
        "creditorId": _first(bill.get("creditorId"), bill.get("supplierId"), bill.get("idCredor")),
        "documentNumber": _text(_first(bill.get("documentNumber"), bill.get("documentIdentificationId"), bill_id)),
        "description": _text(_first(bill.get("notes"), bill.get("documentIdentificationId"), "Titulo a pagar")),
        "descricao": _text(_first(bill.get("notes"), bill.get("documentIdentificationId"), "Titulo a pagar")),
        "dueDate": due_date,
        "dataVencimento": due_date,
        "issueDate": _text(bill.get("issueDate")),
        "dataEmissao": _text(bill.get("issueDate")),
        "paymentDate": payment_date,
        "dataPagamento": payment_date,
        "amount": amount,
        "valor": amount,
        "status": status,
        "situacao": status,
        "installmentNumber": installment_number,
        "source": "sienge_open_accounts",
    }
    return row


async def _fetch_payables_by_status(
    client: SiengeClient,
    *,
    start_year: int,
    today: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fetched: list[dict[str, Any]] = []
    for start, end in _month_chunks(start_year, today):
        fetched.extend(await _fetch_pages(client, "/bills", {"startDate": start, "endDate": end}))

    bills_by_id: dict[str, dict[str, Any]] = {}
    for bill in fetched:
        bill_id = _text(_first(bill.get("id"), bill.get("billId")))
        if bill_id and bill_id not in bills_by_id:
            bills_by_id[bill_id] = bill

    semaphore = asyncio.Semaphore(8)
    open_rows: list[dict[str, Any]] = []
    paid_rows: list[dict[str, Any]] = []

    async def consume_bill(bill: dict[str, Any]) -> None:
        bill_id = _text(_first(bill.get("id"), bill.get("billId")))
        if not bill_id:
            return
        if _is_canceled_status(bill.get("status"), bill.get("situacao")):
            return
        if _is_settled_status(bill.get("status"), bill.get("situacao")):
            row = _payable_row(bill, {}, settled=True)
            if row is not None:
                paid_rows.append(row)
            return
        async with semaphore:
            installments = await _fetch_pages(client, f"/bills/{bill_id}/installments", limit=100)
        if not installments:
            installments = [{}]

        for installment in installments:
            if not isinstance(installment, dict):
                continue
            settled = _is_payable_installment_settled(bill, installment)
            row = _payable_row(bill, installment, settled=settled)
            if row is None:
                continue
            if settled:
                paid_rows.append(row)
            else:
                open_rows.append(row)

    await asyncio.gather(*(consume_bill(bill) for bill in bills_by_id.values()))
    open_rows.sort(key=lambda item: (_text(item.get("dueDate")), _text(item.get("id"))))
    paid_rows.sort(key=lambda item: (_text(item.get("paymentDate") or item.get("dueDate")), _text(item.get("id"))), reverse=True)
    return open_rows, paid_rows


def _receivable_row(
    title: dict[str, Any],
    installment: dict[str, Any],
    *,
    amount: float,
    settled: bool,
) -> dict[str, Any] | None:
    bill_id = _text(_first(title.get("receivableBillId"), title.get("id"), title.get("billId")))
    if not bill_id or amount <= 0:
        return None
    installment_number = _first(installment.get("installmentNumber"), installment.get("indexId"), "")
    row_id = f"{bill_id}-{installment_number or '1'}"
    due_date = _text(_first(installment.get("dueDate"), title.get("dueDate"), title.get("issueDate")))
    payment_date = _payment_date(installment, title)
    status = "Recebido" if settled else "Pendente"
    return {
        "id": row_id,
        "billId": bill_id,
        "numero": row_id,
        "numeroTitulo": row_id,
        "codigoTitulo": row_id,
        "companyId": title.get("companyId"),
        "clientId": title.get("customerId"),
        "customerId": title.get("customerId"),
        "documentNumber": _text(_first(title.get("documentNumber"), title.get("documentId"), bill_id)),
        "description": _text(_first(title.get("note"), title.get("documentId"), "Titulo a receber")),
        "descricao": _text(_first(title.get("note"), title.get("documentId"), "Titulo a receber")),
        "issueDate": _text(title.get("issueDate")),
        "dataEmissao": _text(title.get("issueDate")),
        "dueDate": due_date,
        "dataVencimento": due_date,
        "paymentDate": payment_date,
        "dataPagamento": payment_date,
        "amount": amount,
        "rawValue": amount,
        "valor": amount,
        "status": status,
        "situacao": status,
        "installmentNumber": installment_number,
        "source": "sienge_open_accounts",
    }


async def _fetch_receivables_by_status(client: SiengeClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    titles = await _fetch_pages(client, "/accounts-receivable/receivable-bills")
    open_rows: list[dict[str, Any]] = []
    received_rows: list[dict[str, Any]] = []

    for title in titles:
        if not isinstance(title, dict):
            continue
        bill_id = _text(_first(title.get("receivableBillId"), title.get("id"), title.get("billId")))
        if not bill_id:
            continue
        title_total = _safe_float(_first(title.get("receivableBillValue"), title.get("value"), title.get("totalValue")))
        title_settled = bool(_payment_date(title)) or _is_settled_status(title.get("status"), title.get("situacao"))
        row = _receivable_row(title, {}, amount=title_total, settled=title_settled)
        if row is None:
            continue
        if title_settled:
            received_rows.append(row)
        else:
            open_rows.append(row)

    open_rows.sort(key=lambda item: (_text(item.get("dueDate")), _text(item.get("id"))))
    received_rows.sort(key=lambda item: (_text(item.get("paymentDate") or item.get("dueDate")), _text(item.get("id"))), reverse=True)
    return open_rows, received_rows


def _write_txt(
    payables: list[dict[str, Any]],
    receivables: list[dict[str, Any]],
    paid_payables: list[dict[str, Any]] | None = None,
    received_receivables: list[dict[str, Any]] | None = None,
) -> None:
    try:
        TXT_ROOT.mkdir(parents=True, exist_ok=True)
        lines = [
            "TIPO;ID;VENCIMENTO;EMPRESA;PESSOA;DOCUMENTO;VALOR",
        ]
        for item in payables:
            lines.append(
                f"PAGAR;{item.get('id')};{item.get('dueDate')};{item.get('companyId')};{item.get('creditorId') or ''};{item.get('documentNumber')};{item.get('amount')}"
            )
        for item in receivables:
            lines.append(
                f"RECEBER;{item.get('id')};{item.get('dueDate')};{item.get('companyId')};{item.get('clientId') or ''};{item.get('documentNumber')};{item.get('amount')}"
            )
        OPEN_ACCOUNTS_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

        paid_lines = [
            "TIPO;ID;VENCIMENTO;PAGAMENTO;EMPRESA;PESSOA;DOCUMENTO;VALOR",
        ]
        for item in paid_payables or []:
            paid_lines.append(
                f"PAGAR_BAIXADO;{item.get('id')};{item.get('dueDate')};{item.get('paymentDate') or ''};{item.get('companyId')};{item.get('creditorId') or ''};{item.get('documentNumber')};{item.get('amount')}"
            )
        for item in received_receivables or []:
            paid_lines.append(
                f"RECEBER_BAIXADO;{item.get('id')};{item.get('dueDate')};{item.get('paymentDate') or ''};{item.get('companyId')};{item.get('clientId') or ''};{item.get('documentNumber')};{item.get('amount')}"
            )
        SETTLED_ACCOUNTS_TXT.write_text("\n".join(paid_lines) + "\n", encoding="utf-8")
    except Exception:
        return


def _year_counts(rows: list[dict[str, Any]]) -> str:
    counts = Counter(_text(_first(row.get("dueDate"), row.get("dataVencimento"), row.get("issueDate"), row.get("dataEmissao")))[:4] or "sem_data" for row in rows)
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _status_counts(rows: list[dict[str, Any]]) -> str:
    counts = Counter(_normalize_status(_first(row.get("status"), row.get("situacao"))) or "SEM_STATUS" for row in rows)
    return ", ".join(f"{key}={value}" for key, value in counts.most_common(12))


def _match_key(item: dict[str, Any]) -> tuple[str, str, str]:
    document = _normalize_status(_first(item.get("documentNumber"), item.get("numeroDocumento"), item.get("documentId"), item.get("descricao")))
    due_date = _text(_first(item.get("dueDate"), item.get("dataVencimento"), item.get("issueDate"), item.get("dataEmissao")))[:10]
    amount = round(abs(_safe_float(_first(item.get("amount"), item.get("valor"), item.get("rawValue"), item.get("totalInvoiceAmount")))), 2)
    return (document, due_date, f"{amount:.2f}")


def _sample_lines(label: str, rows: list[dict[str, Any]], *, limit: int = 50) -> list[str]:
    lines = [f"{label}: {len(rows)}"]
    for item in rows[:limit]:
        lines.append(
            "  "
            + ";".join(
                [
                    _text(_first(item.get("id"), item.get("billId"), item.get("codigoTitulo"))),
                    _text(_first(item.get("dueDate"), item.get("dataVencimento"))),
                    _text(_first(item.get("paymentDate"), item.get("dataPagamento"))),
                    _text(item.get("companyId")),
                    _text(_first(item.get("creditorId"), item.get("clientId"), item.get("customerId"))),
                    _text(_first(item.get("documentNumber"), item.get("descricao"), item.get("description"))),
                    f"{_safe_float(_first(item.get('amount'), item.get('valor'), item.get('rawValue'))):.2f}",
                    _text(_first(item.get("status"), item.get("situacao"))),
                ]
            )
        )
    if len(rows) > limit:
        lines.append(f"  ... mais {len(rows) - limit} registros")
    return lines


def _write_divergence_txt(
    db: Session,
    payables: list[dict[str, Any]],
    receivables: list[dict[str, Any]],
    paid_payables: list[dict[str, Any]],
    received_receivables: list[dict[str, Any]],
    *,
    synced_at: str,
) -> None:
    try:
        raw_payables = _extract_rows(read_snapshot(db, "financeiro.json", default=[]) or [])
        raw_receivables = _extract_rows(read_snapshot(db, "receber.json", default=[]) or [])
        sienge_payables = payables + paid_payables
        sienge_receivables = receivables + received_receivables
        raw_payable_keys = {_match_key(item) for item in raw_payables}
        raw_receivable_keys = {_match_key(item) for item in raw_receivables}
        sienge_payable_keys = {_match_key(item) for item in sienge_payables}
        sienge_receivable_keys = {_match_key(item) for item in sienge_receivables}

        db_payable_only = [item for item in raw_payables if _match_key(item) not in sienge_payable_keys]
        db_receivable_only = [item for item in raw_receivables if _match_key(item) not in sienge_receivable_keys]
        sienge_payable_only = [item for item in sienge_payables if _match_key(item) not in raw_payable_keys]
        sienge_receivable_only = [item for item in sienge_receivables if _match_key(item) not in raw_receivable_keys]

        lines = [
            "DIVERGENCIAS SIENGE X BANCO - CONTAS",
            f"Gerado em: {synced_at}",
            "",
            "Resumo:",
            f"  banco.financeiro.json={len(raw_payables)} | Sienge pagar aberto={len(payables)} | Sienge pagar baixado={len(paid_payables)}",
            f"  banco.receber.json={len(raw_receivables)} | Sienge receber aberto={len(receivables)} | Sienge receber baixado={len(received_receivables)}",
            "",
            "Distribuicao por ano:",
            f"  banco.financeiro: {_year_counts(raw_payables)}",
            f"  Sienge pagar: {_year_counts(sienge_payables)}",
            f"  banco.receber: {_year_counts(raw_receivables)}",
            f"  Sienge receber: {_year_counts(sienge_receivables)}",
            "",
            "Status no banco/cache:",
            f"  financeiro.json: {_status_counts(raw_payables)}",
            f"  receber.json: {_status_counts(raw_receivables)}",
            "",
            "Observacao: a chave de comparacao abaixo usa DOCUMENTO + VENCIMENTO + VALOR, porque o txt local antigo nao guarda o ID real do titulo do Sienge.",
            "",
        ]
        lines.extend(_sample_lines("No banco, mas nao encontrado no Sienge atual - pagar", db_payable_only))
        lines.append("")
        lines.extend(_sample_lines("No Sienge atual, mas nao encontrado no banco - pagar", sienge_payable_only))
        lines.append("")
        lines.extend(_sample_lines("No banco, mas nao encontrado no Sienge atual - receber", db_receivable_only))
        lines.append("")
        lines.extend(_sample_lines("No Sienge atual, mas nao encontrado no banco - receber", sienge_receivable_only))
        DIVERGENCES_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception:
        return


async def sync_open_accounts_from_sienge(
    db: Session,
    *,
    start_year: int = 2019,
    today: date | None = None,
) -> dict[str, Any]:
    if today is None:
        today = date.today()
    if not sienge_client.is_configured:
        return {"ok": False, "reason": "sienge_not_configured"}

    (payables, paid_payables), (receivables, received_receivables) = await asyncio.gather(
        _fetch_payables_by_status(sienge_client, start_year=start_year, today=today),
        _fetch_receivables_by_status(sienge_client),
    )
    synced_at = utc_now_iso()

    if not any([payables, paid_payables, receivables, received_receivables]):
        existing_counts = {
            "financeiro_abertos": len(read_snapshot(db, "financeiro_abertos.json", default=[]) or []),
            "financeiro_pagos": len(read_snapshot(db, "financeiro_pagos.json", default=[]) or []),
            "receber_abertos": len(read_snapshot(db, "receber_abertos.json", default=[]) or []),
            "receber_baixados": len(read_snapshot(db, "receber_baixados.json", default=[]) or []),
        }
        meta = {
            "ok": False,
            "source": "sienge_live",
            "synced_at": synced_at,
            "start_year": start_year,
            "today": today.isoformat(),
            "reason": "sienge_returned_no_accounts_or_connection_failed",
            "diagnostic": sienge_client.last_error,
            "preserved_existing_snapshots": True,
            "counts": existing_counts,
        }
        write_snapshot(db, "sienge_open_accounts_meta", meta)
        return meta

    meta = {
        "ok": True,
        "source": "sienge_live",
        "synced_at": synced_at,
        "start_year": start_year,
        "today": today.isoformat(),
        "counts": {
            "financeiro_abertos": len(payables),
            "financeiro_pagos": len(paid_payables),
            "receber_abertos": len(receivables),
            "receber_baixados": len(received_receivables),
        },
    }
    write_snapshot(db, "financeiro_abertos.json", payables)
    write_snapshot(db, "financeiro_pagos.json", paid_payables)
    write_snapshot(db, "receber_abertos.json", receivables)
    write_snapshot(db, "receber_baixados.json", received_receivables)
    write_snapshot(db, "sienge_open_accounts_meta", meta)
    _write_txt(payables, receivables, paid_payables, received_receivables)
    _write_divergence_txt(
        db,
        payables,
        receivables,
        paid_payables,
        received_receivables,
        synced_at=synced_at,
    )
    return meta
