from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from backend.config import BASE_DIR
from backend.services.catalog_sync import upsert_catalog_from_sienge
from backend.services.sienge_cache import utc_now_iso
from backend.services.sienge_storage import read_snapshot, write_snapshot


ASSET_ROOT = BASE_DIR / "assets" / "camada APiteste"
TXT_ROOT = ASSET_ROOT / "txt"
JSON_CACHE_ROOT = ASSET_ROOT / "json" / "data_cache"
PENDING_ROOT = TXT_ROOT / "pendentes"
_LAST_SIGNATURE: tuple[int, float] | None = None

CATALOG_FILES = {
    "obras": "obras.json",
    "usuarios": "usuarios.json",
    "empresas": "empresas.json",
    "credores": "credores.json",
}


def _read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(errors="ignore")


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(_read_text(path))
    except Exception:
        return default


def _parse_header(lines: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in lines[:12]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        out[key.strip().lower()] = value.strip()
    return out


def _section_rows(lines: list[str], marker: str) -> list[dict[str, str]]:
    try:
        idx = next(i for i, line in enumerate(lines) if line.strip() == marker)
    except StopIteration:
        return []
    if idx + 1 >= len(lines):
        return []

    table_lines: list[str] = []
    for raw in lines[idx + 1 :]:
        line = raw.strip()
        if not line:
            if table_lines:
                break
            continue
        table_lines.append(raw)

    if len(table_lines) < 2:
        return []
    return [dict(row) for row in csv.DictReader(table_lines, delimiter=";")]


def _parse_ptbr_money(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    raw = raw.replace("R$", "").replace("%", "").strip()
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _parse_ptbr_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:10]


def _stable_int(*parts: Any) -> int:
    raw = "|".join(str(part or "").strip() for part in parts)
    return int(hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12], 16)


def _txt_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(
        path
        for path in folder.glob("*.txt")
        if not path.name.lower().endswith(("_export.log", "_export.err.log"))
    )


def _local_signature() -> tuple[int, float]:
    roots = [JSON_CACHE_ROOT, TXT_ROOT / "empresas", PENDING_ROOT]
    count = 0
    newest = 0.0
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            if suffix not in {".json", ".txt", ".csv"}:
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            count += 1
            newest = max(newest, stat.st_mtime)
    return count, newest


def _company_header(path: Path, lines: list[str]) -> tuple[int | None, str]:
    header = _parse_header(lines)
    company_id_raw = header.get("id_empresa")
    try:
        company_id = int(company_id_raw) if company_id_raw else None
    except ValueError:
        company_id = None
    return company_id, header.get("empresa") or path.stem


def parse_accounts_payable_txt() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _txt_files(TXT_ROOT / "empresas" / "contas a pagar"):
        lines = _read_text(path).splitlines()
        company_id, company_name = _company_header(path, lines)
        for row_index, row in enumerate(_section_rows(lines, "CONTAS_A_PAGAR"), start=1):
            creditor = row.get("Credor") or ""
            document = row.get("Documento") or ""
            due_date = _parse_ptbr_date(row.get("Lançamento"))
            amount = _parse_ptbr_money(row.get("Total") or row.get("Valor no vencto"))
            item_id = _stable_int("payable", path.name, row_index, company_id, creditor, document, due_date, amount)
            rows.append(
                {
                    "id": item_id,
                    "billId": item_id,
                    "numero": item_id,
                    "codigoTitulo": item_id,
                    "companyId": company_id,
                    "companyName": company_name,
                    "creditorName": creditor,
                    "nomeCredor": creditor,
                    "documentNumber": document,
                    "description": document or "Titulo a pagar",
                    "descricao": document or "Titulo a pagar",
                    "dueDate": due_date,
                    "dataVencimento": due_date,
                    "amount": amount,
                    "valor": amount,
                    "status": "Pendente",
                    "situacao": "Pendente",
                    "source": "txt_local",
                    "sourceFile": str(path.relative_to(BASE_DIR)),
                    "sourceLine": row_index,
                }
            )
    return rows


def parse_receivable_titles_txt() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _txt_files(TXT_ROOT / "empresas" / "titulos_a_receber"):
        lines = _read_text(path).splitlines()
        for row in _section_rows(lines, "TITULOS_A_RECEBER"):
            title_id = str(row.get("Título") or "").strip()
            company_id_raw = row.get("Cód. empresa")
            client_id_raw = row.get("Cód. cliente")
            try:
                company_id = int(company_id_raw) if company_id_raw else None
            except ValueError:
                company_id = None
            try:
                client_id = int(client_id_raw) if client_id_raw else None
            except ValueError:
                client_id = None
            issue_date = _parse_ptbr_date(row.get("Data emissão"))
            amount = _parse_ptbr_money(row.get("Valor total") or row.get("Valor líquido") or row.get("Valor a receber"))
            item_id = int(title_id) if title_id.isdigit() else _stable_int("receivable", title_id, row.get("Documento"), row.get("Nº documento"))
            document = " ".join(part for part in [row.get("Documento"), row.get("Nº documento")] if part)
            rows.append(
                {
                    "id": item_id,
                    "billId": item_id,
                    "numero": item_id,
                    "numeroTitulo": item_id,
                    "codigoTitulo": item_id,
                    "companyId": company_id,
                    "companyName": row.get("Empresa") or "",
                    "clientId": client_id,
                    "customerId": client_id,
                    "clientName": row.get("Cliente") or "",
                    "customerName": row.get("Cliente") or "",
                    "nomeCliente": row.get("Cliente") or "",
                    "documentNumber": document,
                    "description": row.get("Observação") or document or "Titulo a receber",
                    "descricao": row.get("Observação") or document or "Titulo a receber",
                    "issueDate": issue_date,
                    "dataEmissao": issue_date,
                    "dueDate": issue_date,
                    "dataVencimento": issue_date,
                    "amount": amount,
                    "rawValue": amount,
                    "valor": amount,
                    "status": "Pendente",
                    "situacao": "Pendente",
                    "origin": row.get("Origem") or "",
                    "source": "txt_local",
                    "sourceFile": str(path.relative_to(BASE_DIR)),
                }
            )
    return rows


def parse_company_margin_csv() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    root = TXT_ROOT / "empresas"
    if not root.exists():
        return rows
    for path in sorted(root.glob("*.csv")):
        try:
            text = _read_text(path)
            reader = csv.DictReader(text.splitlines(), delimiter=";")
        except Exception:
            continue
        expected = {"EMPRESA", "ID_EMPRESA", "OBRA", "ID_OBRA", "RECEITA", "CUSTOS_DESPESAS", "MC", "MARGEM"}
        if not reader.fieldnames or not expected.issubset(set(reader.fieldnames)):
            continue
        for row in reader:
            rows.append(
                {
                    "companyName": row.get("EMPRESA") or path.stem,
                    "companyId": str(row.get("ID_EMPRESA") or "").strip() or None,
                    "buildingName": row.get("OBRA") or "Sem obra",
                    "buildingId": str(row.get("ID_OBRA") or "").strip() or "0",
                    "receita": _parse_ptbr_money(row.get("RECEITA")),
                    "custo": _parse_ptbr_money(row.get("CUSTOS_DESPESAS")),
                    "mc": _parse_ptbr_money(row.get("MC")),
                    "pct": _parse_ptbr_money(row.get("MARGEM")),
                    "source": "csv_local_root",
                    "sourceFile": str(path.relative_to(BASE_DIR)),
                }
            )
    return rows


def _upsert_by_identity(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def identity(item: dict[str, Any]) -> str:
        for key in ("id", "billId", "numero", "codigoTitulo", "documentNumber"):
            value = item.get(key)
            if value not in (None, ""):
                return f"{key}:{value}"
        return "hash:" + hashlib.sha1(json.dumps(item, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    for item in existing:
        if isinstance(item, dict):
            merged[identity(item)] = item
    for item in incoming:
        if isinstance(item, dict):
            key = identity(item)
            merged[key] = {**merged.get(key, {}), **item}
    return list(merged.values())


def _collection_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return [item for item in data["results"] if isinstance(item, dict)]
        if isinstance(payload.get("results"), list):
            return [item for item in payload["results"] if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _pending_items(dataset: str) -> list[dict[str, Any]]:
    if not PENDING_ROOT.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(PENDING_ROOT.glob(f"*_{dataset}.txt")):
        payload = _read_json(path, {})
        if not isinstance(payload, dict) or payload.get("dataset") != dataset:
            continue
        rows.extend(_collection_from_payload(payload.get("payload")))
    return rows


def list_pending_payload_records(
    dataset: str | None = None,
    *,
    created_after: datetime | None = None,
) -> list[dict[str, Any]]:
    if not PENDING_ROOT.exists():
        return []

    records: list[dict[str, Any]] = []
    for path in sorted(PENDING_ROOT.glob("*.txt")):
        try:
            stat = path.stat()
        except OSError:
            continue

        file_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if created_after is not None and file_dt < created_after:
            continue

        payload = _read_json(path, {})
        if not isinstance(payload, dict):
            continue

        payload_dataset = str(payload.get("dataset") or "").strip()
        if dataset is not None and payload_dataset != dataset:
            continue

        records.append(
            {
                "file_name": path.name,
                "dataset": payload_dataset or dataset or "",
                "reason": payload.get("reason"),
                "start_date": payload.get("start_date"),
                "end_date": payload.get("end_date"),
                "created_at": payload.get("created_at"),
                "mtime": file_dt.isoformat(),
                "path": str(path),
            }
        )

    return records


def append_pending_payload(dataset: str, payload: Any, *, reason: str, start_date: str | None = None, end_date: str | None = None) -> Path | None:
    try:
        PENDING_ROOT.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        safe_dataset = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in dataset)
        path = PENDING_ROOT / f"{stamp}_{safe_dataset}.txt"
        content = {
            "dataset": dataset,
            "reason": reason,
            "start_date": start_date,
            "end_date": end_date,
            "created_at": utc_now_iso(),
            "payload": payload,
        }
        path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
    except Exception:
        return None


def load_local_file_cache(db: Session, *, include_transactions: bool = True, force: bool = False) -> dict[str, Any]:
    global _LAST_SIGNATURE
    signature = _local_signature()
    meta = read_snapshot(db, "sienge_local_files_meta", default={})
    if (
        not force
        and isinstance(meta, dict)
        and meta.get("signature") == list(signature)
        and (not include_transactions or meta.get("transactions_loaded") is True)
    ):
        counts = meta.get("counts")
        if isinstance(counts, dict):
            _LAST_SIGNATURE = signature
            return counts

    catalogs: dict[str, list[dict[str, Any]]] = {}
    for dataset, filename in CATALOG_FILES.items():
        payload = _read_json(JSON_CACHE_ROOT / filename, [])
        if isinstance(payload, list):
            catalogs[dataset] = [item for item in payload if isinstance(item, dict)]
            if catalogs[dataset]:
                write_snapshot(db, f"{dataset}.json", catalogs[dataset])

    if any(catalogs.values()):
        upsert_catalog_from_sienge(
            db,
            obras=catalogs.get("obras") or None,
            usuarios=catalogs.get("usuarios") or None,
            empresas=catalogs.get("empresas") or None,
            credores=catalogs.get("credores") or None,
        )

    if include_transactions:
        financeiro_txt = parse_accounts_payable_txt()
        if financeiro_txt:
            write_snapshot(db, "financeiro.json", _upsert_by_identity(financeiro_txt, _pending_items("financeiro")))

        receber_txt = parse_receivable_titles_txt()
        if receber_txt:
            write_snapshot(db, "receber.json", _upsert_by_identity(receber_txt, _pending_items("receber")))

    margin_rows = parse_company_margin_csv()
    if margin_rows:
        write_snapshot(db, "mc_company_building.json", margin_rows)
        received_by_building = read_snapshot(db, "recebidas_por_obra.json", default=[]) or []
        if isinstance(received_by_building, list) and received_by_building:
            from backend.services.sienge_received_accounts import apply_received_revenue_to_mc_rows

            margin_rows = apply_received_revenue_to_mc_rows(db, received_by_building)

    counts = {
        "obras": len(read_snapshot(db, "obras.json", default=[]) or []),
        "usuarios": len(read_snapshot(db, "usuarios.json", default=[]) or []),
        "credores": len(read_snapshot(db, "credores.json", default=[]) or []),
        "empresas": len(read_snapshot(db, "empresas.json", default=[]) or []),
        "pedidos": len(read_snapshot(db, "pedidos.json", default=[]) or []),
        "financeiro": len(read_snapshot(db, "financeiro.json", default=[]) or []),
        "receber": len(read_snapshot(db, "receber.json", default=[]) or []),
    }
    write_snapshot(
        db,
        "sienge_local_files_meta",
        {
            "loaded_at": utc_now_iso(),
            "counts": counts,
            "signature": list(signature),
            "transactions_loaded": bool(include_transactions),
            "mc_company_building": len(margin_rows),
        },
    )
    _LAST_SIGNATURE = signature
    return counts
