from __future__ import annotations

import argparse
import asyncio
import calendar
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from pathlib import Path
from typing import Any

# Permite rodar via "python scripts/export_receita_operacional.py" sem instalar pacote.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.sienge_client import SiengeClient
from backend.database import SessionLocal
from backend.services.sienge_storage import read_snapshot, write_snapshot


@dataclass(frozen=True)
class MonthBucket:
    key: str  # YYYY-MM
    start: date
    end: date


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    raw = str(value).strip()
    if not raw:
        return None

    # Common ISO formats: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, with/without tz
    try:
        # fromisoformat aceita 'YYYY-MM-DD' e 'YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]'
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.date()
    except Exception:
        pass

    # Fallback: tenta só a parte da data
    try:
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return dt.date()
    except Exception:
        return None


def _add_months(d: date, delta_months: int) -> date:
    y = d.year + (d.month - 1 + delta_months) // 12
    m = (d.month - 1 + delta_months) % 12 + 1
    # clamp day (evita estourar em meses com menos dias)
    last_day = calendar.monthrange(y, m)[1]
    day = min(d.day, last_day)
    return date(y, m, day)


def _last_day_of_month(d: date) -> date:
    last_day = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last_day)


def _format_brl(value: float) -> str:
    """Formata float em padrao BRL: R$ 1.234,56"""
    # arredonda para 2 casas
    s = f"{value:,.2f}"
    # Python usa ',' como milhar e '.' como decimal; inverte para pt-BR
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _month_buckets_for_last_12_months(today: date) -> list[MonthBucket]:
    current_month_first = date(today.year, today.month, 1)
    start_month_first = _add_months(current_month_first, -11)

    buckets: list[MonthBucket] = []
    cursor = start_month_first
    while cursor <= current_month_first:
        month_start = cursor
        month_end = _last_day_of_month(cursor)
        key = f"{cursor.year:04d}-{cursor.month:02d}"
        buckets.append(MonthBucket(key=key, start=month_start, end=month_end))
        cursor = _add_months(cursor, 1)

    return buckets


def _month_buckets_from_start_to_today(start: date, today: date) -> list[MonthBucket]:
    start_month_first = date(start.year, start.month, 1)
    current_month_first = date(today.year, today.month, 1)

    buckets: list[MonthBucket] = []
    cursor = start_month_first
    while cursor <= current_month_first:
        month_start = cursor
        month_end = _last_day_of_month(cursor)
        key = f"{cursor.year:04d}-{cursor.month:02d}"
        buckets.append(MonthBucket(key=key, start=month_start, end=month_end))
        cursor = _add_months(cursor, 1)
    return buckets


def _extract_receita_operacional_by_month(receber: list[dict[str, Any]], buckets: list[MonthBucket]) -> dict[str, float]:
    totals_by_month: dict[str, float] = {b.key: 0.0 for b in buckets}
    for item in receber:
        if not isinstance(item, dict):
            continue
        if _should_ignore(item):
            continue
        if _is_expense(item):
            continue
        due = _item_due_date(item)
        if due is None:
            continue
        for b in buckets:
            if b.start <= due <= b.end:
                totals_by_month[b.key] += _amount_receber_abs(item)
                break
    return totals_by_month


def _read_cached_receber() -> list[dict[str, Any]]:
    db = SessionLocal()
    try:
        payload = read_snapshot(db, "receber.json", default=[])
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        return []
    finally:
        db.close()


def _write_cached_receber(payload: list[dict[str, Any]]) -> None:
    db = SessionLocal()
    try:
        write_snapshot(db, "receber.json", payload)
    finally:
        db.close()


def _infer_min_due_date(receber: list[dict[str, Any]]) -> date | None:
    min_date: date | None = None
    for item in receber:
        if not isinstance(item, dict):
            continue
        due = _item_due_date(item)
        if due is None:
            continue
        if min_date is None or due < min_date:
            min_date = due
    return min_date


def _infer_max_due_date(receber: list[dict[str, Any]]) -> date | None:
    max_date: date | None = None
    for item in receber:
        if not isinstance(item, dict):
            continue
        due = _item_due_date(item)
        if due is None:
            continue
        if max_date is None or due > max_date:
            max_date = due
    return max_date


def _infer_min_due_date_receita(receber: list[dict[str, Any]]) -> date | None:
    """Menor data considerando apenas itens que entram na Receita Operacional."""
    min_date: date | None = None
    for item in receber:
        if not isinstance(item, dict):
            continue
        if _should_ignore(item) or _is_expense(item):
            continue
        due = _item_due_date(item)
        if due is None:
            continue
        if min_date is None or due < min_date:
            min_date = due
    return min_date


def _row_signature(item: dict[str, Any]) -> str:
    """Assinatura estável para deduplicar sem perder anos diferentes com o mesmo id."""
    sig = {
        "id": item.get("id"),
        "companyId": item.get("companyId"),
        "buildingId": item.get("buildingId"),
        "idObra": item.get("idObra"),
        "date": item.get("dataVencimento")
        or item.get("dueDate")
        or item.get("date")
        or item.get("operationDate")
        or item.get("paymentDate"),
        "amount": item.get("rawValue")
        if item.get("rawValue") is not None
        else item.get("amount")
        if item.get("amount") is not None
        else item.get("valor")
        if item.get("valor") is not None
        else item.get("value"),
        "type": item.get("type"),
        "statementType": item.get("statementType"),
        "statementOrigin": item.get("statementOrigin"),
        "documentNumber": item.get("documentNumber"),
        "billId": item.get("billId"),
    }
    return json.dumps(sig, sort_keys=True, ensure_ascii=False)


def _merge_receber(existing: list[dict[str, Any]], fresh: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    merged: list[dict[str, Any]] = []

    for it in existing:
        if not isinstance(it, dict):
            continue
        key = _row_signature(it)
        if key in seen:
            continue
        seen.add(key)
        merged.append(it)

    for it in fresh:
        if not isinstance(it, dict):
            continue
        key = _row_signature(it)
        if key in seen:
            continue
        seen.add(key)
        merged.append(it)

    return merged


def _iter_month_chunks(start: date, end: date, months_per_chunk: int) -> list[tuple[date, date]]:
    if months_per_chunk < 1:
        months_per_chunk = 1

    chunks: list[tuple[date, date]] = []
    cursor = date(start.year, start.month, 1)
    end_month_first = date(end.year, end.month, 1)

    while cursor <= end_month_first:
        chunk_start = cursor
        chunk_end_month = _add_months(cursor, months_per_chunk - 1)
        chunk_end = _last_day_of_month(chunk_end_month)
        if chunk_end > end:
            chunk_end = end
        if chunk_start < start:
            chunk_start = start
        chunks.append((chunk_start, chunk_end))
        cursor = _add_months(cursor, months_per_chunk)

    return chunks


async def _sync_receber_history_backward(
    *,
    today: date,
    months_per_chunk: int,
    max_empty_chunks: int,
    hard_start: date,
    verbose: bool,
) -> list[dict[str, Any]]:
    """Expande receber.json para trás até achar uma sequência de chunks vazios.

    Interpretação prática de "desde quando o SIENGE foi adicionado":
    quando começamos a ter dados no endpoint /accounts-statements.
    """
    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE não está configurado (.env). Não foi possível sincronizar histórico.")

    existing = _read_cached_receber()
    cached_min = _infer_min_due_date(existing)
    cached_max = _infer_max_due_date(existing)

    # Se cache já cobre até hoje, começamos a procurar antes do min.
    end_cursor = (cached_min - timedelta(days=1)) if cached_min else today
    empty_streak = 0

    # Se cache não tem nada, também vamos puxar do passado até hoje (pra frente)
    # usando o mesmo algoritmo (varrendo pra trás) + mesclando.
    while end_cursor >= hard_start and empty_streak < max_empty_chunks:
        # Define um chunk terminando no end_cursor (fechado em meses)
        end_month_last = _last_day_of_month(date(end_cursor.year, end_cursor.month, 1))
        if end_month_last > end_cursor:
            end_month_last = end_cursor

        start_month_first = date(end_cursor.year, end_cursor.month, 1)
        chunk_start_month = _add_months(start_month_first, -(months_per_chunk - 1))
        chunk_start = chunk_start_month
        if chunk_start < hard_start:
            chunk_start = hard_start

        chunk_end = end_month_last
        if chunk_start > chunk_end:
            break

        if verbose:
            print(f"[sync] receber {chunk_start.isoformat()} -> {chunk_end.isoformat()} ...")

        fresh = await client.fetch_receber_range(chunk_start.isoformat(), chunk_end.isoformat())
        fresh_list = [x for x in fresh if isinstance(x, dict)]

        if fresh_list:
            existing = _merge_receber(existing, fresh_list)
            empty_streak = 0
            if verbose:
                print(f"[sync] +{len(fresh_list)} itens (cache: {len(existing)})")
        else:
            empty_streak += 1
            if verbose:
                print(f"[sync] vazio (streak {empty_streak}/{max_empty_chunks})")

        # anda para trás
        end_cursor = chunk_start - timedelta(days=1)

    # Também garante que o cache vá até hoje (caso ele esteja desatualizado)
    if cached_max and cached_max < today:
        for c_start, c_end in _iter_month_chunks(cached_max + timedelta(days=1), today, months_per_chunk):
            if verbose:
                print(f"[sync] receber {c_start.isoformat()} -> {c_end.isoformat()} ...")
            fresh = await client.fetch_receber_range(c_start.isoformat(), c_end.isoformat())
            fresh_list = [x for x in fresh if isinstance(x, dict)]
            if fresh_list:
                existing = _merge_receber(existing, fresh_list)
                if verbose:
                    print(f"[sync] +{len(fresh_list)} itens (cache: {len(existing)})")

    _write_cached_receber(existing)
    return existing


def _should_ignore(item: dict[str, Any]) -> bool:
    statement_type = str(item.get("statementType") or item.get("operationType") or "").strip().lower()
    origin = str(item.get("statementOrigin") or item.get("origin") or "").strip().lower()

    # Ignora transferências/saques e origem BC (não operacional)
    if "transf" in statement_type or "transfer" in statement_type:
        return True
    if "saque" in statement_type:
        return True
    if origin == "bc":
        return True
    return False


def _is_expense(item: dict[str, Any]) -> bool:
    typ = str(item.get("type") or "").strip().lower()
    if typ == "expense":
        return True
    # fallback: alguns payloads podem vir só com valor negativo
    try:
        return float(item.get("rawValue") or 0) < 0
    except (TypeError, ValueError):
        return False


def _amount_receber_abs(item: dict[str, Any]) -> float:
    amount = _safe_float(item.get("rawValue") or item.get("amount") or item.get("valor") or item.get("value") or 0)
    return abs(amount)


def _item_due_date(item: dict[str, Any]) -> date | None:
    return _parse_date(
        item.get("dataVencimento")
        or item.get("dueDate")
        or item.get("date")
        or item.get("operationDate")
        or item.get("paymentDate")
    )


async def _run_last_12_months(*, output_path: Path, today: date) -> None:
    buckets = _month_buckets_for_last_12_months(today)
    start_date = buckets[0].start.strftime("%Y-%m-%d")
    end_date = buckets[-1].end.strftime("%Y-%m-%d")

    client = SiengeClient()
    if not getattr(client, "is_configured", False):
        raise SystemExit("SIENGE não está configurado (.env). Não foi possível buscar dados.")

    receber = await client.fetch_receber_range(start_date, end_date)
    receber_list = [x for x in receber if isinstance(x, dict)]
    totals_by_month = _extract_receita_operacional_by_month(receber_list, buckets)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_all = sum(totals_by_month.values())

    header_lines = [
        "RECEITA OPERACIONAL (SIENGE)",
        f"Periodo: {buckets[0].key} ate {buckets[-1].key} (12 meses, incluindo o mes atual)",
        f"Gerado em: {today.isoformat()}",
        "",
        "MES;RECEITA_OPERACIONAL",
    ]
    body_lines = [f"{k};{_format_brl(totals_by_month[k])}" for k in totals_by_month.keys()]
    footer_lines = ["", f"TOTAL;{_format_brl(total_all)}"]
    output_path.write_text("\n".join(header_lines + body_lines + footer_lines) + "\n", encoding="utf-8")


async def _run_total_history(
    *,
    output_path: Path,
    today: date,
    sync_full_history: bool,
    months_per_chunk: int,
    max_empty_chunks: int,
    hard_start: date,
    verbose: bool,
) -> None:
    receber_cached = _read_cached_receber()

    if sync_full_history:
        receber_cached = await _sync_receber_history_backward(
            today=today,
            months_per_chunk=months_per_chunk,
            max_empty_chunks=max_empty_chunks,
            hard_start=hard_start,
            verbose=verbose,
        )

    if not receber_cached:
        raise SystemExit(
            "Cache local vazio (snapshot receber.json). Rode primeiro a sincronizacao do SIENGE para popular o banco."  # noqa: E501
        )

    min_due = _infer_min_due_date_receita(receber_cached) or _infer_min_due_date(receber_cached)
    if min_due is None:
        raise SystemExit("Nao foi possivel inferir data inicial do dataset receber (datas invalidas).")

    buckets = _month_buckets_from_start_to_today(min_due, today)
    totals_by_month = _extract_receita_operacional_by_month(receber_cached, buckets)
    total_all = sum(totals_by_month.values())

    output_path.parent.mkdir(parents=True, exist_ok=True)

    header_lines = [
        "RECEITA OPERACIONAL TOTAL (SIENGE - CACHE LOCAL)",
        f"Periodo: {buckets[0].key} ate {buckets[-1].key} (todos os meses disponiveis no cache)",
        f"Gerado em: {today.isoformat()}",
        "",
        "MES;RECEITA_OPERACIONAL",
    ]
    body_lines = [f"{k};{_format_brl(totals_by_month[k])}" for k in totals_by_month.keys()]
    footer_lines = ["", f"TOTAL;{_format_brl(total_all)}"]
    output_path.write_text("\n".join(header_lines + body_lines + footer_lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta Receita Operacional mensal (ultimos 12 meses) do SIENGE")
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__).resolve().parents[1]
            / "assets"
            / "camada APiteste"
            / "txt"
            / "Receita.txt"
        ),
        help="Caminho do arquivo de saida (txt)",
    )
    parser.add_argument(
        "--output-total",
        default=str(
            Path(__file__).resolve().parents[1]
            / "assets"
            / "camada APiteste"
            / "txt"
            / "receita_total.txt"
        ),
        help="Caminho do arquivo de saida (txt) com todos os meses do cache + total",
    )
    parser.add_argument(
        "--today",
        default=None,
        help="Data de referencia (YYYY-MM-DD). Default: data atual do sistema.",
    )
    parser.add_argument(
        "--sync-full-history",
        action="store_true",
        help=(
            "Sincroniza o dataset receber.json para tras, ate achar um periodo longo sem dados, "
            "e entao gera receita_total.txt desde o primeiro mes com dados."
        ),
    )
    parser.add_argument(
        "--chunk-months",
        type=int,
        default=12,
        help="Tamanho do bloco (em meses) usado no sync historico. Default: 12.",
    )
    parser.add_argument(
        "--max-empty-chunks",
        type=int,
        default=6,
        help="Quantos blocos vazios seguidos (no passado) ate parar o sync historico. Default: 6.",
    )
    parser.add_argument(
        "--hard-start",
        default="2000-01-01",
        help="Limite inferior absoluto para o sync historico (YYYY-MM-DD). Default: 2000-01-01.",
    )
    parser.add_argument(
        "--verbose-sync",
        action="store_true",
        help="Mostra progresso durante o sync historico.",
    )

    args = parser.parse_args()
    output_path = Path(args.output)
    output_total_path = Path(args.output_total)

    if args.today:
        today = _parse_date(args.today)
        if today is None:
            raise SystemExit("--today invalido. Use YYYY-MM-DD")
    else:
        today = date.today()

    hard_start = _parse_date(args.hard_start)
    if hard_start is None:
        raise SystemExit("--hard-start invalido. Use YYYY-MM-DD")

    asyncio.run(_run_last_12_months(output_path=output_path, today=today))
    asyncio.run(
        _run_total_history(
            output_path=output_total_path,
            today=today,
            sync_full_history=bool(args.sync_full_history),
            months_per_chunk=int(args.chunk_months or 12),
            max_empty_chunks=int(args.max_empty_chunks or 6),
            hard_start=hard_start,
            verbose=bool(args.verbose_sync),
        )
    )


if __name__ == "__main__":
    main()
