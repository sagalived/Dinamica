from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from pathlib import Path

import psycopg
from psycopg import sql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Permite rodar o script direto: `python scripts/setup_sienge_postgres.py`
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _pg_params() -> dict[str, str | int]:
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "admin"),
        "db": os.getenv("POSTGRES_DB", "Sienge"),
    }


def _ensure_database_exists() -> None:
    p = _pg_params()
    target_db = str(p["db"])

    # conecta no DB padrão para poder criar outro DB
    conn = psycopg.connect(
        host=str(p["host"]),
        port=int(p["port"]),
        user=str(p["user"]),
        password=str(p["password"]),
        dbname=os.getenv("POSTGRES_ADMIN_DB", "postgres"),
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(target_db)))
                print(f"OK: banco '{target_db}' criado")
            else:
                print(f"OK: banco '{target_db}' já existe")
    finally:
        conn.close()


def _database_url() -> str:
    p = _pg_params()
    return f"postgresql+psycopg://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['db']}"


async def _run_backfill() -> None:
    # IMPORTANTE: setar DATABASE_URL antes de importar backend.* que depende de config
    os.environ["DATABASE_URL"] = _database_url()

    from backend.database import Base  # noqa: WPS433
    from backend.models import (  # noqa: WPS433
        AppUser,
        Building,
        Company,
        Creditor,
        DirectoryUser,
        Client,
        SiengeSnapshot,
        OperationalMonthlyAggregate,
        SiengeNfeDocument,
        SiengeRawRecord,
    )
    from backend.services.bootstrap import ensure_seed_data  # noqa: WPS433
    from backend.services.immutable_history import _add_month, _last_complete_month, _month_start_end  # noqa: WPS433
    from backend.services.nfe_documents import sync_nfe_documents_range  # noqa: WPS433
    from backend.services.sienge_client import sienge_client  # noqa: WPS433
    from backend.services.sienge_raw_records import upsert_raw_records  # noqa: WPS433

    engine = create_engine(os.environ["DATABASE_URL"], future=True, pool_pre_ping=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    if not sienge_client.is_configured:
        raise SystemExit("SIENGE não configurado no .env (SIENGE_ACCESS_NAME/TOKEN ou user/pass)")

    with SessionLocal() as db:
        ensure_seed_data(db)

    # Catálogo (sem range)
    print("Baixando catálogo (enterprises/users/companies/creditors)...")
    obras = await sienge_client.fetch_obras()
    usuarios = await sienge_client.fetch_users()
    empresas = await sienge_client.fetch_empresas()
    credores = await sienge_client.fetch_credores()

    with SessionLocal() as db:
        upsert_raw_records(db, dataset="enterprises", records=obras, id_fields=("id", "code", "codigoVisivel"))
        upsert_raw_records(db, dataset="users", records=usuarios, id_fields=("id",))
        upsert_raw_records(db, dataset="companies", records=empresas, id_fields=("id",))
        upsert_raw_records(db, dataset="creditors", records=credores, id_fields=("id",))

    # Backfill mensal desde 2019-01 até último mês completo
    start_month = os.getenv("SIENGE_FULL_BACKFILL_START_MONTH", "2019-01").strip() or "2019-01"
    cursor = start_month
    target = _last_complete_month(date.today())

    include_order_items = str(os.getenv("SIENGE_FULL_BACKFILL_ORDER_ITEMS", "false") or "false").lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }

    print(f"Backfill mensal: {cursor} -> {target} (order_items={include_order_items})")

    while cursor <= target:
        m_start, m_end = _month_start_end(cursor)
        print(f"- {cursor} ({m_start}..{m_end})")

        pedidos = await sienge_client.fetch_pedidos_range(m_start, m_end)
        bills = await sienge_client.fetch_financeiro_range(m_start, m_end)
        stmts = await sienge_client.fetch_receber_range(m_start, m_end)

        with SessionLocal() as db:
            upsert_raw_records(db, dataset="purchase-orders", records=pedidos, id_fields=("id", "numero"))
            upsert_raw_records(db, dataset="bills", records=bills, id_fields=("id", "billId", "number"))
            upsert_raw_records(db, dataset="accounts-statements", records=stmts, id_fields=("id", "documentId", "billId"))

        # NF-e
        with SessionLocal() as db:
            await sync_nfe_documents_range(db=db, start_date=m_start, end_date=m_end, allow_updates=False)

        # Itens de pedidos (opcional, pode ser bem pesado)
        if include_order_items and pedidos:
            # processa um por um para não estourar memória
            for p in pedidos:
                oid = p.get("id") or p.get("numero")
                if oid is None:
                    continue
                try:
                    oid_int = int(oid)
                except (TypeError, ValueError):
                    continue
                items = await sienge_client.fetch_purchase_order_items(oid_int)
                if not items:
                    continue
                # dataset por pedido para manter chave estável
                with SessionLocal() as db:
                    upsert_raw_records(
                        db,
                        dataset=f"purchase-orders/{oid_int}/items",
                        records=[x for x in items if isinstance(x, dict)],
                        id_fields=("id", "itemId", "productId", "code"),
                    )

        cursor = _add_month(cursor, 1)

    print("OK: backfill completo finalizado")


def main() -> None:
    print("Testando conexão no Postgres...")
    _ensure_database_exists()

    url = _database_url()
    # valida conexão via SQLAlchemy
    engine = create_engine(url, future=True, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
    print("OK: conexão SQLAlchemy funcionando")

    asyncio.run(_run_backfill())


if __name__ == "__main__":
    main()
