from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _sqlite_has_column(engine: Engine, table: str, column: str) -> bool:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).mappings().all()
    return any(str(r.get("name") or "") == column for r in rows)


def _table_exists(engine: Engine, table: str) -> bool:
    dialect = engine.dialect.name
    if dialect == "sqlite":
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=:t LIMIT 1"
                ),
                {"t": table},
            ).first()
        return row is not None

    if dialect in {"postgresql", "postgres"}:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = :t
                    LIMIT 1
                    """
                ),
                {"t": table},
            ).first()
        return row is not None

    return False


def ensure_sqlite_schema(engine: Engine) -> None:
    """Aplica migrações leves de schema (sem Alembic).

    Observação: `Base.metadata.create_all()` NÃO adiciona colunas novas em tabelas existentes.
    """

    dialect = engine.dialect.name

    if dialect == "sqlite":
        # buildings.active
        try:
            if not _table_exists(engine, "buildings"):
                return
            if not _sqlite_has_column(engine, "buildings", "active"):
                with engine.begin() as conn:
                    conn.execute(
                        text("ALTER TABLE buildings ADD COLUMN active BOOLEAN NOT NULL DEFAULT 1")
                    )
                logger.info("SQLite migration: added buildings.active")
        except Exception as exc:
            # Não derruba o app; apenas registra.
            logger.warning("SQLite migration failed for buildings.active: %s", exc)
        return

    if dialect in {"postgresql", "postgres"}:
        try:
            if not _table_exists(engine, "buildings"):
                return
            with engine.connect() as conn:
                exists = conn.execute(
                    text(
                        """
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'buildings'
                          AND column_name = 'active'
                        LIMIT 1
                        """
                    )
                ).first()
            if not exists:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "ALTER TABLE buildings ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE"
                        )
                    )
                logger.info("Postgres migration: added buildings.active")
        except Exception as exc:
            logger.warning("Postgres migration failed for buildings.active: %s", exc)
        return

    # Outros dialetos: não aplicamos migrações automáticas.
    return


def ensure_database_schema(engine: Engine) -> None:
    """Alias para clareza (mantém retrocompatibilidade)."""
    ensure_sqlite_schema(engine)
