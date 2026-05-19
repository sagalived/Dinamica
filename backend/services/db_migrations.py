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

        # sprints.bitrix_group_id, sprints.bitrix_last_pull_at
        try:
            if _table_exists(engine, "sprints"):
                if not _sqlite_has_column(engine, "sprints", "bitrix_group_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_group_id INTEGER"))
                    logger.info("SQLite migration: added sprints.bitrix_group_id")
                if not _sqlite_has_column(engine, "sprints", "bitrix_last_pull_at"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_last_pull_at DATETIME"))
                    logger.info("SQLite migration: added sprints.bitrix_last_pull_at")

                # sprints.bitrix_crm_*, sprints.contract_ref
                if not _sqlite_has_column(engine, "sprints", "bitrix_crm_entity_type_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_entity_type_id INTEGER"))
                    logger.info("SQLite migration: added sprints.bitrix_crm_entity_type_id")
                if not _sqlite_has_column(engine, "sprints", "bitrix_crm_category_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_category_id INTEGER"))
                    logger.info("SQLite migration: added sprints.bitrix_crm_category_id")
                if not _sqlite_has_column(engine, "sprints", "bitrix_crm_last_pull_at"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_last_pull_at DATETIME"))
                    logger.info("SQLite migration: added sprints.bitrix_crm_last_pull_at")
                if not _sqlite_has_column(engine, "sprints", "contract_ref"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN contract_ref TEXT"))
                    logger.info("SQLite migration: added sprints.contract_ref")
        except Exception as exc:
            logger.warning("SQLite migration failed for sprints Bitrix columns: %s", exc)

        # cards.bitrix_task_id, cards.bitrix_last_pull_at
        try:
            if _table_exists(engine, "cards"):
                if not _sqlite_has_column(engine, "cards", "bitrix_task_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_task_id INTEGER"))
                    logger.info("SQLite migration: added cards.bitrix_task_id")
                if not _sqlite_has_column(engine, "cards", "bitrix_last_pull_at"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_last_pull_at DATETIME"))
                    logger.info("SQLite migration: added cards.bitrix_last_pull_at")

                # cards.bitrix_crm_*
                if not _sqlite_has_column(engine, "cards", "bitrix_crm_entity_type_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_entity_type_id INTEGER"))
                    logger.info("SQLite migration: added cards.bitrix_crm_entity_type_id")
                if not _sqlite_has_column(engine, "cards", "bitrix_crm_category_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_category_id INTEGER"))
                    logger.info("SQLite migration: added cards.bitrix_crm_category_id")
                if not _sqlite_has_column(engine, "cards", "bitrix_crm_item_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_item_id INTEGER"))
                    logger.info("SQLite migration: added cards.bitrix_crm_item_id")
                if not _sqlite_has_column(engine, "cards", "bitrix_crm_stage_id"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_stage_id TEXT"))
                    logger.info("SQLite migration: added cards.bitrix_crm_stage_id")
                if not _sqlite_has_column(engine, "cards", "bitrix_crm_last_pull_at"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_last_pull_at DATETIME"))
                    logger.info("SQLite migration: added cards.bitrix_crm_last_pull_at")
        except Exception as exc:
            logger.warning("SQLite migration failed for cards Bitrix columns: %s", exc)

        # contracts.kind
        try:
            if _table_exists(engine, "contracts"):
                if not _sqlite_has_column(engine, "contracts", "kind"):
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE contracts ADD COLUMN kind TEXT NOT NULL DEFAULT 'privado'"))
                    logger.info("SQLite migration: added contracts.kind")
        except Exception as exc:
            logger.warning("SQLite migration failed for contracts.kind: %s", exc)
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

        # sprints.bitrix_group_id, sprints.bitrix_last_pull_at
        try:
            if _table_exists(engine, "sprints"):
                with engine.connect() as conn:
                    has_group = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'bitrix_group_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_pull = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'bitrix_last_pull_at'
                            LIMIT 1
                            """
                        )
                    ).first()

                    has_crm_entity = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'bitrix_crm_entity_type_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_cat = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'bitrix_crm_category_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_pull = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'bitrix_crm_last_pull_at'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_contract = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'sprints' AND column_name = 'contract_ref'
                            LIMIT 1
                            """
                        )
                    ).first()
                if not has_group:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_group_id INTEGER"))
                    logger.info("Postgres migration: added sprints.bitrix_group_id")
                if not has_pull:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_last_pull_at TIMESTAMP"))
                    logger.info("Postgres migration: added sprints.bitrix_last_pull_at")

                if not has_crm_entity:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_entity_type_id INTEGER"))
                    logger.info("Postgres migration: added sprints.bitrix_crm_entity_type_id")
                if not has_crm_cat:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_category_id INTEGER"))
                    logger.info("Postgres migration: added sprints.bitrix_crm_category_id")
                if not has_crm_pull:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN bitrix_crm_last_pull_at TIMESTAMP"))
                    logger.info("Postgres migration: added sprints.bitrix_crm_last_pull_at")
                if not has_contract:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE sprints ADD COLUMN contract_ref VARCHAR(120)"))
                    logger.info("Postgres migration: added sprints.contract_ref")
        except Exception as exc:
            logger.warning("Postgres migration failed for sprints Bitrix columns: %s", exc)

        # cards.bitrix_task_id, cards.bitrix_last_pull_at
        try:
            if _table_exists(engine, "cards"):
                with engine.connect() as conn:
                    has_task = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_task_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_pull = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_last_pull_at'
                            LIMIT 1
                            """
                        )
                    ).first()

                    has_crm_entity = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_crm_entity_type_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_cat = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_crm_category_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_item = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_crm_item_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_stage = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_crm_stage_id'
                            LIMIT 1
                            """
                        )
                    ).first()
                    has_crm_pull = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'cards' AND column_name = 'bitrix_crm_last_pull_at'
                            LIMIT 1
                            """
                        )
                    ).first()
                if not has_task:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_task_id INTEGER"))
                    logger.info("Postgres migration: added cards.bitrix_task_id")
                if not has_pull:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_last_pull_at TIMESTAMP"))
                    logger.info("Postgres migration: added cards.bitrix_last_pull_at")

                if not has_crm_entity:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_entity_type_id INTEGER"))
                    logger.info("Postgres migration: added cards.bitrix_crm_entity_type_id")
                if not has_crm_cat:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_category_id INTEGER"))
                    logger.info("Postgres migration: added cards.bitrix_crm_category_id")
                if not has_crm_item:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_item_id INTEGER"))
                    logger.info("Postgres migration: added cards.bitrix_crm_item_id")
                if not has_crm_stage:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_stage_id VARCHAR(120)"))
                    logger.info("Postgres migration: added cards.bitrix_crm_stage_id")
                if not has_crm_pull:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE cards ADD COLUMN bitrix_crm_last_pull_at TIMESTAMP"))
                    logger.info("Postgres migration: added cards.bitrix_crm_last_pull_at")
        except Exception as exc:
            logger.warning("Postgres migration failed for cards Bitrix columns: %s", exc)

        # contracts.kind
        try:
            if _table_exists(engine, "contracts"):
                with engine.connect() as conn:
                    has_kind = conn.execute(
                        text(
                            """
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'contracts' AND column_name = 'kind'
                            LIMIT 1
                            """
                        )
                    ).first()
                if not has_kind:
                    with engine.begin() as conn:
                        conn.execute(text("ALTER TABLE contracts ADD COLUMN kind VARCHAR(40) NOT NULL DEFAULT 'privado'"))
                    logger.info("Postgres migration: added contracts.kind")
        except Exception as exc:
            logger.warning("Postgres migration failed for contracts.kind: %s", exc)
        return

    # Outros dialetos: não aplicamos migrações automáticas.
    return


def ensure_database_schema(engine: Engine) -> None:
    """Alias para clareza (mantém retrocompatibilidade)."""
    ensure_sqlite_schema(engine)
