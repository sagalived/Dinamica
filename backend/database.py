from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from backend.config import DATABASE_URL
from backend.services.db_migrations import ensure_sqlite_schema


class Base(DeclarativeBase):
    pass


_is_sqlite = DATABASE_URL.startswith("sqlite")
engine = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=not _is_sqlite,
    connect_args={"check_same_thread": False} if _is_sqlite else {},
)

# `create_all()` não adiciona colunas novas em tabelas existentes.
# Garantimos migrações leves sempre que o engine SQLite existir.
ensure_sqlite_schema(engine)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
