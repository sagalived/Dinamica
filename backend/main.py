import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from backend.config import APP_NAME, BASE_DIR, SIENGE_SYNC_INTERVAL_MINUTES
from backend.database import Base, SessionLocal, engine
from backend.routers import admin, auth, catalog, core, dashboard, kanban, logistics, sienge, operational
from backend.routers.sienge import run_sync_once
from backend.services.bootstrap import ensure_seed_data
from backend.services.db_migrations import ensure_sqlite_schema

# Load environment variables from project-root .env (independente do cwd)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

app = FastAPI(title=APP_NAME, version="2.0.0")
logger = logging.getLogger(__name__)

# Register routers
app.include_router(core.router)
app.include_router(auth.router)
app.include_router(dashboard.router)
app.include_router(admin.router)
app.include_router(catalog.router)
app.include_router(sienge.router)
app.include_router(operational.router)
app.include_router(kanban.router)
app.include_router(logistics.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


async def _run_sienge_scheduler() -> None:
    tz_br = timezone(timedelta(hours=-3))
    target_hours = [0, 6, 12, 18]
    while True:
        now = datetime.now(tz_br)
        next_run = None
        for h in target_hours:
            candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if candidate > now:
                next_run = candidate
                break
        if next_run is None:
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        wait_seconds = (next_run - now).total_seconds()
        logger.info("SIENGE scheduler aguardando %d segundos (próxima execução: %s)", wait_seconds, next_run.strftime("%Y-%m-%d %H:%M:%S"))
        await asyncio.sleep(wait_seconds)
        try:
            threading.Thread(
                target=_run_sienge_sync_blocking,
                args=("scheduler",),
                daemon=True,
            ).start()
        except Exception as exc:
            logger.warning("SIENGE auto-sync falhou: %s", exc)


def _run_sienge_sync_blocking(source: str) -> None:
    try:
        with SessionLocal() as db:
            result = asyncio.run(run_sync_once(db, source=source))
            if result.get("in_progress"):
                logger.info("SIENGE sync ignorado (%s): sincronizacao ja em andamento.", source)
                return
        logger.info("SIENGE %s sync concluido.", source)
    except Exception as exc:
        logger.warning("SIENGE %s sync falhou: %s", source, exc)


@app.on_event("startup")
async def on_startup() -> None:
    app.state.database_ready = False
    app.state.database_error = None
    app.state.sienge_scheduler_task = None
    try:
        Base.metadata.create_all(bind=engine)
        ensure_sqlite_schema(engine)
        with Session(engine) as db:
            ensure_seed_data(db)
        app.state.database_ready = True

        # Nao bloquear o startup: sync inicial roda em background.
        threading.Thread(
            target=_run_sienge_sync_blocking,
            args=("startup",),
            daemon=True,
        ).start()
        app.state.sienge_scheduler_task = asyncio.create_task(_run_sienge_scheduler())
    except Exception as exc:
        app.state.database_error = str(exc)
        logger.error("Erro critico no startup: %s", exc, exc_info=True)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    scheduler_task = getattr(app.state, "sienge_scheduler_task", None)
    if scheduler_task is not None:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass


# Servir frontend React (dist/) — deve ficar por último
_DIST_DIR = BASE_DIR / "dist"
if _DIST_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(_DIST_DIR / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_spa(full_path: str) -> FileResponse:
        return FileResponse(str(_DIST_DIR / "index.html"))
