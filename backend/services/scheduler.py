"""
Scheduler de sincronização automática de títulos a receber.
Executa sincronização 4x/dia: 00:00, 06:00, 12:00, 18:00
"""
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.services.sienge_receivable_reconciliation import sync_receivable_titles_from_sienge
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

scheduler: AsyncIOScheduler | None = None


async def automatic_sync_job():
    """Job que executa sincronização automática"""
    db = SessionLocal()
    try:
        logger.info("Iniciando sincronização automática de títulos a receber...")
        result = await sync_receivable_titles_from_sienge(db)
        logger.info(f"Sincronização concluída: {result}")
    except Exception as e:
        logger.error(f"Erro em sincronização automática: {str(e)}", exc_info=True)
    finally:
        db.close()


def init_scheduler():
    """Inicializa scheduler com jobs automáticos"""
    global scheduler
    
    if scheduler is not None:
        logger.warning("Scheduler já está inicializado")
        return scheduler
    
    try:
        scheduler = AsyncIOScheduler()
        
        # Adiciona jobs para 00:00, 06:00, 12:00, 18:00
        times = ["00:00", "06:00", "12:00", "18:00"]
        for time_str in times:
            hour, minute = map(int, time_str.split(":"))
            logger.info(f"Agendando sync automático em {time_str}")
            scheduler.add_job(
                automatic_sync_job,
                trigger=CronTrigger(hour=hour, minute=minute),
                id=f"auto_sync_{time_str}",
                name=f"Sincronização automática {time_str}",
                replace_existing=True,
            )
        
        scheduler.start()
        logger.info("✓ Scheduler iniciado com sucesso")
        return scheduler
    except Exception as e:
        logger.error(f"Erro ao inicializar scheduler: {str(e)}", exc_info=True)
        scheduler = None
        return None


def stop_scheduler():
    """Para scheduler"""
    global scheduler
    if scheduler and scheduler.running:
        scheduler.shutdown()
        scheduler = None
        logger.info("Scheduler parado")


def get_scheduler() -> AsyncIOScheduler | None:
    """Retorna scheduler existente"""
    return scheduler


def get_next_sync_time() -> datetime | None:
    """Retorna próximo tempo de sync automático"""
    if not scheduler or not scheduler.running:
        return None
    
    for job in scheduler.get_jobs():
        if "auto_sync" in job.id:
            return job.next_run_time
    
    return None
