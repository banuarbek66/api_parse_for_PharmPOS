# tasks/scheduler.py
# ============================================================
# SCHEDULER ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

import os
import time
import atexit
from contextlib import contextmanager
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
from sqlalchemy import text

from database import SessionLocal
from services import SyncService, PostProcessService, StockMovementService
from repositories import PostProcessStateRepo

def _get_max_hourly_created_at(db: Session):
    return db.execute(text("SELECT max(created_at) FROM hourly_products")).scalar()


def get_session() -> Session:
    """
    Создание сессии БД для задач
    """
    return SessionLocal()


@contextmanager
def session_scope():
    """
    Контекстный менеджер для сессии БД:
    - commit/rollback по необходимости
    - гарантированное закрытие
    """
    db = get_session()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _pid_log(msg: str) -> None:
    # systemd/journalctl и так соберёт stdout, но добавим PID и время
    print(f"[{datetime.utcnow().isoformat()}Z][PID:{os.getpid()}] {msg}", flush=True)

def run_stock_movements_only() -> None:
    """
    Отдельный запуск stock movements (если понадобится)
    """
    db: Session = SessionLocal()

    try:
        created = StockMovementService.build_hourly_movements_all(db)
        print(
            f"[STOCK_MOVEMENTS] created={created} at {datetime.utcnow().isoformat()}"
        )

    except Exception as e:
        print(f"[STOCK_MOVEMENTS][ERROR] {e}")
        raise

    finally:
        db.close()
def _try_advisory_lock(db: Session, lock_key: int) -> bool:
    """
    Защита от гонок:
    - если сервис запущен в нескольких процессах/инстансах,
      то задачу выполнит только один.
    """
    # pg_try_advisory_lock возвращает boolean
    return bool(db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}).scalar())


def _advisory_unlock(db: Session, lock_key: int) -> None:
    db.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key})
def nightly_rebuild_job():
    _pid_log("🌙 [TASK] Nightly rebuild started")

    LOCK_KEY = 10006

    with session_scope() as db:
        if not _try_advisory_lock(db, LOCK_KEY):
            _pid_log("⚠️ Nightly rebuild skipped (lock busy)")
            return

        try:
            PostProcessService.rebuild_all(db)
            _pid_log("✅ Nightly rebuild completed")
        except Exception as e:
            _pid_log(f"❌ Nightly rebuild failed: {e}")
            raise
        finally:
            _advisory_unlock(db, LOCK_KEY)


# ------------------------------------------------------------
# ЗАДАЧИ
# ------------------------------------------------------------

def hourly_sync_job():
    """
    Каждый час:
    - асинхронно забираем данные от поставщиков
    - синхронно пишем в hourly_products
    ❗ БЕЗ postprocess
    """
    _pid_log("⏰ [TASK] Hourly sync started")

    # lock на hourly sync
    LOCK_KEY = 10001

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] Hourly sync skipped (lock busy)")
            return

        try:
            result = SyncService.run_hourly_sync(db)
            _pid_log(f"✅ [TASK] Hourly sync completed: {result}")
        except Exception as e:
            _pid_log(f"❌ [TASK] Hourly sync failed: {e}")
            raise
        finally:
            _advisory_unlock(db, LOCK_KEY)


def postprocess_job():
    _pid_log("⚙️ [TASK] Postprocess started")

    LOCK_KEY = 10002

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] Postprocess skipped (lock busy)")
            return

        try:
            state = PostProcessStateRepo.get(db)
            max_hourly = _get_max_hourly_created_at(db)

            if not max_hourly:
                _pid_log("⏭️ [TASK] Postprocess skipped (hourly empty)")
                return

            if state.last_hourly_at and max_hourly <= state.last_hourly_at:
                _pid_log("⏭️ [TASK] Postprocess skipped (no new hourly data)")
                return

            
            

            started = time.time()
            result = PostProcessService.rebuild_all(db)
            duration = round(time.time() - started, 2)

            # refresh max_hourly AFTER rebuild (на всякий случай)
            max_hourly2 = _get_max_hourly_created_at(db)
            PostProcessStateRepo.set_success(db, last_hourly_at=max_hourly2)
            db.commit()

            _pid_log(f"✅ [TASK] Postprocess completed in {duration}s: {result}")

        except Exception as e:
            PostProcessStateRepo.set_failed(db)
            db.commit()
            _pid_log(f"❌ [TASK] Postprocess failed: {e}")
            raise

        finally:
            _advisory_unlock(db, LOCK_KEY)


def daily_snapshot_job():
    """
    23:50 — перенос hourly → daily
    """
    _pid_log("🕚 [TASK] Daily snapshot started")

    LOCK_KEY = 10003

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] Daily snapshot skipped (lock busy)")
            return

        try:
            count = SyncService.run_daily_snapshot(db)
            _pid_log(f"✅ [TASK] Daily snapshot completed: {count}")
        except Exception as e:
            _pid_log(f"❌ [TASK] Daily snapshot failed: {e}")
            raise
        finally:
            _advisory_unlock(db, LOCK_KEY)


def cleanup_hourly_job():
    """
    00:00 — ОЧИСТКА hourly (1 раз в день)
    """
    _pid_log("🕛 [TASK] Hourly cleanup started")

    LOCK_KEY = 10004

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] Hourly cleanup skipped (lock busy)")
            return

        try:
            SyncService.cleanup_hourly_table(db)
            _pid_log("✅ [TASK] Hourly table cleaned")
        except Exception as e:
            _pid_log(f"❌ [TASK] Cleanup failed: {e}")
            raise
        finally:
            _advisory_unlock(db, LOCK_KEY)


def first_sync_of_day():
    """
    01:00 — первый hourly нового дня
    """
    _pid_log("🕐 [TASK] First daily sync started")

    LOCK_KEY = 10005

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] First sync skipped (lock busy)")
            return

        try:
            result = SyncService.run_hourly_sync(db)
            _pid_log(f"✅ [TASK] First sync completed: {result}")
        except Exception as e:
            _pid_log(f"❌ [TASK] First sync failed: {e}")
            raise
        finally:
            _advisory_unlock(db, LOCK_KEY)


# ------------------------------------------------------------
# ЗАПУСК ПЛАНИРОВЩИКА
# ------------------------------------------------------------

def start_scheduler():
    """
    Важно для systemd/uvicorn:
    - BackgroundScheduler живёт в процессе
    - при нескольких воркерах uvicorn/gunicorn задачи могут дублироваться
      -> поэтому добавлены advisory locks
    """
    scheduler = BackgroundScheduler(timezone="Asia/Almaty")

    # ⏰ Каждый час → HOURLY SYNC
    scheduler.add_job(
        hourly_sync_job,
        CronTrigger(minute=0),
        id="hourly_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time= 15 * 60,  # 15 минут
    )

    scheduler.add_job(
        run_stock_movements_only,
        trigger=CronTrigger(minute=40),
        id="stock_movement",
        max_instances=1,
        replace_existing=True,
        coalesce=True,
        misfire_grace_time= 20 * 60,
    )

    # ⚙️ Каждый час +10 минут → POSTPROCESS
    scheduler.add_job(
        postprocess_job,
        CronTrigger(minute=20),
        id="postprocess",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time= 20 * 60,  # 30 минут
    )

    # 🕚 23:50 → DAILY SNAPSHOT
    scheduler.add_job(
        daily_snapshot_job,
        CronTrigger(hour=23, minute=50),
        id="daily_snapshot",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 60,  # 1 час
    )
    scheduler.add_job(
    nightly_rebuild_job,
    CronTrigger(hour=3, minute=0),
    id="nightly_rebuild",
    replace_existing=True,
    max_instances=1,
    coalesce=True,
)

    # 🕛 00:00 → CLEANUP HOURLY
    scheduler.add_job(
        cleanup_hourly_job,
        CronTrigger(hour=0, minute=0),
        id="cleanup_hourly",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 60,  # 1 час
    )

    # 🕐 01:00 → FIRST SYNC OF DAY
    scheduler.add_job(
        first_sync_of_day,
        CronTrigger(hour=1, minute=0),
        id="first_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 60,  # 1 час
    )

    scheduler.start()
    _pid_log("🚀 Scheduler started")

    # graceful shutdown
    atexit.register(lambda: scheduler.shutdown(wait=False))

    return scheduler
