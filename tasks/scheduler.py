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
from services import SyncService, PostProcessService


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
    """
    ТЯЖЁЛАЯ ЗАДАЧА:
    - canonical resolve
    - product_compare rebuild

    Требование:
    - при запуске сначала очищаем витрину product_compare
      (внутри rebuild_from_hourly уже есть TRUNCATE, но оставляем защиту от частичных запусков)
    """
    _pid_log("⚙️ [TASK] Postprocess started")

    # lock на postprocess
    LOCK_KEY = 10002

    with session_scope() as db:
        got_lock = _try_advisory_lock(db, LOCK_KEY)
        if not got_lock:
            _pid_log("⚠️ [TASK] Postprocess skipped (lock busy)")
            return

        try:
            # на всякий случай — чтобы при падении в середине не оставалось мусора
            db.execute(text("TRUNCATE TABLE product_compare"))
            db.commit()

            result = PostProcessService.rebuild_all(db)
            _pid_log(f"✅ [TASK] Postprocess completed: {result}")
        except Exception as e:
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
        misfire_grace_time=15 * 60,  # 15 минут
    )

    # ⚙️ Каждый час +10 минут → POSTPROCESS
    scheduler.add_job(
        postprocess_job,
        CronTrigger(minute=45),
        id="postprocess",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30 * 60,  # 30 минут
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
