# tasks/scheduler.py
# ============================================================
# SCHEDULER ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from database import SessionLocal
from services import SyncService, PostProcessService


def get_session() -> Session:
    """
    Создание сессии БД для задач
    """
    return SessionLocal()


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
    print("⏰ [TASK] Hourly sync started")

    db = get_session()
    try:
        result = SyncService.run_hourly_sync(db)
        
        print(f"✅ [TASK] Hourly sync completed: {result}")
    except Exception as e:
        print(f"❌ [TASK] Hourly sync failed: {e}")
    finally:
        db.close()


def postprocess_job():
    """
    ТЯЖЁЛАЯ ЗАДАЧА:
    - canonical resolve
    - product_compare rebuild
    """
    print("⚙️ [TASK] Postprocess started")

    db = get_session()
    try:
        result = PostProcessService.rebuild_all(db)
        print(f"✅ [TASK] Postprocess completed: {result}")
    except Exception as e:
        print(f"❌ [TASK] Postprocess failed: {e}")
    finally:
        db.close()


def daily_snapshot_job():
    """
    23:50 — перенос hourly → daily
    """
    print("🕚 [TASK] Daily snapshot started")

    db = get_session()
    try:
        count = SyncService.run_daily_snapshot(db)
        print(f"✅ [TASK] Daily snapshot completed: {count}")
    except Exception as e:
        print(f"❌ [TASK] Daily snapshot failed: {e}")
    finally:
        db.close()


def cleanup_hourly_job():
    """
    00:00 — ОЧИСТКА hourly (1 раз в день)
    """
    print("🕛 [TASK] Hourly cleanup started")

    db = get_session()
    try:
        SyncService.cleanup_hourly_table(db)
        print("✅ [TASK] Hourly table cleaned")
    except Exception as e:
        print(f"❌ [TASK] Cleanup failed: {e}")
    finally:
        db.close()


def first_sync_of_day():
    """
    01:00 — первый hourly нового дня
    """
    print("🕐 [TASK] First daily sync started")

    db = get_session()
    try:
        result = SyncService.run_hourly_sync(db)
        
        print(f"✅ [TASK] First sync completed: {result}")
    except Exception as e:
        print(f"❌ [TASK] First sync failed: {e}")
    finally:
        db.close()


# ------------------------------------------------------------
# ЗАПУСК ПЛАНИРОВЩИКА
# ------------------------------------------------------------

def start_scheduler():
    scheduler = BackgroundScheduler(timezone="Asia/Almaty")

    # ⏰ Каждый час → HOURLY SYNC
    scheduler.add_job(
        hourly_sync_job,
        CronTrigger(minute=0),
        id="hourly_sync",
        replace_existing=True
    )

    # ⚙️ Каждый час +10 минут → POSTPROCESS
    scheduler.add_job(
        postprocess_job,
        CronTrigger(minute=10),
        id="postprocess",
        replace_existing=True
    )

    # 🕚 23:50 → DAILY SNAPSHOT
    scheduler.add_job(
        daily_snapshot_job,
        CronTrigger(hour=23, minute=50),
        id="daily_snapshot",
        replace_existing=True
    )

    # 🕛 00:00 → CLEANUP HOURLY
    scheduler.add_job(
        cleanup_hourly_job,
        CronTrigger(hour=0, minute=0),
        id="cleanup_hourly",
        replace_existing=True
    )

    # 🕐 01:00 → FIRST SYNC OF DAY
    scheduler.add_job(
        first_sync_of_day,
        CronTrigger(hour=1, minute=0),
        id="first_sync",
        replace_existing=True
    )

    scheduler.start()
    print("🚀 Scheduler started")

    return scheduler
