# tasks/scheduler.py
# ============================================================
# SCHEDULER ДЛЯ PHARM-POS AGGREGATOR
# ============================================================

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from database import SessionLocal
from services import SyncService


def get_session() -> Session:
    """
    Создание сессии БД для задач
    """
    return SessionLocal()


# ------------------------------------------------------------
# ЗАДАЧИ
# ------------------------------------------------------------

def hourly_sync_job():
    print("⏰ [TASK] Hourly sync started")

    db = get_session()
    try:
        result = SyncService.run_hourly_sync(db)
        print(f"✅ [TASK] Hourly sync completed: {result}")
    finally:
        db.close()


def daily_snapshot_job():
    print("🕚 [TASK] Daily snapshot started")

    db = get_session()
    try:
        count = SyncService.run_daily_snapshot(db)
        print(f"✅ [TASK] Daily snapshot completed: {count}")
    finally:
        db.close()


def cleanup_hourly_job():
    print("🕛 [TASK] Hourly cleanup started")

    db = get_session()
    try:
        SyncService.cleanup_hourly_table(db)
        print("✅ [TASK] Hourly table cleaned")
    finally:
        db.close()


def first_sync_of_day():
    print("🕐 [TASK] First daily sync started")

    db = get_session()
    try:
        result = SyncService.run_hourly_sync(db)
        print(f"✅ [TASK] First sync completed: {result}")
    finally:
        db.close()


# ------------------------------------------------------------
# ЗАПУСК ПЛАНИРОВЩИКА
# ------------------------------------------------------------

def start_scheduler():
    scheduler = BackgroundScheduler(timezone="Asia/Almaty")

    # Каждый час
    scheduler.add_job(
        hourly_sync_job,
        CronTrigger(minute='0'),
        id="hourly_sync",
        replace_existing=True
    )

    # В 23:50 → сохранить daily snapshot
    scheduler.add_job(
        daily_snapshot_job,
        CronTrigger(hour=23, minute=50),
        id="daily_snapshot",
        replace_existing=True
    )

    # В 00:00 → очистка таблицы hourly
    scheduler.add_job(
        cleanup_hourly_job,
        CronTrigger(hour=0, minute=0),
        id="cleanup_hourly",
        replace_existing=True
    )

    # В 01:00 → первый запуск нового дня
    scheduler.add_job(
        first_sync_of_day,
        CronTrigger(hour=1, minute=0),
        id="first_sync",
        replace_existing=True
    )

    scheduler.start()
    print("🚀 Scheduler started")

    return scheduler
