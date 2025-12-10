# core.py
# ============================================================
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ ДЛЯ PHARM-POS SUPPLIER AGGREGATOR
# ============================================================
import models 
from datetime import time
from sqlalchemy.orm import Session
# ------------------------------------------------------------
# ⏱ ЧАСОВОЕ ОБНОВЛЕНИЕ (РАБОТАЕТ 24/7)
# ------------------------------------------------------------
# Сбор данных от поставщиков идёт каждый час.
# В 00:00 таблица очищается, в 01:00 начинается новый цикл.
# ------------------------------------------------------------

HOURLY_SYNC_ACTIVE = True      # Флаг активности синхронизации
HOURLY_SYNC_INTERVAL = 1       # Каждые 1 час


# ------------------------------------------------------------
# 🕒 Время дневного snapshot
# ------------------------------------------------------------
# В 23:50 сохраняем последние данные в daily_products
DAILY_SNAPSHOT_AT = time(hour=23, minute=50)


# ------------------------------------------------------------
# 🧹 Время очистки hourly таблицы
# ------------------------------------------------------------
# В 00:00 очищаем hourly_products полностью
DAILY_CLEANUP_AT = time(hour=0, minute=0)


# ------------------------------------------------------------
# 🟦 Время начала нового цикла
# ------------------------------------------------------------
# В 01:00 начинается сбор данных заново
DAILY_RESTART_AT = time(hour=1, minute=0)


# ------------------------------------------------------------
# 🌙 Ночной режим
# ------------------------------------------------------------
# С 00:00 до 01:00 hourly пуста → используются данные из daily_products
NIGHT_MODE_START = time(hour=0, minute=0)
NIGHT_MODE_END   = time(hour=1, minute=0)


# ------------------------------------------------------------
# 🌐 Глобальные параметры проекта
# ------------------------------------------------------------
PROJECT_NAME = "PharmPOS Supplier Aggregator"
PROJECT_VERSION = "1.1.0"


# ------------------------------------------------------------
# 🔧 Настройки HTTP-запросов
# ------------------------------------------------------------
HTTP_TIMEOUT = 120     # таймаут запросов к API
HTTP_RETRIES = 2        # повторные попытки
HTTP_BACKOFF = 1        # задержка между попытками (сек)


# ------------------------------------------------------------
# 📌 ОГРАНИЧЕНИЕ (СТРАХОВКА)
# ------------------------------------------------------------
# По твоему решению: ОСТАВЛЯЕМ, но ставим ОЧЕНЬ БОЛЬШОЕ значение
MAX_PRODUCTS_PER_PROVIDER = 10000000

ALLOW_EMPTY_FIELDS = True

from schemas import SupplierUnitCreate

def create_or_update_supplier_unit(db: Session, data: SupplierUnitCreate):

    existing = (
        db.query(models.SupplierUnit)
        .filter(
            models.SupplierUnit.provider_name == data.provider_name,
            models.SupplierUnit.supplier_unit == data.supplier_unit,
        )
        .first()
    )

    if existing:
        existing.normalized_unit = data.normalized_unit
        db.commit()
        db.refresh(existing)
        return existing

    new = models.SupplierUnit(
        provider_name=data.provider_name,
        supplier_unit=data.supplier_unit,
        normalized_unit=data.normalized_unit
    )

    db.add(new)
    db.commit()
    db.refresh(new)

    return new
