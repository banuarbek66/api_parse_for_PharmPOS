# main.py
# ============================================================
# ENTRY POINT ДЛЯ PHARM-POS SUPPLIER AGGREGATOR
# ============================================================
import os
import sys

from tasks.scheduler import start_scheduler

# ✅ Жёсткая принудительная установка UTF-8 для Python
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["LANG"] = "C.UTF-8"
os.environ["LC_ALL"] = "C.UTF-8"

# ✅ Для Windows (на будущее)
if sys.platform == "win32":
    os.system("chcp 65001 > nul")

from contextlib import asynccontextmanager

from fastapi import FastAPI

from database import engine
from tasks.scheduler import start_scheduler
from v1.router import router as v1_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Запускается при старте и остановке приложения
    """

    # 1. Инициализация базы данных (создание таблиц)

    # 2. Запуск scheduler (cron-задачи)
    print("⏰ Starting scheduler...")
    scheduler = start_scheduler()

    yield

    # 3. При остановке — выключаем scheduler
    scheduler.shutdown()
    print("🛑 Scheduler stopped")


app = FastAPI(
    title="PharmPOS Supplier Aggregator",
    version="1.0.0",
    description="API for parsing, normalizing & syncing suppliers data",
    lifespan=lifespan,
)


@app.on_event("startup")
def on_startup():
    start_scheduler()


# Подключение API версии 1
app.include_router(v1_router)


# Тестовый маршрут
@app.get("/")
def root():
    return {
        "message": "PharmPOS Supplier Aggregator is running 🚀",
        "docs": "/docs",
        "version": "1.0.0",
    }


from sqladmin import Admin

from views import (
    CityResponseAdmin,
    DailyProductAdmin,
    HourlyProductAdmin,
    ProductCompareAdmin,
    StockMovementAdmin,
    SupllierSrokAdmin,
    SupplierAdmin,
    SupplierMappingAdmin,
    SupplierUnitAdmin,
    ClietnAdmin,
)

# ✅ ADMIN
admin = Admin(app, engine)

# ---- REGISTRATION OF ADMIN VIEWS ----
admin.add_view(SupplierAdmin)
admin.add_view(SupplierMappingAdmin)
admin.add_view(CityResponseAdmin)
admin.add_view(SupllierSrokAdmin)
admin.add_view(SupplierUnitAdmin)
admin.add_view(HourlyProductAdmin)
admin.add_view(DailyProductAdmin)
admin.add_view(StockMovementAdmin)
admin.add_view(ProductCompareAdmin)
admin.add_view(ClietnAdmin)
