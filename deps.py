# deps.py
# ============================================================
# DEPENDENCIES ДЛЯ FASTAPI И СЕРВИСОВ
# ============================================================

from database import SessionLocal


def get_db():
    """
    Dependency для получения сессии БД.
    Используется в FastAPI и задачах.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
