from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from deps import get_db
from services import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/hot-products")
def get_hot_products(
    period: str = Query(
        "today",
        description="Период анализа: today | week | month | custom",
        regex="^(today|week|month|custom)$",
    ),
    start_date: Optional[str] = Query(
        None,
        description="Дата начала периода (YYYY-MM-DD). Используется только если period=custom",
    ),
    end_date: Optional[str] = Query(
        None,
        description="Дата конца периода (YYYY-MM-DD). Используется только если period=custom",
    ),
    provider_name: Optional[str] = Query(
        None, description="Название поставщика (опционально)"
    ),
    city: Optional[str] = Query(None, description="Город (опционально)"),
    limit: int = Query(10, ge=1, le=100, description="Количество товаров в топе"),
    db: Session = Depends(get_db),
):
    """
    Получить список «горячих товаров» — быстрых продаж
    за выбранный период.

    period = today  → c 01:00 сегодня до сейчас
    period = week   → последние 7 дней
    period = month  → последние 30 дней
    period = custom → произвольный период (нужны start_date и end_date)
    """

    # Преобразуем start/end в datetime для custom
    start_dt = None
    end_dt = None

    if period == "custom":
        if not start_date or not end_date:
            return {
                "status": "error",
                "message": "Для period=custom требуется start_date и end_date",
            }

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return {
                "status": "error",
                "message": "Неверный формат дат. Формат должен быть YYYY-MM-DD.",
            }

    result = AnalyticsService.get_hot_products_period(
        db=db,
        period=period,
        start_date=start_dt,
        end_date=end_dt,
        provider_name=provider_name,
        city=city,
        limit=limit,
    )

    return result
