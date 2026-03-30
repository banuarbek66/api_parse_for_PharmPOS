# schemas/stock_movement.py
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class StockMovementAggItem(BaseModel):
    # то, что нужно AnalyticsService
    provider_name: str
    city: str | None

    canonical_id: UUID | None = None
    sku_uid: str | None = None
    sku_name: str | None = None

    sold_qty: float
    restocked_qty: float
    net_delta: float

    from_dt: datetime
    to_dt: datetime
