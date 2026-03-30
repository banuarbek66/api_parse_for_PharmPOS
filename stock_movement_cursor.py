# models/stock_movement_cursor.py
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, String
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class StockMovementCursor(Base):
    __tablename__ = "stock_movement_cursors"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    provider_name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    last_hourly_processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False), nullable=True, index=True
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=datetime.utcnow
    )

    __table_args__ = (Index("ix_stock_movement_cursors_provider", "provider_name"),)
