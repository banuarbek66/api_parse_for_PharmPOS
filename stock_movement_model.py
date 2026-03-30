# models/stock_movement.py
from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from sqlalchemy import DateTime
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class StockMovementType(str, Enum):
    sale = "sale"  # delta < 0
    restock = "restock"  # delta > 0
    correction = "correction"
    unknown = "unknown"


class StockMovement(Base):
    __tablename__ = "stock_movements"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Идентификаторы (можно хранить и sku_uid, и canonical_id)
    provider_name: Mapped[str] = mapped_column(String(100), index=True)
    city: Mapped[str | None] = mapped_column(String(100), index=True, nullable=True)

    # Optional: если уже используешь canonical_id (рекомендую для аналитики)
    canonical_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), nullable=True, index=True
    )

    # Optional: если хочешь оставлять связь с sku_uid поставщика
    sku_uid: Mapped[str | None] = mapped_column(String(200), nullable=True, index=True)

    sku_name: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Снимки: откуда → куда
    stock_before: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    stock_after: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)

    # delta = after - before
    delta: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)

    movement_type: Mapped[StockMovementType] = mapped_column(
        SQLEnum(StockMovementType, name="stock_movement_type_enum"),
        default=StockMovementType.unknown,
        nullable=False,
        index=True,
    )

    source: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # "hourly" | "daily"

    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False), default=datetime.utcnow, index=True
    )

    __table_args__ = (
        Index(
            "ix_stock_movements_provider_city_time",
            "provider_name",
            "city",
            "snapshot_at",
        ),
        Index("ix_stock_movements_canonical_time", "canonical_id", "snapshot_at"),
    )
