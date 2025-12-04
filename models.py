# models.py
# ============================================================
# ВСЕ МОДЕЛИ ДЛЯ ПРОЕКТА PHARM-POS SUPPLIER AGGREGATOR
# ============================================================

import uuid
from datetime import datetime, date
from typing import List, Optional

from sqlalchemy import (
    String,
    DateTime,
    Date,
    Boolean,
    ForeignKey,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


# ============================================================
# 1. Supplier Mapping
# ============================================================

class SupplierMapping(Base):
    __tablename__ = "supplier_mapping"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    provider_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)

    format: Mapped[str] = mapped_column(String(10), default="json", nullable=False)
    items_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # producer info
    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))

    # Data fields from supplier
    sku_uid: Mapped[Optional[str]] = mapped_column(String(255))
    sku_name: Mapped[Optional[str]] = mapped_column(String(255))
    sku_price: Mapped[Optional[str]] = mapped_column(String(255))
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255))

    sku: Mapped[Optional[str]] = mapped_column(String(255))
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255))
    sku_barcodes: Mapped[Optional[str]] = mapped_column(String(255))
    sku_srok: Mapped[Optional[str]] = mapped_column(String(255))
    sku_step: Mapped[Optional[str]] = mapped_column(String(255))
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255))
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255))
    sku_box: Mapped[Optional[str]] = mapped_column(String(255))

    unit: Mapped[Optional[str]] = mapped_column(String(255))

    min_order: Mapped[Optional[str]] = mapped_column(String(255))

    # City logic
    city_path: Mapped[Optional[str]] = mapped_column(String(500))
    city_in_params: Mapped[bool] = mapped_column(Boolean, default=False)
    city_in_body: Mapped[bool] = mapped_column(Boolean, default=False)
    city_in_headers: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ============================================================
# 2. Supplier
# ============================================================

class Supplier(Base):
    __tablename__ = "suppliers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    provider_name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    provider_bin: Mapped[Optional[str]] = mapped_column(String(20))

    city_param_name: Mapped[Optional[str]] = mapped_column(String(50), default="city_id")

    # URLs
    json_url_get_price: Mapped[Optional[str]] = mapped_column(Text)
    json_url_get_address: Mapped[Optional[str]] = mapped_column(Text)
    json_url_get_order: Mapped[Optional[str]] = mapped_column(Text)

    xml_url_get_price: Mapped[Optional[str]] = mapped_column(Text)
    xml_url_get_address: Mapped[Optional[str]] = mapped_column(Text)
    xml_url_get_order: Mapped[Optional[str]] = mapped_column(Text)

    login: Mapped[Optional[str]] = mapped_column(String(255))
    password: Mapped[Optional[str]] = mapped_column(String(255))

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ============================================================
# 3. Hourly Product
# ============================================================

class HourlyProduct(Base):
    __tablename__ = "hourly_products"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    provider_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("suppliers.id"), nullable=False
    )

    provider_name: Mapped[str] = mapped_column(String(255), index=True)
    provider_bin: Mapped[Optional[str]] = mapped_column(String(50))

    city: Mapped[Optional[str]] = mapped_column(String(100))

    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))

    sku_uid: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    sku_name: Mapped[Optional[str]] = mapped_column(String(500))
    sku_price: Mapped[Optional[str]] = mapped_column(String(255))
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255))

    sku: Mapped[Optional[str]] = mapped_column(String(255))
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255))

    sku_barcodes: Mapped[Optional[List[str]]] = mapped_column(JSONB)

    sku_srok: Mapped[Optional[str]] = mapped_column(String(255))
    sku_step: Mapped[Optional[str]] = mapped_column(String(255))
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255))
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255))
    sku_box: Mapped[Optional[str]] = mapped_column(String(255))

    # 👉 unit исправлено — допускает NULL, но SyncService всегда подставляет "упаковка"
    unit: Mapped[Optional[str]] = mapped_column(String(255))

    min_order: Mapped[Optional[str]] = mapped_column(String(255))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ============================================================
# 4. Daily Product
# ============================================================

class DailyProduct(Base):
    __tablename__ = "daily_products"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    provider_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("suppliers.id"), nullable=False
    )

    provider_name: Mapped[str] = mapped_column(String(255))
    provider_bin: Mapped[Optional[str]] = mapped_column(String(50))

    city: Mapped[Optional[str]] = mapped_column(String(100))

    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))

    sku_uid: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    sku_name: Mapped[Optional[str]] = mapped_column(String(500))
    sku_price: Mapped[Optional[str]] = mapped_column(String(255))
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255))

    sku: Mapped[Optional[str]] = mapped_column(String(255))
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255))

    sku_barcodes: Mapped[Optional[List[str]]] = mapped_column(JSONB)

    sku_srok: Mapped[Optional[str]] = mapped_column(String(255))
    sku_step: Mapped[Optional[str]] = mapped_column(String(255))
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255))
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255))
    sku_box: Mapped[Optional[str]] = mapped_column(String(255))

    unit: Mapped[Optional[str]] = mapped_column(String(255))

    min_order: Mapped[Optional[str]] = mapped_column(String(255))

    snapshot_date: Mapped[date] = mapped_column(Date, default=date.today)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


# ============================================================
# 5. City + SupplierUnit
# ============================================================

class SupplierCity(Base):
    __tablename__ = "supplier_cities"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_name: Mapped[str] = mapped_column(String(255), index=True)
    supplier_city_code: Mapped[str] = mapped_column(String(255), index=True)
    normalized_city: Mapped[str] = mapped_column(String(255), index=True)


class CityResponse(Base):
    __tablename__ = "city_responses"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_name: Mapped[str] = mapped_column(String(255), index=True)

    supplier_city_code: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    supplier_city_name: Mapped[Optional[str]] = mapped_column(String(255), index=True)

    normalized_city: Mapped[str] = mapped_column(String(255), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class SupplierUnit(Base):
    __tablename__ = "supplier_units"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    provider_name: Mapped[str] = mapped_column(String(100), index=True)
    supplier_unit: Mapped[str] = mapped_column(String(100), index=True)
    normalized_unit: Mapped[str] = mapped_column(String(100), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
