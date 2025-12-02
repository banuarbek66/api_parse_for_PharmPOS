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
# 1. Supplier Mapping (таблица маппинга ключей поставщика)
# ============================================================

class SupplierMapping(Base):
    __tablename__ = "supplier_mapping"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    provider_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)

    # Формат ответа
    format: Mapped[str] = mapped_column(String(10), default="json", nullable=False)

    # Где находятся товары в ответе (XML/JSON)
    items_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))

    # === НАШИ ПОЛЯ → ИХ КЛЮЧИ ===
    sku_uid: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_price: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    sku: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_barcodes: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_srok: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_step: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sku_box: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    unit: Mapped[Optional[str]] = mapped_column(String(255), nullable=True) 

    min_order: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # ----- CITY LOGIC -----
    city_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    city_in_params: Mapped[bool] = mapped_column(Boolean, default=False)
    city_in_body: Mapped[bool] = mapped_column(Boolean, default=False)
    city_in_headers: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SupplierMapping provider={self.provider_name}>"


# ============================================================
# 2. Suppliers (таблица поставщиков)
# ============================================================

class Supplier(Base):
    __tablename__ = "suppliers"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    city_param_name: Mapped[Optional[str]] = mapped_column(String(50), default="city_id")

    provider_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    provider_bin = mapped_column(String(20), nullable=True)


    # JSON URLS
    json_url_get_price: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    json_url_get_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    json_url_get_order: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # XML URLS
    xml_url_get_price: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    xml_url_get_address: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    xml_url_get_order: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    login: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    password: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Supplier {self.provider_name}>"


# ============================================================
# 3. Hourly Products (почасовые данные)
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
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))


    sku_uid: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    sku_name: Mapped[Optional[str]] = mapped_column(String(500))
    sku_price: Mapped[Optional[str]] = mapped_column(String(255))
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255))

    sku: Mapped[Optional[str]] = mapped_column(String(255))
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255))

    sku_barcodes: Mapped[Optional[List[str]]] = mapped_column(JSONB, nullable=True)

    sku_srok: Mapped[Optional[str]] = mapped_column(String(255))
    sku_step: Mapped[Optional[str]] = mapped_column(String(255))
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255))
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255))
    sku_box: Mapped[Optional[str]] = mapped_column(String(255))
    unit: Mapped[Optional[str]] = mapped_column(String(255), nullable=True) 
    min_order: Mapped[Optional[str]] = mapped_column(String(255))

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<HourlyProduct {self.sku_uid} / {self.provider_name}>"


# ============================================================
# 4. Daily Products (дневные снимки)
# ============================================================

class DailyProduct(Base):
    __tablename__ = "daily_products"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    provider_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("suppliers.id"), nullable=False
    )

    provider_name: Mapped[str] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(100))

    producer: Mapped[Optional[str]] = mapped_column(String(255))
    producer_country: Mapped[Optional[str]] = mapped_column(String(255))


    sku_uid: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    sku_name: Mapped[Optional[str]] = mapped_column(String(500))
    sku_price: Mapped[Optional[str]] = mapped_column(String(255))
    sku_stock: Mapped[Optional[str]] = mapped_column(String(255))

    sku: Mapped[Optional[str]] = mapped_column(String(255))
    sku_serial: Mapped[Optional[str]] = mapped_column(String(255))

    sku_barcodes: Mapped[Optional[List[str]]] = mapped_column(JSONB, nullable=True)

    sku_srok: Mapped[Optional[str]] = mapped_column(String(255))
    sku_step: Mapped[Optional[str]] = mapped_column(String(255))
    sku_marker: Mapped[Optional[str]] = mapped_column(String(255))
    sku_pack: Mapped[Optional[str]] = mapped_column(String(255))
    sku_box: Mapped[Optional[str]] = mapped_column(String(255))
    unit: Mapped[Optional[str]] = mapped_column(String(255), nullable=True) 
    min_order: Mapped[Optional[str]] = mapped_column(String(255))

    snapshot_date: Mapped[date] = mapped_column(Date, default=date.today)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<DailyProduct {self.sku_uid} / {self.provider_name} / {self.snapshot_date}>"


# ============================================================
# 5. Supplier Cities (сопоставление городов поставщика)
# ============================================================

class SupplierCity(Base):
    __tablename__ = "supplier_cities"

    id: Mapped[int] = mapped_column(primary_key=True)
    provider_name: Mapped[str] = mapped_column(String(255), index=True)
    supplier_city_code: Mapped[str] = mapped_column(String(255), index=True)
    normalized_city: Mapped[str] = mapped_column(String(255), index=True)

    def __repr__(self):
        return f"<SupplierCity {self.provider_name} / {self.supplier_city_code}>"


class CityResponse(Base):
    __tablename__ = "city_responses"

    id: Mapped[int] = mapped_column(primary_key=True)

    provider_name: Mapped[str] = mapped_column(String(255), index=True)

    # Как приходит от поставщика
    supplier_city_code: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    supplier_city_name: Mapped[Optional[str]] = mapped_column(String(255), index=True)

    # ✅ КАК ТЕБЕ НУЖНО В БАЗЕ/ОТВЕТЕ
    normalized_city: Mapped[str] = mapped_column(String(255), index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CityResponse {self.provider_name} | "
            f"{self.supplier_city_code} | {self.supplier_city_name} -> {self.normalized_city}>"
        )