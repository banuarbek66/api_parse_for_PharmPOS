# schemas.py
# ============================================================
# Pydantic СХЕМЫ ДЛЯ PHARM-POS SUPPLIER AGGREGATOR
# ============================================================

from typing import List, Optional
from datetime import datetime, date
from pydantic import BaseModel
from uuid import UUID


# ============================================================
# 1. Supplier Mapping
# ============================================================

class SupplierMappingBase(BaseModel):
    provider_name: str
    format: str = "json"

    items_path: Optional[str] = None

    city_path: Optional[str] = None
    city_in_params: bool = False
    city_in_body: bool = False
    city_in_headers: bool = False

    producer: Optional[str] = None
    producer_country: Optional[str] = None

    sku_uid: Optional[str] = None
    sku_name: Optional[str] = None
    sku_price: Optional[str] = None
    sku_stock: Optional[str] = None

    sku: Optional[str] = None
    sku_serial: Optional[str] = None
    sku_barcodes: Optional[str] = None
    sku_srok: Optional[str] = None
    sku_step: Optional[str] = None
    sku_marker: Optional[str] = None
    sku_pack: Optional[str] = None
    sku_box: Optional[str] = None

    unit: Optional[str] = None
    min_order: Optional[str] = None


class SupplierMappingCreate(SupplierMappingBase):
    pass


class SupplierMappingRead(SupplierMappingBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================
# 2. Supplier
# ============================================================

class SupplierBase(BaseModel):
    provider_name: str
    provider_bin: Optional[str] = None

    json_url_get_price: Optional[str] = None
    json_url_get_address: Optional[str] = None
    json_url_get_order: Optional[str] = None

    xml_url_get_price: Optional[str] = None
    xml_url_get_address: Optional[str] = None
    xml_url_get_order: Optional[str] = None

    login: Optional[str] = None
    password: Optional[str] = None

    city_param_name: Optional[str] = "city_id"
    is_active: bool = True


class SupplierCreate(SupplierBase):
    pass


class SupplierRead(SupplierBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================
# 3. Supplier City
# ============================================================

class SupplierCityBase(BaseModel):
    provider_name: str
    supplier_city_code: str
    normalized_city: str


class SupplierCityCreate(SupplierCityBase):
    pass


class SupplierCityRead(SupplierCityBase):
    id: int

    class Config:
        from_attributes = True


# ============================================================
# 4. Продукт (Hourly / Daily)
# ============================================================

class ProductBase(BaseModel):
    provider_name: str
    city: Optional[str] = None
    producer: Optional[str] = None
    producer_country: Optional[str] = None

    sku_uid: Optional[str] = None
    sku_name: Optional[str] = None
    sku_price: Optional[str] = None
    sku_stock: Optional[str] = None

    sku: Optional[str] = None
    sku_serial: Optional[str] = None

    sku_barcodes: Optional[List[str]] = None

    sku_srok: Optional[str] = None
    sku_step: Optional[str] = None
    sku_marker: Optional[str] = None
    sku_pack: Optional[str] = None
    sku_box: Optional[str] = None

    unit: Optional[str] = "упаковка"
    min_order: Optional[str] = None


class HourlyProductRead(ProductBase):
    id: UUID
    provider_id: UUID
    provider_bin: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class DailyProductRead(ProductBase):
    id: UUID
    provider_id: UUID
    provider_bin: Optional[str] = None
    snapshot_date: date
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================
# 5. AggregatedItem
# ============================================================

class AggregatedItem(BaseModel):
    provider_name: str
    provider_bin: Optional[str] = None

    city: Optional[str] = None
    producer: Optional[str] = None
    producer_country: Optional[str] = None

    sku_uid: Optional[str] = None
    sku_name: Optional[str] = None
    sku_barcode: Optional[str] = None

    sku_price: Optional[str] = None
    sku_stock: Optional[str] = None

    sku_step: Optional[str] = None
    unit: Optional[str] = None
    min_order: Optional[str] = None

    sku_serial: Optional[str] = None
    sku_srok: Optional[str] = None
    sku_marker: Optional[str] = None

    last_update: Optional[datetime] = None


class ProductByBarcodeResponse(BaseModel):
    sku_barcode: str
    city: Optional[str] = None
    results: List[AggregatedItem]


# ============================================================
# 6. Sync Result
# ============================================================

class SyncResult(BaseModel):
    provider_name: str
    processed: int
    status: str

    city: Optional[str] = None
    message: Optional[str] = None


# ============================================================
# 7. City Response
# ============================================================

class CityResponseCreate(BaseModel):
    provider_name: str
    supplier_city_code: Optional[str] = None
    supplier_city_name: Optional[str] = None
    normalized_city: str


class CityResponseRead(CityResponseCreate):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


# ============================================================
# 8. Supplier Unit
# ============================================================

class SupplierUnitCreate(BaseModel):
    provider_name: str
    supplier_unit: str
    normalized_unit: str


class SupplierUnitOut(BaseModel):
    id: int
    provider_name: str
    supplier_unit: str
    normalized_unit: str
    created_at: datetime

    class Config:
        from_attributes = True


from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class SupplierSrokBase(BaseModel):
    provider_name: str
    provider_srok_raw: str


class SupplierSrokCreate(SupplierSrokBase):
    pass


class SupplierSrokUpdate(BaseModel):
    provider_srok_raw: Optional[str] = None


class SupplierSrokResponseSchema(BaseModel):
    id: int
    provider_name: str
    provider_srok_raw: str
    normalized_srok: Optional[str]
    created_at: datetime

    class Config:
        orm_mode = True
