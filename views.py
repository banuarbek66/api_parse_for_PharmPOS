from sqladmin import ModelView

from models import (
    Supplier,
    SupplierMapping,
    CityResponse,
    SupplierUnit,
    HourlyProduct,
    DailyProduct,
    ProductCompare,
    SupplierSrokResponse
)
from stock_movement_model import StockMovement

# ============================================================
# SUPPLIERS
# ============================================================

class SupplierAdmin(ModelView, model=Supplier):
    column_list = [
        Supplier.id,
        Supplier.provider_name,
        Supplier.provider_bin,
        Supplier.is_active,
        Supplier.created_at,
    ]

    column_labels = {
        Supplier.provider_name: "Поставщик",
        Supplier.provider_bin: "БИН поставщика",
        Supplier.is_active: "Активен",
        Supplier.created_at: "Создан",
    }

    column_searchable_list = [
        Supplier.provider_name,
        Supplier.provider_bin,
    ]


# ============================================================
# SUPPLIER MAPPING
# ============================================================

class SupplierMappingAdmin(ModelView, model=SupplierMapping):
    column_list = [
        SupplierMapping.provider_name,
        SupplierMapping.format,
        SupplierMapping.items_path,
        SupplierMapping.city_in_params,
        SupplierMapping.created_at,
    ]

    column_labels = {
        SupplierMapping.provider_name: "Поставщик",
        SupplierMapping.format: "Формат",
        SupplierMapping.items_path: "Путь к товарам",
        SupplierMapping.city_in_params: "Города в параметрах",
        SupplierMapping.created_at: "Создан",
    }

    form_columns = [
        SupplierMapping.provider_name,
        SupplierMapping.format,
        SupplierMapping.items_path,
        SupplierMapping.city_path,
        SupplierMapping.city_in_params,
        SupplierMapping.city_in_body,
        SupplierMapping.city_in_headers,

        SupplierMapping.sku_uid,
        SupplierMapping.sku_name,
        SupplierMapping.sku_price,
        SupplierMapping.sku_stock,
        SupplierMapping.sku_barcodes,
        SupplierMapping.unit,
    ]


# ============================================================
# CITY RESPONSE
# ============================================================

class CityResponseAdmin(ModelView, model=CityResponse):
    column_list = [
        CityResponse.provider_name,
        CityResponse.supplier_city_code,
        CityResponse.supplier_city_name,
        CityResponse.normalized_city,
    ]

    column_labels = {
        CityResponse.provider_name: "Поставщик",
        CityResponse.supplier_city_code: "Код города (поставщик)",
        CityResponse.supplier_city_name: "Название города (поставщик)",
        CityResponse.normalized_city: "Нормализованный город",
    }

    column_searchable_list = [
        CityResponse.provider_name,
        CityResponse.supplier_city_code,
        CityResponse.supplier_city_name,
        CityResponse.normalized_city,
    ]


# ============================================================
# ✅ SUPPLIER UNIT (КЛЮЧЕВОЙ КЛАССИФИКАТОР)
# ============================================================

class SupplierUnitAdmin(ModelView, model=SupplierUnit):
    column_list = [
        SupplierUnit.provider_name,
        SupplierUnit.supplier_unit,
        SupplierUnit.normalized_unit,
        SupplierUnit.created_at,
    ]

    column_labels = {
        SupplierUnit.provider_name: "Поставщик",
        SupplierUnit.supplier_unit: "Единица поставщика",
        SupplierUnit.normalized_unit: "Код классификатора",
        SupplierUnit.created_at: "Создано",
    }

    column_searchable_list = [
        SupplierUnit.provider_name,
        SupplierUnit.supplier_unit,
        SupplierUnit.normalized_unit,
    ]

    form_columns = [
        SupplierUnit.provider_name,
        SupplierUnit.supplier_unit,
        SupplierUnit.normalized_unit,
    ]


# ============================================================
# ✅ HOURLY PRODUCTS
# ============================================================

class HourlyProductAdmin(ModelView, model=HourlyProduct):
    can_create = False
    can_edit = False
    can_delete = True

    column_list = [
        HourlyProduct.id,
        HourlyProduct.provider_name,
        HourlyProduct.city,
        HourlyProduct.sku_name,
        HourlyProduct.sku_barcodes,
        HourlyProduct.sku_stock,
        HourlyProduct.unit,
        HourlyProduct.created_at,
    ]

    

    column_labels = {
        HourlyProduct.id: "ID",
        HourlyProduct.provider_name: "Поставщик",
        HourlyProduct.city: "Город",
        HourlyProduct.sku_name: "Товар",
        HourlyProduct.sku_barcodes: "Баркоды",
        HourlyProduct.sku_stock: "Остаток",
        HourlyProduct.unit: "Ед.изм (код)",
        HourlyProduct.created_at: "Обновлено",
    }

    column_searchable_list = [
        "sku_name",
        "sku_barcodes",
        "created_at",
    ]

    column_sortable_list = ["id", "created_at"]


# ============================================================
# ✅ DAILY PRODUCTS
# ============================================================

class DailyProductAdmin(ModelView, model=DailyProduct):
    can_create = False
    can_edit = False
    can_delete = True

    column_list = [
        DailyProduct.id,
        DailyProduct.provider_name,
        DailyProduct.city,
        DailyProduct.sku_name,
        DailyProduct.sku_barcodes,
        DailyProduct.sku_stock,
        DailyProduct.unit,
        DailyProduct.snapshot_date,
    ]

    column_labels = {
        DailyProduct.id: "ID",
        DailyProduct.provider_name: "Поставщик",
        DailyProduct.city: "Город",
        DailyProduct.sku_name: "Товар",
        DailyProduct.sku_barcodes: "Баркоды",
        DailyProduct.sku_stock: "Остаток",
        DailyProduct.unit: "Ед.изм (код)",
        DailyProduct.snapshot_date: "Дата снимка",
    }
    
   


    column_searchable_list = [
        "sku_name",
        "sku_barcodes",
        "snapshot_date"
    ]

    column_sortable_list = ["id", "created_at"]



class ProductCompareAdmin(ModelView, model=ProductCompare):
    name = "Сравнение товаров"
    name_plural = "Сравнение товаров"
    icon = "fa-solid fa-scale-balanced"

    column_list = [
        ProductCompare.barcode,
        ProductCompare.sku_name,
        ProductCompare.price_atamiras,
        ProductCompare.price_medservice,
        ProductCompare.price_stopharm,
        ProductCompare.price_amanat,
        ProductCompare.price_rauza,
        
    ]

    column_searchable_list = [
        ProductCompare.barcode,
        ProductCompare.sku_name,
    ]

    column_sortable_list = [
        ProductCompare.barcode,
        ProductCompare.sku_name,
        ProductCompare.price_atamiras,
        ProductCompare.price_medservice,
        ProductCompare.price_stopharm,
        ProductCompare.price_amanat,
    ]

    can_create = False
    can_edit = False
    can_delete = False
class StockMovementAdmin(ModelView, model=StockMovement):
    name= "Stock Movement"

    column_exclude_list = [StockMovement.canonical_id]
    can_create = False
    can_edit=False
    can_delete=False

class SupllierSrokAdmin(ModelView, model=SupplierSrokResponse):
    name='Suplier Srok'
    column_list = [
        SupplierSrokResponse.provider_name,
        SupplierSrokResponse.provider_srok_raw,
        SupplierSrokResponse.normalized_srok,
    ]
    form_columns=[
        SupplierSrokResponse.provider_name,
        SupplierSrokResponse.provider_srok_raw,
        SupplierSrokResponse.normalized_srok,
    ]

