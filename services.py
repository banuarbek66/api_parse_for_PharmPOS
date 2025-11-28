# services.py
# ============================================================
# BUSINESS LOGIC FOR PHARM-POS SUPPLIER AGGREGATOR
# ============================================================

import base64
import json
from datetime import datetime
from typing import List, Optional

import requests
import xmltodict
from sqlalchemy.orm import Session

from core import (
    HTTP_TIMEOUT,
    MAX_PRODUCTS_PER_PROVIDER,
)

from models import (
    Supplier,
    SupplierMapping,
    HourlyProduct,
    DailyProduct,
    CityResponse,
)

from repositories import (
    SupplierRepo,
    MappingRepo,
    HourlyRepo,
    DailyRepo,
    SupplierCityRepo,
)

from schemas import AggregatedItem, SupplierMappingCreate
from utils import nested_get, normalize_barcode

from repositories import CityResponseRepo
# ============================================================
# CITY SERVICE
# ============================================================

class CityService:

    @staticmethod
    def extract_city_from_response(mapping: SupplierMapping, parsed_data: dict) -> Optional[str]:
        """
        Пытаемся получить город из ответа поставщика.
        Приоритет:
        1) city_path
        2) поля city / City
        3) params.city
        """

        value = None

        # 1. Через path
        if mapping.city_path:
            value = nested_get(parsed_data, mapping.city_path)

        # 2. В корне
        if not value:
            value = parsed_data.get("city") or parsed_data.get("City")

        # 3. В params если есть
        if not value and isinstance(parsed_data, dict):
            params = parsed_data.get("params")
            if isinstance(params, dict):
                value = params.get("city")

        if not value:
            return None

        return str(value).strip()

    @staticmethod
    def resolve_city(
        db: Session,
        provider_name: str,
        city_code: Optional[str] = None,
        city_name: Optional[str] = None,
    ) -> str:

        city = CityResponseRepo.find_city(
            db=db,
            provider_name=provider_name,
            code=city_code,
            name=city_name
        )

        if not city:
            raise ValueError(
                f"City '{city_code or city_name}' for provider '{provider_name}' is not registered"
            )

        return city.normalized_city
    

import chardet
import re
# ============================================================
# FETCH SERVICE
# ============================================================
class FetchService:

    @staticmethod
    def fetch(
        url: str,
        login: Optional[str] = None,
        password: Optional[str] = None
    ) -> str:

        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url

        headers: dict = {
            "User-Agent": "PharmPOS/1.0"
        }

        if login and password:
            auth_raw = f"{login}:{password}"
            auth_bytes = auth_raw.encode("utf-8")
            auth_b64 = base64.b64encode(auth_bytes).decode("ascii")
            headers["Authorization"] = f"Basic {auth_b64}"

        response = requests.get(
            url,
            headers=headers,
            timeout=HTTP_TIMEOUT,
            verify=False
        )

        response.raise_for_status()

        # 1. Получаем «сырае тело» ответа
        raw_bytes = response.content

        # 2. Пытаемся определить кодировку автоматически
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"

        try:
            text = raw_bytes.decode(encoding)
        except Exception:
            # fallback на windows-1251 (самая частая у аптек)
            text = raw_bytes.decode("cp1251", errors="ignore")

        return text


# ============================================================
# PARSE SERVICE
# ============================================================

class ParseService:

    @staticmethod
    def parse(raw_text: str, data_format: str) -> dict:

        fmt = (data_format or "").lower().strip()

        if fmt == "json":
            return json.loads(raw_text)

        if fmt == "xml":
            return xmltodict.parse(raw_text)

        raise ValueError(f"Unsupported format: {data_format}")

    @staticmethod
    def normalize(item: dict, mapping: SupplierMapping) -> dict:

        def get_value(key: Optional[str]):
            if not key:
                return None
            if not isinstance(item, dict):
                return None
            return item.get(key)

        # Приводим штрихкод к валидному списку
        raw_barcodes = get_value(mapping.sku_barcodes)
        barcodes = [
            b for b in normalize_barcode(raw_barcodes)
            if b and len(str(b)) >= 6
        ]

        return {
            "sku_uid":      get_value(mapping.sku_uid),
            "sku_name":     get_value(mapping.sku_name),
            "sku_price":    get_value(mapping.sku_price),
            "sku_stock":    get_value(mapping.sku_stock),

            "sku":          get_value(mapping.sku),
            "sku_serial":   get_value(mapping.sku_serial),
            "sku_barcodes": barcodes,

            "sku_srok":     get_value(mapping.sku_srok),
            "sku_step":     get_value(mapping.sku_step),
            "sku_marker":   get_value(mapping.sku_marker),
            "sku_pack":     get_value(mapping.sku_pack),
            "sku_box":      get_value(mapping.sku_box),

            "min_order":    get_value(mapping.min_order),
            "producer": get_value(mapping.producer),
            "producer_country": get_value(mapping.producer_country),
        }

    @staticmethod
    def get_items_by_path(data: dict, path: Optional[str]) -> List[dict]:

        if not path:
            return []

        current = data

        for part in path.split("."):

            if isinstance(current, list):
                if not current:
                    return []
                current = current[0]

            if not isinstance(current, dict):
                return []

            current = current.get(part)

            if current is None:
                return []

        if isinstance(current, dict):
            return [current]

        if isinstance(current, list):
            return current

        return []


# ============================================================
# SYNC SERVICE
# ============================================================

class SyncService:

    @staticmethod
    def _select_price_url(
        supplier: Supplier,
        mapping: SupplierMapping
    ) -> Optional[str]:

        fmt = (mapping.format or "").lower().strip()

        if fmt == "json":
            return supplier.json_url_get_price

        if fmt == "xml":
            return supplier.xml_url_get_price

        return None

    # ------------------------------------------------------------
    # ОСНОВНАЯ СИНХРОНИЗАЦИЯ ОДНОГО ПОСТАВЩИКА (С УЧЁТОМ ГОРОДОВ)
    # ------------------------------------------------------------
    @staticmethod
    def sync_single_supplier(
        db: Session,
        supplier: Supplier
    ) -> dict:

        mapping = MappingRepo.get_by_provider(db, supplier.provider_name)

        if not mapping:
            return {
                "provider": supplier.provider_name,
                "status": "failed",
                "message": "Mapping not found"
            }

        base_url = SyncService._select_price_url(supplier, mapping)

        if not base_url:
            return {
                "provider": supplier.provider_name,
                "status": "failed",
                "message": "URL not found for selected format"
            }

        results: List[dict] = []
        total_products = 0

        # =========================================================
        # ЕСЛИ ГОРОД ПЕРЕДАЁТСЯ ЧЕРЕЗ QUERY PARAMS (city_in_params = True)
        # Берём список городов из CityResponse
        # =========================================================
        if mapping.city_in_params:

            cities = (
                db.query(CityResponse)
                .filter(CityResponse.provider_name == supplier.provider_name)
                .all()
            )

            if not cities:
                return {
                    "provider": supplier.provider_name,
                    "status": "error",
                    "message": "No cities in CityResponse for supplier"
                }

            for city in cities:
                # 1) Если в URL есть плейсхолдер {city} → подставляем прямо туда
                # 2) Иначе добавляем параметр из supplier.city_param_name (по умолчанию "city_id")
                if "{city}" in base_url:
                    url = base_url.format(city=city.supplier_city_code)
                else:
                    param_name = getattr(supplier, "city_param_name", None) or "city_id"
                    sep = "&" if "?" in base_url else "?"
                    url = f"{base_url}{sep}{param_name}={city.supplier_city_code}"

                try:
                    raw_text = FetchService.fetch(
                        url,
                        supplier.login,
                        supplier.password
                    )

                    parsed_data = ParseService.parse(raw_text, mapping.format)

                    items = ParseService.get_items_by_path(
                        parsed_data,
                        mapping.items_path
                    )

                    objects: List[HourlyProduct] = []

                    for item in items[:MAX_PRODUCTS_PER_PROVIDER]:
                        normalized = ParseService.normalize(item, mapping)

                        product = HourlyProduct(
                            provider_id=supplier.id,
                            provider_name=supplier.provider_name,
                            city=city.normalized_city,
                            **normalized,
                        )

                        objects.append(product)

                    if objects:
                        HourlyRepo.bulk_create(db, objects)

                    total_products += len(objects)

                    results.append({
                        "city": city.normalized_city,
                        "supplier_city_code": city.supplier_city_code,
                        "processed": len(objects),
                        "status": "success",
                        "url": url,
                    })

                except Exception as e:
                    results.append({
                        "city": city.normalized_city,
                        "supplier_city_code": city.supplier_city_code,
                        "status": "error",
                        "message": str(e),
                        "url": url,
                    })

        # =========================================================
        # ЕСЛИ ГОРОД ПРИХОДИТ В BODY / В ОТВЕТЕ ПОСТАВЩИКА
        # (city_in_params = False)
        # =========================================================
        else:
            try:
                raw_text = FetchService.fetch(
                    base_url,
                    supplier.login,
                    supplier.password
                )

                parsed_data = ParseService.parse(
                    raw_text,
                    mapping.format
                )

                city_value = CityService.extract_city_from_response(
                    mapping,
                    parsed_data
                )

                city = CityService.resolve_city(
                    db=db,
                    provider_name=supplier.provider_name,
                    city_code=city_value,
                    city_name=city_value,
                )

                items = ParseService.get_items_by_path(
                    parsed_data,
                    mapping.items_path
                )

                objects: List[HourlyProduct] = []

                for item in items[:MAX_PRODUCTS_PER_PROVIDER]:
                    normalized = ParseService.normalize(item, mapping)

                    product = HourlyProduct(
                        provider_id=supplier.id,
                        provider_name=supplier.provider_name,
                        city=city,
                        **normalized,
                    )

                    objects.append(product)

                if objects:
                    HourlyRepo.bulk_create(db, objects)

                total_products = len(objects)

                results.append({
                    "city": city,
                    "processed": len(objects),
                    "status": "success",
                    "url": base_url,
                })

            except Exception as e:
                return {
                    "provider": supplier.provider_name,
                    "status": "error",
                    "message": str(e),
                }

        return {
            "provider": supplier.provider_name,
            "status": "success",
            "total_products": total_products,
            "cities_processed": len(results),
            "details": results,
        }

    # ------------------------------------------------------------
    # СИНХРОНИЗАЦИЯ ВСЕХ ПОСТАВЩИКОВ
    # ------------------------------------------------------------
    @staticmethod
    def run_hourly_sync(db: Session) -> List[dict]:
        results: List[dict] = []
        for supplier in SupplierRepo.get_active(db):
            result = SyncService.sync_single_supplier(db, supplier)
            results.append(result)
        return results

    # ------------------------------------------------------------
    # ЕЖЕДНЕВНЫЙ СНИМОК
    # ------------------------------------------------------------
    @staticmethod
    def run_daily_snapshot(db: Session) -> int:

        hourly = HourlyRepo.get_all(db)
        daily: List[DailyProduct] = []

        for item in hourly:
            daily.append(
                DailyProduct(
                    producer=item.producer,
                    producer_country=item.producer_country,

                    provider_id=item.provider_id,
                    provider_name=item.provider_name,
                    city=item.city,

                    sku_uid=item.sku_uid,
                    sku_name=item.sku_name,
                    sku_price=item.sku_price,
                    sku_stock=item.sku_stock,

                    sku=item.sku,
                    sku_serial=item.sku_serial,
                    sku_barcodes=item.sku_barcodes,

                    sku_srok=item.sku_srok,
                    sku_step=item.sku_step,
                    sku_marker=item.sku_marker,
                    sku_pack=item.sku_pack,
                    sku_box=item.sku_box,

                    min_order=item.min_order,
                    snapshot_date=datetime.utcnow().date(),
                )
            )

        if daily:
            DailyRepo.bulk_create(db, daily)

        return len(daily)

    # ------------------------------------------------------------
    # ЧИСТКА HOURLY-ТАБЛИЦЫ
    # ------------------------------------------------------------
    @staticmethod
    def cleanup_hourly_table(db: Session) -> bool:
        HourlyRepo.clear_table(db)
        return True




  



# ============================================================
# PRODUCT SERVICE
# ============================================================

class ProductService:

    @staticmethod
    def get_by_barcode(
        db: Session,
        barcode: str,
        city: Optional[str] = None
    ) -> List[AggregatedItem]:

        items = HourlyRepo.get_latest_by_barcode(db, barcode, city=city)

        if not items:
            items = DailyRepo.get_latest_by_barcode(db, barcode, city=city)

        result: List[AggregatedItem] = []

        for item in items:

            matched_barcode = None

            if item.sku_barcodes:
                for b in item.sku_barcodes:
                    if str(b).strip() == str(barcode).strip():
                        matched_barcode = b
                        break

            # если вдруг не совпало — не пускаем в ответ
            if not matched_barcode:
                continue

            result.append(
                AggregatedItem(
                    provider_name=item.provider_name,
                    city=item.city,

                    sku_uid=item.sku_uid,
                    sku_name=item.sku_name,
                    sku_barcode=matched_barcode,

                    sku_price=item.sku_price,
                    sku_stock=item.sku_stock,
                    sku_step=item.sku_step,
                    min_order=item.min_order,
                    last_update=item.created_at,
                )
            )

        return result



# ============================================================
# MAPPING CREATE + AUTO SYNC
# ============================================================

class SupplierMappingService:

    @staticmethod
    def create_mapping(
        db: Session,
        mapping_data: SupplierMappingCreate
    ) -> dict:

        mapping = SupplierMapping(**mapping_data.dict())
        db.add(mapping)
        db.commit()
        db.refresh(mapping)

        supplier = SupplierRepo.get_by_name(db, mapping.provider_name)

        if not supplier:
            return {
                "status": "warning",
                "message": "Mapping created, but supplier not found",
                "mapping_id": str(mapping.id)
            }

        result = SyncService.sync_single_supplier(db, supplier)

        return {
            "status": "success",
            "mapping_id": str(mapping.id),
            "sync_result": result,
        }
