# services.py
# ============================================================
# BUSINESS LOGIC FOR PHARM-POS SUPPLIER AGGREGATOR
# ============================================================

import base64
import json
import re
from datetime import datetime, time, timedelta
from typing import List, Optional, Dict, Tuple, Set
from collections import defaultdict

import requests
import xmltodict
import chardet
from sqlalchemy.orm import Session
from sqlalchemy import text

import asyncio
import httpx

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
    ProductCompare,
    ProductCanonical,
    BarcodeAlias,
)

from repositories import (
    SupplierRepo,
    MappingRepo,
    HourlyRepo,
    DailyRepo,
    SupplierCityRepo,
    SupplierUnitRepo,
    CityResponseRepo,
    ProductCanonicalRepo,
    BarcodeAliasRepo,
)

from schemas import AggregatedItem, SupplierMappingCreate
from utils import nested_get, normalize_barcode, clean_unit, normalize_name
from database import SessionLocal

def sanitize_xml(raw: str) -> str:
    end_tag = "</root>"
    idx = raw.lower().find(end_tag)
    if idx != -1:
        return raw[: idx + len(end_tag)]
    return raw


def clean_xml(raw: str) -> str:
    """
    Убирает мусор после </root>, который ломает xmltodict
    (часто встречается у rauza)
    """
    if not raw:
        return raw

    lower = raw.lower()
    end = lower.rfind("</root>")
    if end != -1:
        return raw[: end + len("</root>")]

    return raw

def sanitize_barcodes(raw) -> list[str]:
    """
    Оставляем ТОЛЬКО реальные баркоды:
    - только цифры
    - длина 6–14
    """
    cleaned = []

    for b in raw or []:
        try:
            b = str(b).strip()
        except Exception:
            continue

        if b.isdigit() and 6 <= len(b) <= 14:
            cleaned.append(b)

    return cleaned



# ============================================================
# CITY SERVICE
# ============================================================

class CityService:

    @staticmethod
    def extract_city_from_response(mapping: SupplierMapping, parsed_data: dict) -> Optional[str]:
        value = None

        # 1) city_path
        if mapping.city_path:
            value = nested_get(parsed_data, mapping.city_path)

        # 2) city / City
        if not value:
            value = parsed_data.get("city") or parsed_data.get("City")

        # 3) params.city
        if not value and isinstance(parsed_data, dict):
            params = parsed_data.get("params")
            if isinstance(params, dict):
                value = params.get("city")

        return str(value).strip() if value else None

    @staticmethod
    def resolve_city(db: Session, provider_name: str, city_code=None, city_name=None) -> str:

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


# ============================================================
# FETCH SERVICE (СИНХРОННЫЙ)
# ============================================================

class FetchService:

    @staticmethod
    def fetch(url: str, login: str | None = None, password: str | None = None) -> str:

        if not url.startswith(("http://", "https://")):
            url = "http://" + url

        headers = {"User-Agent": "PharmPOS/1.0"}

        if login and password:
            raw = f"{login}:{password}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")

        response = requests.get(
            url,
            headers=headers,
            timeout=HTTP_TIMEOUT,
            verify=False
        )
        response.raise_for_status()

        raw_bytes = response.content
        encoding = chardet.detect(raw_bytes).get("encoding") or "utf-8"

        def looks_broken(txt: str) -> bool:
            bad = ["Ð", "Ñ", "â", "€", "", ""]
            return sum(1 for p in bad if p in txt) >= 2

        # autodetect
        try:
            txt = raw_bytes.decode(encoding, errors="strict")
            if not looks_broken(txt):
                return txt
        except Exception:
            pass

        # cp1251
        try:
            txt = raw_bytes.decode("cp1251", errors="strict")
            if not looks_broken(txt):
                return txt
        except Exception:
            pass

        # fallback
        return raw_bytes.decode("latin-1", errors="ignore")


# ============================================================
# FETCH SERVICE (АСИНХРОННЫЙ — ТОЛЬКО HTTP)
# ============================================================



# ============================================================
# PARSE SERVICE — FINAL VERSION
# ============================================================

class ParseService:

    @staticmethod
    def parse(raw: str, data_format: str) -> dict:
        fmt = (data_format or "").lower().strip()

        if fmt == "json":
            return json.loads(raw)

        if fmt == "xml":
            
            return xmltodict.parse(raw)

        raise ValueError(f"Unsupported format: {data_format}")

    @staticmethod
    def normalize(item: dict, mapping: SupplierMapping) -> dict:

        def get_value(key: Optional[str]):
            if not key:
                return None
            if not isinstance(item, dict):
                return None
            return item.get(key)

        # BARCODE NORMALIZATION
        raw_barcodes = get_value(mapping.sku_barcodes)
        barcodes = [
            b for b in normalize_barcode(raw_barcodes)
            if b and len(str(b)) >= 6
        ]

        # STEP
        raw_step = get_value(mapping.sku_step)
        step_value = 1
        if raw_step not in (None, "", "0", 0):
            try:
                iv = int(str(raw_step).replace(",", "."))
                if iv > 0:
                    step_value = iv
            except Exception:
                step_value = 1

        # UNIT — ТОЛЬКО СЫРОЕ ЗНАЧЕНИЕ ИЗ АПИ
        raw_unit = get_value(mapping.unit)

        return {
            "sku_uid": get_value(mapping.sku_uid),
            "sku_name": get_value(mapping.sku_name),
            "sku_price": get_value(mapping.sku_price),
            "sku_stock": get_value(mapping.sku_stock),

            "sku": get_value(mapping.sku),
            "sku_serial": get_value(mapping.sku_serial),

            "sku_barcodes": barcodes,
            "sku_srok": get_value(mapping.sku_srok),
            "sku_step": str(step_value),
            "sku_marker": get_value(mapping.sku_marker),
            "sku_pack": get_value(mapping.sku_pack),
            "sku_box": get_value(mapping.sku_box),

            "unit": raw_unit,  # ← ТОЛЬКО СЫРОЙ ЮНИТ, нормализация в SyncService
            "min_order": get_value(mapping.min_order),

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

    DEFAULT_UNIT = "778"  # упаковка

    # Кэш нормализации юнитов: (provider_name, raw_unit) -> normalized_unit
    _unit_cache: Dict[Tuple[str, str], str] = {}

    @staticmethod
    def _reset_unit_cache():
        SyncService._unit_cache.clear()

    @staticmethod
    def _apply_unit_normalization(db: Session, provider: str, raw_unit: str | None) -> str:
        """
        Логика нормализации:
        1) Если unit пуст → default 778
        2) Если есть точное совпадение в supplier_units → normalized_unit
        3) Если нет → default 778

        ⚠️ Эту функцию не трогаем по смыслу — она может использоваться снаружи.
        """
        if not raw_unit:
            return SyncService.DEFAULT_UNIT

        fixed = SupplierUnitRepo.find(db, provider, raw_unit)

        if fixed:
            return fixed.normalized_unit

        return SyncService.DEFAULT_UNIT

    @staticmethod
    def _normalize_unit_cached(db: Session, provider: str, raw_unit: Optional[str]) -> str:
        """
        Кэшированная нормализация unit с сохранением твоей исходной логики:
        - если raw_unit пустой → DEFAULT_UNIT
        - если raw_unit есть и есть mapping → normalized_unit
        - если raw_unit есть и mapping НЕТ → оставить raw_unit как есть
        """
        key = (provider, str(raw_unit) if raw_unit is not None else "")

        cached = SyncService._unit_cache.get(key)
        if cached is not None:
            return cached

        if not raw_unit:
            value = SyncService.DEFAULT_UNIT
        else:
            fixed = SupplierUnitRepo.find(db, provider, raw_unit)
            if fixed:
                value = fixed.normalized_unit
            else:
                # важный момент: если маппинга нет — оставляем как есть
                value = str(raw_unit)

        SyncService._unit_cache[key] = value
        return value

    @staticmethod
    def _select_price_url(supplier: Supplier, mapping: SupplierMapping) -> Optional[str]:

        fmt = (mapping.format or "").lower().strip()

        if fmt == "json":
            return supplier.json_url_get_price
        if fmt == "xml":
            return supplier.xml_url_get_price

        return None

    # ------------------------------------------------------------
    # ASYNC ВЕРСИЯ ДЛЯ FETCH (HTTP — асинхронно, БД — синхронно)
    # ------------------------------------------------------------
    

    
    # ------------------------------------------------------------
    # СТАРАЯ СИНХРОННАЯ ВЕРСИЯ (ЛОГИКА НЕ ТРОГАЛАСЬ)
    # ------------------------------------------------------------
    @staticmethod
    def sync_single_supplier(db: Session, supplier: Supplier) -> dict:

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
                "message": "URL not found"
            }

        results = []
        total_products = 0

        # ============================================================
        # MULTI-CITY MODE
        # ============================================================
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
                    "message": "No cities registered"
                }

            for city in cities:

                if "{city}" in base_url:
                    url = base_url.format(city=city.supplier_city_code)
                else:
                    param = supplier.city_param_name or "city_id"
                    sep = "&" if "?" in base_url else "?"
                    url = f"{base_url}{sep}{param}={city.supplier_city_code}"

                try:
                    raw = FetchService.fetch(
                        url,
                        supplier.login,
                        supplier.password
                    )

                    # 🔥 FIX 1 — мусор после </root>
                    if mapping.format == "xml":
                        low = raw.lower()
                        end = low.rfind("</root>")
                        if end != -1:
                            raw = raw[: end + len("</root>")]

                    parsed = ParseService.parse(raw, mapping.format)
                    items = ParseService.get_items_by_path(parsed, mapping.items_path)

                    objects: List[HourlyProduct] = []

                    for it in items[:MAX_PRODUCTS_PER_PROVIDER]:

                        normalized = ParseService.normalize(it, mapping)

                        # 🔥 FIX 2 — санитизация баркодов
                        raw_barcodes = normalized.get("sku_barcodes") or []
                        clean_barcodes = []

                        for b in raw_barcodes:
                            b = str(b).strip()
                            if b.isdigit() and 6 <= len(b) <= 14:
                                clean_barcodes.append(b)

                        if not clean_barcodes:
                            continue

                        normalized["sku_barcodes"] = clean_barcodes

                        # --- UNIT NORMALIZATION (с кэшем) ---
                        raw_unit = normalized.get("unit")
                        normalized["unit"] = SyncService._normalize_unit_cached(
                            db, supplier.provider_name, raw_unit
                        )

                        objects.append(
                            HourlyProduct(
                                provider_id=supplier.id,
                                provider_name=supplier.provider_name,
                                provider_bin=supplier.provider_bin,
                                city=city.normalized_city,
                                **normalized
                            )
                        )

                    if objects:
                        HourlyRepo.bulk_create(db, objects)

                    total_products += len(objects)

                    results.append({
                        "city": city.normalized_city,
                        "supplier_city_code": city.supplier_city_code,
                        "processed": len(objects),
                        "status": "success",
                        "url": url
                    })

                except Exception as e:
                    results.append({
                        "city": city.normalized_city,
                        "supplier_city_code": city.supplier_city_code,
                        "status": "error",
                        "message": str(e),
                        "url": url
                    })

        # ============================================================
        # SINGLE CITY MODE
        # ============================================================
        else:

            try:
                raw = FetchService.fetch(
                    base_url,
                    supplier.login,
                    supplier.password
                )

                if mapping.format == "xml":
                    low = raw.lower()
                    end = low.rfind("</root>")
                    if end != -1:
                        raw = raw[: end + len("</root>")]

                parsed = ParseService.parse(raw, mapping.format)

                city_raw = CityService.extract_city_from_response(mapping, parsed)
                city = CityService.resolve_city(
                    db=db,
                    provider_name=supplier.provider_name,
                    city_code=city_raw,
                    city_name=city_raw,
                )

                items = ParseService.get_items_by_path(parsed, mapping.items_path)

                objects: List[HourlyProduct] = []

                for it in items[:MAX_PRODUCTS_PER_PROVIDER]:

                    normalized = ParseService.normalize(it, mapping)

                    raw_barcodes = normalized.get("sku_barcodes") or []
                    clean_barcodes = [
                        str(b).strip()
                        for b in raw_barcodes
                        if str(b).strip().isdigit()
                    ]

                    if not clean_barcodes:
                        continue

                    normalized["sku_barcodes"] = clean_barcodes

                    raw_unit = normalized.get("unit")
                    normalized["unit"] = SyncService._normalize_unit_cached(
                        db, supplier.provider_name, raw_unit
                    )

                    objects.append(
                        HourlyProduct(
                            provider_id=supplier.id,
                            provider_name=supplier.provider_name,
                            provider_bin=supplier.provider_bin,
                            city=city,
                            **normalized
                        )
                    )

                if objects:
                    HourlyRepo.bulk_create(db, objects)

                total_products += len(objects)

                results.append({
                    "city": city,
                    "processed": len(objects),
                    "status": "success",
                    "url": base_url
                })

            except Exception as e:
                return {
                    "provider": supplier.provider_name,
                    "status": "error",
                    "message": str(e)
                }

        return {
            "provider": supplier.provider_name,
            "status": "success",
            "total_products": total_products,
            "cities_processed": len(results),
            "details": results,
        }

    # ------------------------------------------------------------
    @staticmethod
    def run_daily_snapshot(db: Session) -> int:

        hourly = HourlyRepo.get_all(db)
        daily: List[DailyProduct] = []

        for item in hourly:
            daily.append(
                DailyProduct(
                    provider_id=item.provider_id,
                    provider_name=item.provider_name,
                    provider_bin=item.provider_bin,
                    city=item.city,

                    producer=item.producer,
                    producer_country=item.producer_country,

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

                    unit=item.unit,
                    min_order=item.min_order,

                    snapshot_date=datetime.utcnow().date()
                )
            )

        if daily:
            DailyRepo.bulk_create(db, daily)

        return len(daily)

    @staticmethod
    def run_hourly_sync(db: Session):
        """
        Главный метод почасовой синхронизации (синхронная версия).
        Вызывается CRON-ом или scheduler'ом.
        """
        suppliers = SupplierRepo.get_active(db)

        if not suppliers:
            return {"status": "error", "message": "No active suppliers"}

        # Сбрасываем кэш юнитов перед запуском
        SyncService._reset_unit_cache()

        results: List[dict] = []
        total = 0

        for supplier in suppliers:
            try:
                res = SyncService.sync_single_supplier(db, supplier)
                results.append(res)
                total += res.get("total_products", 0)
            except Exception as e:
                results.append({
                    "provider": supplier.provider_name,
                    "status": "error",
                    "message": str(e)
                })

        # 1) пересобираем канонические товары + алиасы
      
        return {
            "status": "success",
            "total_suppliers": len(suppliers),
            "total_products": total,
            "details": results
        }

    @staticmethod
    def cleanup_hourly_table(db: Session) -> bool:
        HourlyRepo.clear_table(db)
        return True



# ============================================================
# PRODUCT SERVICE
# ============================================================

class ProductService:

    @staticmethod
    def get_by_barcode(db: Session, barcode: str, city: Optional[str] = None) -> List[AggregatedItem]:

        # 1) HOURLY
        items = HourlyRepo.get_latest_by_barcode(db, barcode, city=city)

        # 2) DAILY fallback
        if not items:
            items = DailyRepo.get_latest_by_barcode(db, barcode, city=city)

        result: List[AggregatedItem] = []

        for item in items:

            matched = None
            if item.sku_barcodes:
                for b in item.sku_barcodes:
                    if str(b).strip() == str(barcode).strip():
                        matched = b
                        break

            if not matched:
                continue

            result.append(
                AggregatedItem(
                    provider_name=item.provider_name,
                    provider_bin=item.provider_bin,
                    city=item.city,
                    producer=item.producer,
                    producer_country=item.producer_country,
                    sku_uid=item.sku_uid,
                    sku_name=item.sku_name,
                    sku_barcode=matched,
                    sku_price=item.sku_price,
                    sku_stock=item.sku_stock,
                    sku_step=item.sku_step,
                    unit=item.unit,
                    min_order=item.min_order,
                    sku_serial=item.sku_serial,
                    sku_srok=item.sku_srok,
                    sku_marker=item.sku_marker,
                    last_update=item.created_at
                )
            )

        return result


# ============================================================
# MAPPING CREATE + AUTO SYNC
# ============================================================

class SupplierMappingService:

    @staticmethod
    def create_mapping(db: Session, mapping_data: SupplierMappingCreate) -> dict:

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


# ============================================================
# UNIVERSAL ANALYTICS SERVICE — DAILY / WEEKLY / MONTHLY
# ============================================================

class AnalyticsService:

    # ------------------------------------------------------------
    @staticmethod
    def get_period_bounds(period: str, start_date=None, end_date=None):

        now = datetime.utcnow()

        if period == "today":
            start = datetime.combine(now.date(), time(hour=1))
            return start, now

        if period == "week":
            start = now - timedelta(days=7)
            return start, now

        if period == "month":
            start = now - timedelta(days=30)
            return start, now

        if period == "custom":
            if not start_date or not end_date:
                raise ValueError("Custom period requires start_date and end_date")
            return start_date, end_date

        raise ValueError(f"Unknown period: {period}")

    # ------------------------------------------------------------
    @staticmethod
    def get_hot_products_period(
        db: Session,
        period: str = "today",
        start_date=None,
        end_date=None,
        provider_name: Optional[str] = None,
        city: Optional[str] = None,
        limit: int = 10
    ) -> dict:

        start_dt, end_dt = AnalyticsService.get_period_bounds(period, start_date, end_date)

        now = datetime.utcnow()
        include_hourly = start_dt.date() <= now.date()

        hourly: List[HourlyProduct] = []
        if include_hourly:
            today_start = datetime.combine(now.date(), time(hour=1))
            hourly = HourlyRepo.get_for_period(
                db=db,
                start_dt=max(start_dt, today_start),
                end_dt=end_dt,
                provider_name=provider_name,
                city=city
            )

        daily = DailyRepo.get_range(
            db=db,
            start_date=start_dt.date(),
            end_date=end_dt.date(),
            provider_name=provider_name,
            city=city
        )

        if not hourly and not daily:
            return {"status": "empty", "items": []}

        combined = defaultdict(list)

        # ---- DAILY ----
        for d in daily:
            combined[(d.provider_name, d.city, d.sku_uid)].append(
                (d.snapshot_date, d.sku_stock, d.sku_name, d.sku_barcodes, "daily")
            )

        # ---- HOURLY ----
        for h in hourly:
            combined[(h.provider_name, h.city, h.sku_uid)].append(
                (h.created_at, h.sku_stock, h.sku_name, h.sku_barcodes, "hourly")
            )

        # Process
        hot = []

        def to_num(val):
            try:
                return float(val)
            except Exception:
                return 0

        for (provider, city_, sku_uid), entries in combined.items():

            normalized = []
            for t, stock, name, bc, src in entries:
                if isinstance(t, datetime):
                    normalized.append((t, stock, name, bc))
                else:
                    normalized.append((datetime.combine(t, time.min), stock, name, bc))

            entries = sorted(normalized, key=lambda x: x[0])

            start_stock = to_num(entries[0][1])
            end_stock = to_num(entries[-1][1])

            if start_stock <= 0:
                continue

            sold = start_stock - end_stock
            if sold <= 0:
                continue

            percent = round(sold / start_stock * 100, 2)

            hot.append({
                "provider_name": provider,
                "city": city_,

                "sku_uid": sku_uid,
                "sku_name": entries[-1][2],
                "sku_barcodes": entries[-1][3],

                "start_stock": start_stock,
                "end_stock": end_stock,
                "sold": sold,
                "percent": percent
            })

        hot.sort(key=lambda x: (x["percent"], x["sold"]), reverse=True)

        return {
            "status": "success",
            "period": period,
            "from": start_dt.isoformat(),
            "to": end_dt.isoformat(),
            "items": hot[:limit]
        }


# ============================================================
# CANONICAL RESOLVE SERVICE
# ============================================================

class CanonicalResolveService:
    """
    Строит и обновляет product_canonical и barcode_aliases
    на основе hourly_products.

    ⚡ Оптимизация:
    - preload canonical + aliases
    - bulk insert
    - один commit в конце
    """

    @staticmethod
    def rebuild_from_hourly(db: Session) -> None:
        items = HourlyRepo.get_all(db)

        # ✅ preload ВСЁ в память
        canonical_map = ProductCanonicalRepo.preload_all(db)
        alias_map = BarcodeAliasRepo.preload_all(db)

        seen_keys: Set[Tuple[str, Optional[str], Tuple[str, ...]]] = set()

        new_canonicals = []
        new_aliases = []

        for item in items:
            if not item.sku_name and not item.sku_barcodes:
                continue

            barcodes = [str(b).strip() for b in (item.sku_barcodes or []) if str(b).strip()]
            name_key = normalize_name(item.sku_name) if item.sku_name else None

            if not barcodes and not name_key:
                continue

            key = (
                name_key,
                item.producer,
                tuple(sorted(barcodes))
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)

            canonical_id = None

            # 1️⃣ ищем canonical по баркоду (из памяти)
            for b in barcodes:
                cid = alias_map.get(b)
                if cid:
                    canonical_id = cid
                    break

            # 2️⃣ ищем canonical по name_key + producer (из памяти)
            if not canonical_id and name_key:
                canonical_id = canonical_map.get((name_key, item.producer))

            # 3️⃣ если нет — создаём НОВЫЙ
            if not canonical_id:
                new_canonicals.append({
                    "canonical_barcode": barcodes[0] if barcodes else None,
                    "name_key": name_key,
                    "producer": item.producer,
                    "producer_country": item.producer_country,
                })
                # временный отрицательный id, позже заменится
                canonical_id = -len(new_canonicals)

                canonical_map[(name_key, item.producer)] = canonical_id

            # 4️⃣ собираем алиасы
            for b in barcodes:
                if b not in alias_map:
                    new_aliases.append({
                        "provider_name": item.provider_name,
                        "barcode": b,
                        "canonical_id": canonical_id,
                    })
                    alias_map[b] = canonical_id

        # ✅ СОХРАНЕНИЕ
        
            # канонические товары
        if new_canonicals:
            ProductCanonicalRepo.bulk_create(db, new_canonicals)

            # обновляем реальные id
        real_canon = ProductCanonicalRepo.preload_all(db)

        for a in new_aliases:
            from uuid import UUID

            if not isinstance(a["canonical_id"], UUID):
    
                key = (
                    normalize_name(a.get("sku_name")),
                    None
                )
                a["canonical_id"] = real_canon.get(key)

            # алиасы
        BarcodeAliasRepo.bulk_create(db, new_aliases)
# ============================================================
# PRODUCT COMPARE SERVICE
# ============================================================

class ProductCompareService:

    @staticmethod
    def rebuild_from_hourly(db: Session) -> None:
        # 1️⃣ чистим витрину
        db.execute(text("TRUNCATE TABLE product_compare"))
        db.commit()

        # 2️⃣ наполняем витрину
        db.execute(text("""
            INSERT INTO product_compare (
                barcode,
                sku_name,
                price_atamiras,
                price_medservice,
                price_stopharm,
                price_amanat,
                price_rauza
            )
            SELECT
                MIN(b.code)              AS barcode,
                MIN(h.sku_name)          AS sku_name,

                MAX(h.sku_price)
                    FILTER (WHERE h.provider_name = 'atamiras') AS price_atamiras,

                MAX(h.sku_price)
                    FILTER (WHERE h.provider_name = 'medservice') AS price_medservice,

                MAX(h.sku_price)
                    FILTER (WHERE h.provider_name = 'stopharm') AS price_stopharm,

                MAX(h.sku_price)
                    FILTER (WHERE h.provider_name = 'amanat') AS price_amanat,

                MAX(h.sku_price)
                    FILTER (WHERE h.provider_name = 'rauza') AS price_rauza

            FROM hourly_products h
            JOIN LATERAL jsonb_array_elements_text(h.sku_barcodes) AS b(code) ON TRUE
            JOIN barcode_aliases ba ON ba.barcode = b.code
            JOIN product_canonical pc ON pc.id = ba.canonical_id

            WHERE
                h.sku_barcodes IS NOT NULL
                AND jsonb_array_length(h.sku_barcodes) > 0

            GROUP BY pc.id
        """))
        db.commit()
        

# ============================================================
# POST PROCESS SERVICE (CANONICAL + COMPARE)
# ============================================================

class PostProcessService:

    @staticmethod
    def rebuild_all(db: Session) -> dict:
        started = datetime.utcnow()

        CanonicalResolveService.rebuild_from_hourly(db)
        ProductCompareService.rebuild_from_hourly(db)

        finished = datetime.utcnow()

        return {
            "status": "success",
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "duration_seconds": (finished - started).total_seconds(),
        }
