# repositories.py
# ============================================================
# РЕПОЗИТОРИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ
# ============================================================

from typing import List, Optional
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import (
    Supplier,
    SupplierMapping,
    HourlyProduct,
    DailyProduct,
    SupplierCity,
    CityResponse,
    SupplierUnit,
    ProductCanonical,
    BarcodeAlias,
    SupplierSrokResponse,
)

from utils import clean_unit


# ============================================================
# SUPPLIER UNIT REPO — нормализация единиц измерения
# ============================================================

class SupplierUnitRepo:

    @staticmethod
    def find(db: Session, provider: str, raw_unit: str):
        """
        Ищем нормализованную единицу измерения.
        1) точное совпадение (cleaned)
        2) мягкий поиск через ilike
        """
        cleaned = clean_unit(raw_unit).lower().strip()

        if not cleaned:
            return None

        # --- 1: точное совпадение ---
        record = (
            db.query(SupplierUnit)
            .filter(
                SupplierUnit.provider_name == provider,
                func.lower(SupplierUnit.supplier_unit) == cleaned
            )
            .first()
        )
        if record:
            return record

        # --- 2: мягкое совпадение ---
        return (
            db.query(SupplierUnit)
            .filter(
                SupplierUnit.provider_name == provider,
                SupplierUnit.supplier_unit.ilike(f"%{cleaned}%")
            )
            .first()
        )


# ============================================================
# SUPPLIER REPO
# ============================================================

class SupplierRepo:

    @staticmethod
    def get_all(db: Session) -> List[Supplier]:
        return db.query(Supplier).all()

    @staticmethod
    def get_active(db: Session) -> List[Supplier]:
        return db.query(Supplier).filter(Supplier.is_active.is_(True)).all()

    @staticmethod
    def get_by_id(db: Session, supplier_id) -> Optional[Supplier]:
        return db.query(Supplier).filter(Supplier.id == supplier_id).first()

    @staticmethod
    def get_by_name(db: Session, provider_name: str) -> Optional[Supplier]:
        return (
            db.query(Supplier)
            .filter(Supplier.provider_name == provider_name)
            .first()
        )

    @staticmethod
    def create(db: Session, supplier: Supplier) -> Supplier:
        db.add(supplier)
        db.commit()
        db.refresh(supplier)
        return supplier

    @staticmethod
    def update(db: Session):
        db.commit()

    @staticmethod
    def delete(db: Session, supplier: Supplier):
        db.delete(supplier)
        db.commit()


# ============================================================
# MAPPING REPO
# ============================================================

class MappingRepo:

    @staticmethod
    def get_all(db: Session) -> List[SupplierMapping]:
        return db.query(SupplierMapping).all()

    @staticmethod
    def get_by_provider(db: Session, provider_name: str) -> Optional[SupplierMapping]:
        return (
            db.query(SupplierMapping)
            .filter(SupplierMapping.provider_name == provider_name)
            .first()
        )

    @staticmethod
    def create(db: Session, mapping: SupplierMapping) -> SupplierMapping:
        db.add(mapping)
        db.commit()
        db.refresh(mapping)
        return mapping

    @staticmethod
    def update(db: Session):
        db.commit()

    @staticmethod
    def delete(db: Session, mapping: SupplierMapping):
        db.delete(mapping)
        db.commit()


# ============================================================
# SUPPLIER CITY REPO — старый механизм
# ============================================================

class SupplierCityRepo:

    @staticmethod
    def get(db: Session, provider_name: str, supplier_city_code: str) -> Optional[SupplierCity]:
        return (
            db.query(SupplierCity)
            .filter(
                SupplierCity.provider_name == provider_name,
                SupplierCity.supplier_city_code == supplier_city_code,
            )
            .first()
        )

    @staticmethod
    def create(db: Session, provider_name: str, supplier_city_code: str, normalized_city: str) -> SupplierCity:
        city = SupplierCity(
            provider_name=provider_name,
            supplier_city_code=supplier_city_code,
            normalized_city=normalized_city,
        )
        db.add(city)
        db.commit()
        db.refresh(city)
        return city

    @staticmethod
    def get_or_create(db: Session, provider_name: str, supplier_city_code: str, normalized_city: str) -> SupplierCity:
        city = SupplierCityRepo.get(db, provider_name, supplier_city_code)
        if city:
            return city
        return SupplierCityRepo.create(db, provider_name, supplier_city_code, normalized_city)


# ============================================================
# CITY RESPONSE — главная логика городов
# ============================================================

class CityResponseRepo:

    @staticmethod
    def find_city(
        db: Session,
        provider_name: str,
        code: str | None,
        name: str | None,
    ) -> Optional[CityResponse]:

        q = db.query(CityResponse).filter(
            CityResponse.provider_name == provider_name
        )

        if code:
            q = q.filter(CityResponse.supplier_city_code == code)

        if name:
            q = q.filter(CityResponse.supplier_city_name.ilike(name))

        return q.first()

    @staticmethod
    def create(
        db: Session,
        provider_name: str,
        supplier_city_code: str | None,
        supplier_city_name: str | None,
        normalized_city: str,
    ) -> CityResponse:

        city = CityResponse(
            provider_name=provider_name,
            supplier_city_code=supplier_city_code,
            supplier_city_name=supplier_city_name,
            normalized_city=normalized_city,
        )

        db.add(city)
        db.commit()
        db.refresh(city)

        return city


# ============================================================
# HOURLY PRODUCTS
# ============================================================

class HourlyRepo:

    @staticmethod
    def bulk_create(db: Session, items: List[HourlyProduct]):
        db.bulk_save_objects(items)
        db.commit()

    @staticmethod
    def get_all(db: Session):
        return db.query(HourlyProduct).all()

    @staticmethod
    def clear_table(db: Session):
        db.query(HourlyProduct).delete()
        db.commit()

    @staticmethod
    def get_latest_by_barcode(
        db: Session,
        barcode: str,
        city: Optional[str] = None
    ) -> List[HourlyProduct]:

        barcode_json = [str(barcode)]

        base_query = (
            db.query(
                HourlyProduct.provider_name,
                HourlyProduct.sku_uid,
                HourlyProduct.city,
                func.max(HourlyProduct.created_at).label("max_date")
            )
            .filter(HourlyProduct.sku_barcodes.contains(barcode_json))
        )

        if city:
            base_query = base_query.filter(HourlyProduct.city == city)

        subquery = (
            base_query
            .group_by(
                HourlyProduct.provider_name,
                HourlyProduct.sku_uid,
                HourlyProduct.city
            )
            .subquery()
        )

        return (
            db.query(HourlyProduct)
            .join(
                subquery,
                (HourlyProduct.provider_name == subquery.c.provider_name) &
                (HourlyProduct.sku_uid == subquery.c.sku_uid) &
                (HourlyProduct.city == subquery.c.city) &
                (HourlyProduct.created_at == subquery.c.max_date)
            )
            .order_by(HourlyProduct.provider_name)
            .all()
        )

    @staticmethod
    def get_for_period(
        db: Session,
        start_dt: datetime,
        end_dt: datetime,
        provider_name: Optional[str] = None,
        city: Optional[str] = None,
    ) -> List[HourlyProduct]:

        q = db.query(HourlyProduct).filter(
            HourlyProduct.created_at >= start_dt,
            HourlyProduct.created_at <= end_dt,
        )

        if provider_name:
            q = q.filter(HourlyProduct.provider_name == provider_name)

        if city:
            q = q.filter(HourlyProduct.city == city)

        return q.all()


# ============================================================
# DAILY PRODUCTS
# ============================================================

class DailyRepo:

    @staticmethod
    def bulk_create(db: Session, items: List[DailyProduct]):
        db.bulk_save_objects(items)
        db.commit()

    @staticmethod
    def get_all(db: Session):
        return db.query(DailyProduct).all()

    @staticmethod
    def clear_by_date(db: Session, snapshot_date):
        db.query(DailyProduct).filter(
            DailyProduct.snapshot_date == snapshot_date
        ).delete()
        db.commit()

    @staticmethod
    def get_latest_by_barcode(
        db: Session,
        barcode: str,
        city: Optional[str] = None
    ) -> List[DailyProduct]:

        barcode_json = [str(barcode)]

        base_query = (
            db.query(
                DailyProduct.provider_name,
                DailyProduct.city,
                func.max(DailyProduct.created_at).label("max_date")
            )
            .filter(DailyProduct.sku_barcodes.contains(barcode_json))
        )

        if city:
            base_query = base_query.filter(DailyProduct.city == city)

        subquery = (
            base_query
            .group_by(
                DailyProduct.provider_name,
                DailyProduct.city
            )
            .subquery()
        )

        return (
            db.query(DailyProduct)
            .join(
                subquery,
                (DailyProduct.provider_name == subquery.c.provider_name) &
                (DailyProduct.city == subquery.c.city) &
                (DailyProduct.created_at == subquery.c.max_date)
            )
            .order_by(DailyProduct.provider_name)
            .all()
        )

    @staticmethod
    def get_range(db: Session, start_date, end_date, provider_name=None, city=None):
        q = db.query(DailyProduct).filter(
            DailyProduct.snapshot_date >= start_date,
            DailyProduct.snapshot_date <= end_date
        )

        if provider_name:
            q = q.filter(DailyProduct.provider_name == provider_name)

        if city:
            q = q.filter(DailyProduct.city == city)

        return q.all()


# ============================================================
# PRODUCT CANONICAL REPO
# ============================================================
from sqlalchemy import select
from typing import Tuple, Dict, Iterable
from sqlalchemy.dialects.postgresql import insert
class ProductCanonicalRepo:

    @staticmethod
    def preload_all(db: Session) -> Dict[Tuple[str, Optional[str]], int]:
        """
        ⚡ Загружаем ВСЕ канонические товары в память
        key = (name_key, producer)
        value = canonical_id
        """
        rows = db.execute(
            select(
                ProductCanonical.id,
                ProductCanonical.name_key,
                ProductCanonical.producer,
            )
        ).all()

        return {
            (r.name_key, r.producer): r.id
            for r in rows
            if r.name_key
        }

    @staticmethod
    def bulk_create(
        db: Session,
        items: Iterable[dict],
    ) -> None:
        """
        items = [{
            canonical_barcode,
            name_key,
            producer,
            producer_country
        }]
        """
        if not items:
            return

        db.bulk_insert_mappings(ProductCanonical, list(items))

# ============================================================
# BARCODE ALIAS REPO
# ============================================================

class BarcodeAliasRepo:

    @staticmethod
    def preload_all(db: Session) -> Dict[str, int]:
        """
        ⚡ barcode -> canonical_id
        """
        rows = db.execute(
            select(
                BarcodeAlias.barcode,
                BarcodeAlias.canonical_id,
            )
        ).all()

        return {r.barcode: r.canonical_id for r in rows}
    @staticmethod
    def bulk_create(db: Session, rows: list[dict]) -> None:
        rows = [r for r in rows if r["canonical_id"] is not None]

        if not rows:
            return

        db.execute(
            insert(BarcodeAlias)
            .values(rows)
            .on_conflict_do_nothing(index_elements=["barcode"])
        )
   

class SupplierSrokRepo:

    @staticmethod
    def get_by_provider(db: Session, provider_name: str) -> SupplierSrokResponse | None:
        """
        Возвращает конфигурацию формата срока для поставщика.
        Ожидается, что в таблице ОДНА строка на provider_name,
        где provider_srok_raw = маска, например 'yyyymmdd'.
        """
        return (
            db.query(SupplierSrokResponse)
            .filter(SupplierSrokResponse.provider_name == provider_name)
            .first()
        )
