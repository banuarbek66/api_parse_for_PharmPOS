# repositories.py
# ============================================================
# РЕПОЗИТОРИИ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ
# ============================================================

from __future__ import annotations

from typing import List, Optional, Dict, Tuple, Iterable
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, text, select, or_, and_, cast as sqcast, Date, Time
from sqlalchemy.orm import Session
from sqlalchemy.engine import CursorResult
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
    PostProcessState
)

from utils import clean_unit



from typing import Iterable, Optional
from uuid import UUID
from sqlalchemy import case
from sqlalchemy.orm import Session
from sqlalchemy import func, true
from sqlalchemy.dialects.postgresql import UUID as SqlUUID
from stock_movement_model import StockMovement, StockMovementType
from stock_movement_cursor import StockMovementCursor
from datetime import datetime, time, timedelta, date
from sqlalchemy import values as sa_values
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
        cleaned = clean_unit(raw_unit)
        if not cleaned:
            return None

        cleaned = cleaned.lower().strip()

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
        """
        Логика:
        - если есть code -> ищем строго по supplier_city_code
        - иначе если есть name -> ищем по supplier_city_name (ILIKE)
        """
        q = db.query(CityResponse).filter(CityResponse.provider_name == provider_name)

        if code:
            q = q.filter(CityResponse.supplier_city_code == code)
            return q.first()

        if name:
            # мягкий поиск по имени
            q = q.filter(CityResponse.supplier_city_name.ilike(f"%{name}%"))
            return q.first()

        return None

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
        if not items:
            return
        db.bulk_save_objects(items)
        db.commit()

    @staticmethod
    def get_all_after_23(db: Session) -> List[HourlyProduct]:
        """
        ORM-only.
        Корректно для PostgreSQL.
        Совпадает с scheduler (Asia/Almaty).
        Работает при created_at в UTC или naive.
        """

        created_at_kz = func.timezone("Asia/Almaty", HourlyProduct.created_at)
        now_kz = func.timezone("Asia/Almaty", func.now())

        return (
            db.query(HourlyProduct)
            .filter(
                and_(
                    sqcast(created_at_kz, Date) == sqcast(now_kz, Date),
                    sqcast(created_at_kz, Time) >= time(1, 0),
                    sqcast(created_at_kz, Time) <= time(23, 50),
                )
            )
            .all()
        )

    @staticmethod
    def clear_table(db: Session):
        db.query(HourlyProduct).delete()
        db.commit()

    @staticmethod
    def get_latest_by_barcode(
        db: Session,
        barcode: str,
        city: Optional[str] = None,
        provider_name: Optional[str] = None,
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

        if provider_name:
            base_query = base_query.filter(HourlyProduct.provider_name == provider_name)

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

    @staticmethod
    def get_latest(db: Session):
        # Возвращает строки (Row), как в твоём SQL-использовании
        return db.execute(text("""
            SELECT DISTINCT ON (b.code)
                   h.*
            FROM hourly_products h
            JOIN LATERAL jsonb_array_elements_text(h.sku_barcodes) AS b(code) ON TRUE
            WHERE jsonb_array_length(h.sku_barcodes) > 0
            ORDER BY b.code, h.created_at DESC;
        """)).all()
    
    @staticmethod
    def get_newer_than(
        db: Session,
        last_hourly_at,
    ):
        q = db.query(HourlyProduct)

        if last_hourly_at:
            q = q.filter(HourlyProduct.created_at > last_hourly_at)

        return q.order_by(HourlyProduct.created_at.asc()).all()
    
    @staticmethod
    def get_prev_snapshot_for_canonical(
        db: Session,
        *,
        provider_name: str,
        canonical_id: UUID,
        city: Optional[str],
        before_dt: datetime,
    ) -> Optional[HourlyProduct]:
        """
        Возвращает последний snapshot ДО before_dt
        """

        q = db.query(HourlyProduct).filter(
            HourlyProduct.provider_name == provider_name,
            HourlyProduct.canonical_id == canonical_id,
            HourlyProduct.created_at < before_dt,
        )

        if city is None:
            q = q.filter(HourlyProduct.city.is_(None))
        else:
            q = q.filter(HourlyProduct.city == city)

        return q.order_by(HourlyProduct.created_at.desc()).first()
    
    @staticmethod
    def attach_canonical_ids(db: Session) -> int:
        """
        Проставляет canonical_id в hourly_products,
        где canonical_id IS NULL.

        Возвращает количество обновлённых строк.
        """

        rows = (
            db.query(HourlyProduct)
            .filter(HourlyProduct.canonical_id.is_(None))
            .all()
        )

        updated = 0

        for row in rows:
            barcodes = row.sku_barcodes or []
            canonical_id = None

            for b in barcodes:
                s = str(b).strip()
                if not s.isdigit():
                    continue

                canonical_id = BarcodeAliasRepo.get_canonical_id_by_barcode(
                    db, s
                )
                if canonical_id:
                    break

            if canonical_id:
                row.canonical_id = canonical_id
                updated += 1
        if updated:
            db.flush()
        
            

        return updated
    @staticmethod
    def get_changed_canonical_ids(
        db: Session,
        *,
        last_hourly_at,
    ) -> list[UUID]:
        """
        Возвращает список canonical_id,
        которые затронуты новыми hourly записями
        """
        q = db.query(
            HourlyProduct.canonical_id
        ).filter(
            HourlyProduct.canonical_id.is_not(None)
        )

        if last_hourly_at:
            q = q.filter(HourlyProduct.created_at > last_hourly_at)

        rows = q.distinct().all()
        return [r[0] for r in rows if r[0] is not None]
    @staticmethod
    def get_newer_than_for_provider(db: Session, *, provider_name: str, since: datetime | None):
        q = db.query(HourlyProduct).filter(HourlyProduct.provider_name==provider_name)
        if since:
            q = q.filter(HourlyProduct.created_at>since)
        return q.order_by(HourlyProduct.created_at.asc()).all()

    @staticmethod
    def get_prev_snapshots_batch(
        db: Session,
        *,
        provider_name: str,
        keys: Iterable[Tuple[UUID, Optional[str]]],
        before_dt: datetime,
    ) -> Dict[Tuple[UUID, Optional[str]], Optional[str]]:

        keys = list(keys)
        if not keys:
            return {}

        canonical_ids = {cid for cid, _ in keys}

        # -----------------------------
        # BASE QUERY WITH WINDOW
        # -----------------------------
        base_q = (
            db.query(
                HourlyProduct.canonical_id.label("canonical_id"),
                HourlyProduct.city.label("city"),
                HourlyProduct.sku_stock.label("sku_stock"),
                func.row_number()
                .over(
                    partition_by=(
                        HourlyProduct.canonical_id,
                        HourlyProduct.city,
                    ),
                    order_by=HourlyProduct.created_at.desc(),
                )
                .label("rn"),
            )
            .filter(
                HourlyProduct.provider_name == provider_name,
                HourlyProduct.created_at < before_dt,
                HourlyProduct.canonical_id.in_(canonical_ids),
            )
        )

        # -----------------------------
        # NULL-SAFE CITY FILTER
        # -----------------------------
        city_conditions = []
        for cid, city in keys:
            if city is None:
                city_conditions.append(
                    and_(
                        HourlyProduct.canonical_id == cid,
                        HourlyProduct.city.is_(None),
                    )
                )
            else:
                city_conditions.append(
                    and_(
                        HourlyProduct.canonical_id == cid,
                        HourlyProduct.city == city,
                    )
                )

        base_q = base_q.filter(or_(*city_conditions))

        # -----------------------------
        # SUBQUERY
        # -----------------------------
        subq = base_q.subquery()

        # -----------------------------
        # FINAL QUERY
        # -----------------------------
        rows = (
            db.query(
                subq.c.canonical_id,
                subq.c.city,
                subq.c.sku_stock,
            )
            .filter(subq.c.rn == 1)
            .all()
        )

        # -----------------------------
        # BUILD RESULT
        # -----------------------------
        result: Dict[Tuple[UUID, Optional[str]], Optional[str]] = {}
        for r in rows:
            result[(r.canonical_id, r.city)] = r.sku_stock

        return result
    
    @staticmethod
    def iter_newer_than_for_provider(
        db: Session,
        *,
        provider_name: str,
        since: datetime | None,
        chunk_size: int = 5000,
    ) -> Iterable[list[HourlyProduct]]:
        """
        Итератор по hourly_products чанками.
        НЕ грузит всё в память.
        """

        q = (
            db.query(HourlyProduct)
            .filter(HourlyProduct.provider_name == provider_name)
        )

        if since:
            q = q.filter(HourlyProduct.created_at > since)

        q = q.order_by(HourlyProduct.created_at.asc())

        offset = 0
        while True:
            batch = (
                q.limit(chunk_size)
                 .offset(offset)
                 .all()
            )
            if not batch:
                break

            yield batch
            offset += chunk_size
    @staticmethod
    def get_distinct_providers(db: Session) -> list[str]:
        return [
            r[0]
            for r in db.query(HourlyProduct.provider_name)
            .distinct()
            .all()
        ]

# ============================================================
# DAILY PRODUCTS
# ============================================================

class DailyRepo:

    @staticmethod
    def bulk_create(db: Session, items: List[DailyProduct]):
        if not items:
            return
        db.bulk_save_objects(items)
        db.commit()

    @staticmethod
    def get_all_after_23(
        db: Session,
        
    ):
        """
        Возвращает HourlyProduct, созданные после 23:00 указанной даты.
        """
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
        city: Optional[str] = None,
        provider_name: Optional[str] = None,
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

        if provider_name:
            base_query = base_query.filter(DailyProduct.provider_name == provider_name)

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

class ProductCanonicalRepo:

    @staticmethod
    def preload_all(db: Session) -> Dict[Tuple[str, Optional[str]], UUID]:
        rows = db.execute(
            select(
                ProductCanonical.id,
                ProductCanonical.name_key,
                ProductCanonical.producer,
            )
        ).all()

        return {
            (r.name_key, r.producer): r.id
            for r in rows if r.name_key
        }

    @staticmethod
    def find_by_name_and_producer(
        db: Session,
        *,
        name_key: str,
        producer: Optional[str],
    ) -> Optional[ProductCanonical]:
        q = db.query(ProductCanonical).filter(ProductCanonical.name_key == name_key)
        if producer is None:
            q = q.filter(ProductCanonical.producer.is_(None))
        else:
            q = q.filter(ProductCanonical.producer == producer)
        return q.first()

    @staticmethod
    def create(
        db: Session,
        *,
        canonical_barcode: Optional[str],
        name_key: Optional[str],
        producer: Optional[str],
        producer_country: Optional[str],
    ) -> ProductCanonical:
        obj = ProductCanonical(
            canonical_barcode=canonical_barcode,
            name_key=name_key,
            producer=producer,
            producer_country=producer_country,
        )
        db.add(obj)
        
        db.flush()
        return obj


# ============================================================
# BARCODE ALIAS REPO
# ============================================================

class BarcodeAliasRepo:

    @staticmethod
    def preload_all(db: Session) -> Dict[str, UUID]:
        """
        barcode -> canonical_id
        """
        rows = db.execute(
            select(
                BarcodeAlias.barcode,
                BarcodeAlias.canonical_id,
            )
        ).all()

        return {r.barcode: r.canonical_id for r in rows}

    @staticmethod
    def get_by_barcode(db: Session, barcode: str) -> Optional[BarcodeAlias]:
        return (
            db.query(BarcodeAlias)
            .filter(BarcodeAlias.barcode == str(barcode))
            .first()
        )

    @staticmethod
    def get_or_create(
        db: Session,
        *,
        provider_name: Optional[str],
        barcode: str,
        canonical_id: UUID,
    ) -> BarcodeAlias:
        barcode_str = str(barcode).strip()

        existing = (
            db.query(BarcodeAlias)
            .filter(BarcodeAlias.barcode == barcode_str)
            .first()
        )
        if existing:
            # если уже есть — гарантируем canonical_id (не ломая данные)
            if existing.canonical_id != canonical_id:
                existing.canonical_id = canonical_id
                existing.provider_name = provider_name
                db.commit()
                db.refresh(existing)
            return existing

        obj = BarcodeAlias(
            provider_name=provider_name,
            barcode=barcode_str,
            canonical_id=canonical_id,
        )
        db.add(obj)
        
        
        return obj
    
    @staticmethod
    def get_canonical_id_by_barcode(db: Session, barcode: str):
        rec = (
            db.query(BarcodeAlias)
            .filter(BarcodeAlias.barcode == barcode)
            .first()
        )
        return rec.canonical_id if rec else None


# ============================================================
# SUPPLIER SROK REPO
# ============================================================

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


class StockMovementRepo:

    @staticmethod
    def get_or_create_cursor(db: Session, provider_name: str) -> StockMovementCursor:
        cur = (
            db.query(StockMovementCursor)
            .filter(StockMovementCursor.provider_name == provider_name)
            .first()
        )
        if cur:
            return cur

        cur = StockMovementCursor(provider_name=provider_name, last_hourly_processed_at=None)
        db.add(cur)
        db.commit()
        db.refresh(cur)
        return cur

    @staticmethod
    def update_cursor(db: Session, cursor: StockMovementCursor, last_hourly_processed_at: datetime) -> None:
        cursor.last_hourly_processed_at = last_hourly_processed_at
        cursor.updated_at = datetime.utcnow()
        db.commit()

    @staticmethod
    def bulk_insert(db: Session, rows: list[StockMovement]) -> None:
        if not rows:
            return
        db.add_all(rows)
        db.commit()
    @staticmethod
    def bulk_insert_mappings_no_commit(
        db: Session,
        rows: list[dict],
    ) -> None:
        if not rows:
            return
        db.bulk_insert_mappings(StockMovement, rows)

    @staticmethod
    def aggregate_for_period(
        db: Session,
        *,
        start_dt: datetime,
        end_dt: datetime,
        provider_name: Optional[str] = None,
        city: Optional[str] = None,
        limit: int = 10,
    ) -> list[dict]:
        """
        Возвращает агрегат:
        - sold_qty = сумма ABS(delta) где delta < 0
        - restocked_qty = сумма delta где delta > 0
        - net_delta = сумма delta
        """
        q = db.query(
            StockMovement.provider_name.label("provider_name"),
            StockMovement.city.label("city"),
            StockMovement.canonical_id.label("canonical_id"),
            func.min(StockMovement.sku_uid).label("sku_uid"),
            func.min(StockMovement.sku_name).label("sku_name"),

            func.coalesce(
                func.sum(case((StockMovement.delta < 0, -StockMovement.delta), else_=0)),
                0
            ).label("sold_qty"),

            func.coalesce(
                func.sum(case((StockMovement.delta > 0, StockMovement.delta), else_=0)),
                0
            ).label("restocked_qty"),

            func.coalesce(func.sum(StockMovement.delta), 0).label("net_delta"),
        ).filter(
            StockMovement.snapshot_at >= start_dt,
            StockMovement.snapshot_at <= end_dt,
        )

        if provider_name:
            q = q.filter(StockMovement.provider_name == provider_name)
        if city:
            q = q.filter(StockMovement.city == city)

        q = q.group_by(
            StockMovement.provider_name,
            StockMovement.city,
            StockMovement.canonical_id,
        )

        # сортируем по sold_qty desc
        q = q.order_by(func.coalesce(
            func.sum(case((StockMovement.delta < 0, -StockMovement.delta), else_=0)),
            0
        ).desc())

        q = q.limit(limit)

        rows = q.all()
        return [
            {
                "provider_name": r.provider_name,
                "city": r.city,
                "canonical_id": r.canonical_id,
                "sku_uid": r.sku_uid,
                "sku_name": r.sku_name,
                "sold_qty": float(r.sold_qty or 0),
                "restocked_qty": float(r.restocked_qty or 0),
                "net_delta": float(r.net_delta or 0),
            }
            for r in rows
        ]
    



class PostProcessStateRepo:
    """
    ЕДИНЫЙ state postprocess (id = 1)

    Используется для:
    - контроля выполнения postprocess
    - cursor'а для инкрементального CanonicalResolve
    """

    # -------------------------------------------------
    # BASE
    # -------------------------------------------------
    @staticmethod
    def get(db: Session) -> PostProcessState:
        """
        Возвращает state (id=1), создаёт если не существует
        """
        state = db.query(PostProcessState).get(1)
        if not state:
            state = PostProcessState(
                id=1,
                status="idle",
                
                last_run_at=None,
                last_hourly_at=None,
            )
            db.add(state)
            db.commit()
            db.refresh(state)
        return state

    # -------------------------------------------------
    # STATUS CONTROL
    # -------------------------------------------------
    

    @staticmethod
    def set_success(
        db: Session,
        *,
        last_hourly_at: datetime | None,
    ) -> None:
        db.execute(text("""
            UPDATE postprocess_state
            SET status = 'success',
                last_run_at = now(),
                last_hourly_at = :h,
                updated_at = now()
            WHERE id = 1
        """), {"h": last_hourly_at})
        

    @staticmethod
    def set_failed(db: Session) -> None:
        db.execute(text("""
            UPDATE postprocess_state
            SET status = 'failed',
                updated_at = now()
            WHERE id = 1
        """))
        
    @staticmethod
    def try_set_running(db: Session) -> bool:
        """
        Пытаемся атомарно перевести postprocess в running.
        Возвращает:
        - True  → мы захватили lock
        - False → уже running
        """

        state = PostProcessStateRepo.get(db)

        if not state:
            raise RuntimeError("postprocess_state row not found")

        if state.status == "running":
            return False

        state.status = "running"
        state.updated_at = datetime.utcnow()

        
        return True

    # -------------------------------------------------
    # CURSOR (для CanonicalResolve)
    # -------------------------------------------------
    @staticmethod
    def get_last_hourly_at(db: Session):
        """
        Возвращает cursor для инкрементальной обработки hourly
        """
        state = PostProcessStateRepo.get(db)
        return state.last_hourly_at

    @staticmethod
    def update_last_hourly_at(
        db: Session,
        *,
        last_hourly_at,
    ) -> None:
        """
        Обновляет cursor после успешной обработки
        """
        db.execute(text("""
            UPDATE postprocess_state
            SET last_hourly_at = :hid,
                updated_at = now()
            WHERE id = 1
        """), {"hid": last_hourly_at})