from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from deps import get_db
from schemas import ProductByBarcodeResponse
from services import ProductService

router = APIRouter(prefix="/products", tags=["Products"])


@router.get("/barcode/{sku_barcode}", response_model=ProductByBarcodeResponse)
def get_product_by_barcode(
    sku_barcode: str,
    client_uid: UUID | None = Query(None),
    city: str | None = Query(
        None,
        description="Город (необязательно). Если указан — вернёт данные только по этому городу",
        example="almaty",
    ),
    provider_name: str = Query(None),
    db: Session = Depends(get_db),
):
    """
    ✅ Главный эндпоинт PharmPOS

    Получает товары по ШТРИХКОДУ.
    - ищет в hourly_products (самые свежие)
    - если не нашёл — ищет в daily_products
    - если передан city — фильтрует по городу
    """

    barcode = sku_barcode.strip()
    city = city.strip().lower() if city else None

    results = ProductService.get_by_barcode(
        db=db,
        barcode=barcode,
        city=city,
        provider_name=provider_name,
        client_uid=client_uid,
    )

    return {"sku_barcode": barcode, "city": city, "results": results}
