from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from deps import get_db
from services import ProductService
from schemas import ProductByBarcodeResponse

router = APIRouter(prefix="/products", tags=["Products"])


@router.get("/barcode/{sku_barcode}", response_model=ProductByBarcodeResponse)
def get_product_by_barcode(
    sku_barcode: str,
    city: str | None = Query(
        None,
        description="Город (необязательно). Если указан — вернёт данные только по этому городу",
        example="almaty"
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
    )
    


    return {
        "sku_barcode": barcode,
        "city": city,
        "results": results
    }
