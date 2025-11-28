from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from deps import get_db
from schemas import CityResponseCreate, CityResponseRead
from repositories import CityResponseRepo

router = APIRouter(prefix="/city-response", tags=["City Response"])


@router.post("/", response_model=CityResponseRead)
def create_city(data: CityResponseCreate, db: Session = Depends(get_db)):

    city = CityResponseRepo.create(
        db=db,
        provider_name=data.provider_name,
        supplier_city_code=data.supplier_city_code,
        supplier_city_name=data.supplier_city_name,
        normalized_city=data.normalized_city,
    )

    return city
