from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from deps import get_db
from models import CityResponse
from repositories import CityResponseRepo
from schemas import CityResponseCreate, CityResponseRead

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


@router.get("/list_normalized_cities", response_model=list[CityResponseRead])
def get_cities(provider_name: str | None = Query(None), db: Session = Depends(get_db)):
    if provider_name:
        cities = (
            db.query(CityResponse)
            .filter(CityResponse.provider_name == provider_name)
            .all()
        )
    else:
        cities = db.query(CityResponse).all()

    return cities
