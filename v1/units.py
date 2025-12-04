from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from deps import get_db

import core
import schemas
import models
router = APIRouter(prefix="/v1/units", tags=["Units"])

@router.post("/", response_model=schemas.SupplierUnitOut)
def create_unit(data: schemas.SupplierUnitCreate, db: Session = Depends(get_db)):
    return core.create_or_update_supplier_unit(db, data)


@router.get("/", response_model=list[schemas.SupplierUnitOut])
def list_units(
    provider_name: str | None = None,
    db: Session = Depends(get_db)
):
    q = db.query(models.SupplierUnit)
    if provider_name:
        q = q.filter(models.SupplierUnit.provider_name == provider_name)
    return q.all()