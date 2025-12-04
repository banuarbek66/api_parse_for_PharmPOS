from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from deps import get_db

import core
import schemas

router = APIRouter(prefix="/v1/units", tags=["Units"])

@router.post("/", response_model=schemas.SupplierUnitOut)
def create_unit(data: schemas.SupplierUnitCreate, db: Session = Depends(get_db)):
    return core.create_or_update_supplier_unit(db, data)
