from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from deps import get_db
from models import SupplierSrokResponse
from repositories import SupplierSrokRepo
from utils import normalize_srok

from schemas import (
    SupplierSrokCreate,
    SupplierSrokUpdate,
    SupplierSrokResponseSchema,
)

router = APIRouter(prefix="/supplier-srok", tags=["Supplier Srok"])


# ------------------------------------------------------------
# GET ALL
# ------------------------------------------------------------
@router.get("/", response_model=list[SupplierSrokResponseSchema])
def get_all(db: Session = Depends(get_db)):
    rows = db.query(SupplierSrokResponse).all()
    return rows


# ------------------------------------------------------------
# GET ONE
# ------------------------------------------------------------
@router.get("/{item_id}", response_model=SupplierSrokResponseSchema)
def get_one(item_id: int, db: Session = Depends(get_db)):
    row = db.query(SupplierSrokResponse).filter_by(id=item_id).first()
    if not row:
        raise HTTPException(404, "Srok not found")
    return row


# ------------------------------------------------------------
# CREATE
# ------------------------------------------------------------
@router.post("/", response_model=SupplierSrokResponseSchema)
def create(data: SupplierSrokCreate, db: Session = Depends(get_db)):
    normalized = normalize_srok(data.provider_srok_raw)

    row = SupplierSrokResponse(
        provider_name=data.provider_name,
        provider_srok_raw=data.provider_srok_raw,
        normalized_srok=normalized,
    )

    db.add(row)
    db.commit()
    db.refresh(row)

    return row


# ------------------------------------------------------------
# UPDATE
# ------------------------------------------------------------
@router.put("/{item_id}", response_model=SupplierSrokResponseSchema)
def update(item_id: int, data: SupplierSrokUpdate, db: Session = Depends(get_db)):
    row = db.query(SupplierSrokResponse).filter_by(id=item_id).first()

    if not row:
        raise HTTPException(404, "Srok not found")

    if data.provider_srok_raw is not None:
        row.provider_srok_raw = data.provider_srok_raw
        row.normalized_srok = normalize_srok(data.provider_srok_raw)

    db.commit()
    db.refresh(row)

    return row


# ------------------------------------------------------------
# DELETE
# ------------------------------------------------------------
@router.delete("/{item_id}")
def delete(item_id: int, db: Session = Depends(get_db)):
    row = db.query(SupplierSrokResponse).filter_by(id=item_id).first()

    if not row:
        raise HTTPException(404, "Srok not found")

    db.delete(row)
    db.commit()

    return {"status": "success", "message": "Deleted"}
