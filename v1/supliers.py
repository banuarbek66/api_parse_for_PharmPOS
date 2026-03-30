from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from deps import get_db
from models import Supplier
from repositories import SupplierRepo
from schemas import SupplierCreate, SupplierRead

router = APIRouter(prefix="/suppliers", tags=["Suppliers"])


# =====================================
# GET ALL SUPPLIERS
# =====================================
@router.get("/", response_model=list[SupplierRead])
def get_suppliers(db: Session = Depends(get_db)):
    suppliers = SupplierRepo.get_all(db)
    return [SupplierRead.model_validate(s) for s in suppliers]


# =====================================
# CREATE SUPPLIER
# =====================================
@router.post("/", response_model=SupplierRead)
def create_supplier(data: SupplierCreate, db: Session = Depends(get_db)):

    # Проверка на дубликат по имени
    exists = SupplierRepo.get_by_name(db, data.provider_name)
    if exists:
        raise HTTPException(
            status_code=400, detail=f"Supplier '{data.provider_name}' already exists"
        )

    supplier = Supplier(**data.model_dump())
    return SupplierRepo.create(db, supplier)


# =====================================
# GET BY ID
# =====================================
@router.get("/id/{supplier_id}", response_model=SupplierRead)
def get_supplier_by_id(supplier_id: UUID, db: Session = Depends(get_db)):

    supplier = SupplierRepo.get_by_id(db, supplier_id)

    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    return SupplierRead.model_validate(supplier)


# =====================================
# GET BY PROVIDER NAME
# =====================================
@router.get("/by-name/{provider_name}", response_model=SupplierRead)
def get_supplier_by_name(provider_name: str, db: Session = Depends(get_db)):

    supplier = SupplierRepo.get_by_name(db, provider_name)

    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    return SupplierRead.model_validate(supplier)


# =====================================
# UPDATE SUPPLIER
# =====================================
@router.put("/{supplier_id}", response_model=SupplierRead)
def update_supplier(
    supplier_id: UUID,
    data: SupplierCreate,
    db: Session = Depends(get_db),
):

    supplier = SupplierRepo.get_by_id(db, supplier_id)

    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    for field, value in data.model_dump().items():
        setattr(supplier, field, value)

    SupplierRepo.update(db)

    return SupplierRead.model_validate(supplier)


# =====================================
# DELETE SUPPLIER
# =====================================
@router.delete("/{supplier_id}")
def delete_supplier(supplier_id: UUID, db: Session = Depends(get_db)):

    supplier = SupplierRepo.get_by_id(db, supplier_id)

    if not supplier:
        raise HTTPException(status_code=404, detail="Supplier not found")

    SupplierRepo.delete(db, supplier)

    return {"status": "deleted", "supplier_id": str(supplier_id)}
