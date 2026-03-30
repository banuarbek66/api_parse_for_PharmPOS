# v1/mappings.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from deps import get_db
from models import SupplierMapping
from repositories import MappingRepo
from schemas import SupplierMappingCreate, SupplierMappingRead
from services import SupplierMappingService

router = APIRouter(prefix="/mappings", tags=["Mappings"])


# =====================================================
# GET ALL MAPPINGS
# =====================================================


@router.get("/", response_model=list[SupplierMappingRead])
def get_mappings(db: Session = Depends(get_db)):
    return MappingRepo.get_all(db)


# =====================================================
# CREATE MAPPING + AUTO SYNC
# =====================================================


@router.post("/", response_model=dict)
def create_mapping(data: SupplierMappingCreate, db: Session = Depends(get_db)):
    """
    ✅ Создаёт mapping
    ✅ Сразу делает авто-синхронизацию поставщика
    ✅ Возвращает результат синхронизации
    """
    result = SupplierMappingService.create_mapping(db, data)
    return result


# =====================================================
# GET MAPPING BY PROVIDER
# =====================================================


@router.get("/{provider_name}", response_model=SupplierMappingRead)
def get_mapping(provider_name: str, db: Session = Depends(get_db)):

    mapping = MappingRepo.get_by_provider(db, provider_name)

    if not mapping:
        raise HTTPException(
            status_code=404, detail=f"Mapping for provider '{provider_name}' not found"
        )

    return mapping


# =====================================================
# UPDATE MAPPING
# =====================================================


@router.put("/{provider_name}", response_model=SupplierMappingRead)
def update_mapping(
    provider_name: str, data: SupplierMappingCreate, db: Session = Depends(get_db)
):
    """
    Обновление маппинга по provider_name
    """

    mapping = MappingRepo.get_by_provider(db, provider_name)

    if not mapping:
        raise HTTPException(
            status_code=404, detail=f"Mapping for provider '{provider_name}' not found"
        )

    for key, value in data.dict().items():
        setattr(mapping, key, value)

    db.commit()
    db.refresh(mapping)

    return mapping


# =====================================================
# DELETE MAPPING
# =====================================================


@router.delete("/{provider_name}")
def delete_mapping(provider_name: str, db: Session = Depends(get_db)):

    mapping = MappingRepo.get_by_provider(db, provider_name)

    if not mapping:
        raise HTTPException(
            status_code=404, detail=f"Mapping for provider '{provider_name}' not found"
        )

    MappingRepo.delete(db, mapping)

    return {"status": "deleted", "provider": provider_name}
