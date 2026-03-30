from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from deps import get_db
from repositories import SupplierRepo
from services import PostProcessService, StockMovementService, SyncService

router = APIRouter(prefix="/sync", tags=["Sync"])


# ===============================
# SYNC ALL PROVIDERS (HOURLY)
# ===============================
@router.post("/hourly")
def run_hourly(db: Session = Depends(get_db)):
    result = SyncService.run_hourly_sync(db)

    return {"status": "completed", "total_suppliers": len(result), "details": result}


# ===============================
# SYNC ONE SUPPLIER BY NAME
# ===============================
@router.post("/supplier/{provider_name}")
def run_supplier(provider_name: str, db: Session = Depends(get_db)):

    supplier = SupplierRepo.get_by_name(db, provider_name)

    if not supplier:
        return {"status": "error", "message": f"Supplier '{provider_name}' not found"}

    result = SyncService.sync_single_supplier(db, supplier)

    return {"status": "completed", "provider": provider_name, "result": result}


# ===============================
# DAILY SNAPSHOT 23:50
# ===============================
@router.post("/daily")
def run_daily(db: Session = Depends(get_db)):
    count = SyncService.run_daily_snapshot(db)

    return {"status": "completed", "copied_records": count}


# ===============================
# CLEANUP HOURLY (00:00)
# ===============================
@router.post("/cleanup")
def cleanup(db: Session = Depends(get_db)):
    SyncService.cleanup_hourly_table(db)

    return {"status": "cleaned", "message": "hourly_products table cleared"}


@router.post("/run", summary="Postprocess hourly products")
def run_postprocess(
    db: Session = Depends(get_db),
):
    return PostProcessService.rebuild_all(db)


@router.post("/stockmovement")
def run_stockmovement(db: Session = Depends(get_db)):
    a = StockMovementService.build_hourly_movements_all(db)

    return a
