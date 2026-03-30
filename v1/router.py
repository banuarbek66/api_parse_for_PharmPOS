# v1/router.py

from fastapi import APIRouter

import v1.analytics
import v1.cities
import v1.client
import v1.health
import v1.mappings
import v1.products
import v1.supliers
import v1.supplier_srok
import v1.sync
import v1.units

router = APIRouter(prefix="/v1")

router.include_router(v1.supliers.router, tags=["Suppliers"])
router.include_router(v1.mappings.router, tags=["Mappings"])
router.include_router(v1.products.router, tags=["Products"])
router.include_router(v1.sync.router, tags=["Sync"])
router.include_router(v1.health.router, tags=["Health"])
router.include_router(v1.cities.router, tags=["city-response"])
router.include_router(v1.units.router, tags=["Units"])
router.include_router(v1.analytics.router, tags=["Analytics"])
router.include_router(v1.client.router, tags=["Clients"])

router.include_router(v1.supplier_srok.router, tags=["Supplier Srok Normalizations"])
