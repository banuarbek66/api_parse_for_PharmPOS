import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# ------------------------------------------------------------
# PATH
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

# ------------------------------------------------------------
# Alembic config
# ------------------------------------------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ------------------------------------------------------------
# IMPORT ENGINE + BASE
# ------------------------------------------------------------
from database import engine, Base  # ← ТВОЙ database.py

# ------------------------------------------------------------
# IMPORT ALL MODELS (ОБЯЗАТЕЛЬНО!)
# ------------------------------------------------------------
from models import (  # noqa
    Supplier,
    SupplierMapping,
    HourlyProduct,
    DailyProduct,
    CityResponse,
    ProductCanonical,
    BarcodeAlias,
    ProductCompare,
    SupplierCity,
    SupplierUnit,
    SupplierSrokResponse,
)
from stock_movement_model import StockMovement, StockMovementType
from stock_movement_cursor import StockMovementCursor

# ------------------------------------------------------------
# TARGET METADATA
# ------------------------------------------------------------
target_metadata = Base.metadata


# ------------------------------------------------------------
# OFFLINE MODE
# ------------------------------------------------------------
def run_migrations_offline():
    context.configure(
        url=str(engine.url),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ------------------------------------------------------------
# ONLINE MODE
# ------------------------------------------------------------
def run_migrations_online():
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# ------------------------------------------------------------
# ENTRYPOINT
# ------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
