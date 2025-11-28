import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()
user = "postgres"

host = "localhost"
port = "5432"
database = "responce_pharmpos"
password = quote_plus("Qawsed1.")
DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
print("DB URL:", DATABASE_URL)
engine = create_engine(DATABASE_URL, future=True)
@event.listens_for(engine, "connect")
def set_client_encoding(dbapi_connection, connection_record):
    try:
        dbapi_connection.set_client_encoding("UTF8")
    except Exception as e:
        print("WARNING: could not set client_encoding:", e)

SessionLocal = sessionmaker(bind=engine, autoflush=True)
print("DB URL:", DATABASE_URL)


class Base(DeclarativeBase):
    pass





# ------------------------------------------------------------
# Ручной запуск создания таблиц (если нужно)
# Эта функция вызывается один раз при старте проекта
# ------------------------------------------------------------

