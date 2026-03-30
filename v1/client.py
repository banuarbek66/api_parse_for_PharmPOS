from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from deps import get_db
from models import Client
from schemas import ClientResponse
from services import ClientService

router = APIRouter(prefix="/clients", tags=["Client"])


@router.get("/", response_model=List[ClientResponse])
def get_clients(db: Session = Depends(get_db)):
    clients = ClientService.get_clients(db)

    return clients


@router.get("/{client_uid}", response_model=ClientResponse)
def get_client(client_uid: UUID, db: Session = Depends(get_db)):
    client = ClientService.get_client(client_uid, db)
    return client


@router.post("/", response_model=ClientResponse)
def create_client(payload: ClientResponse, db: Session = Depends(get_db)):
    a = ClientService.create_client(payload, db=db)

    return a


@router.patch("/{client_uid}")
def update_client(
    client_uid: UUID, name: str | None, bin: int | None, db: Session = Depends(get_db)
):
    a = ClientService.update_client(client_uid=client_uid, name=name, bin=bin, db=db)

    return a


@router.delete("/{client_uid}")
def delete_client(client_uid: UUID, db: Session = Depends(get_db)):
    a = ClientService.delete_client(db=db, client_uid=client_uid)

    return a
