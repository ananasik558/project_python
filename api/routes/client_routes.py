from fastapi import APIRouter, HTTPException
import pandas as pd
from utils.helpers import load_client_data

router = APIRouter(prefix="/clients", tags=["Clients"])

@router.get("/")
def get_all_clients():
    df = load_client_data()
    return df.to_dict(orient="records")

@router.get("/{client_id}")
def get_client(client_id: str):
    df = load_client_data()
    row = df[df['full_name'] == client_id]

    if row.empty:
        raise HTTPException(status_code=404, detail="Клиент не найден")

    return row.iloc[0].to_dict()
