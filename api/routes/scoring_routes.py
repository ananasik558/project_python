# api/routes/scoring_routes.py

from fastapi import APIRouter, HTTPException
from utils.scoring_client import send_to_scoring_assistant

router = APIRouter(prefix="/scoring", tags=["Scoring"])

@router.post("/request_score/{client_id}")
def request_score(client_id: str):
    from utils.helpers import load_client_data
    df = load_client_data()
    row = df[df['full_name'] == client_id]

    if row.empty:
        raise HTTPException(status_code=404, detail="Клиент не найден")

    result = send_to_scoring_assistant(row.iloc[0].to_dict())
    return {"client_id": client_id, "score_result": result}
