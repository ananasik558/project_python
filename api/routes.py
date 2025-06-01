from fastapi import APIRouter, UploadFile, File, HTTPException, FastAPI
from pydantic import BaseModel, Field
from typing import Dict
import pandas as pd
import requests
import io
import xml.etree.ElementTree as ET
from etl.save_to_data_lake import save_to_data_lake

def configure_routes(app: FastAPI):
    router = APIRouter()

    class APIUploadRequest(BaseModel):
        url: str
        headers: Dict[str, str] = Field(default_factory=dict)

    @router.post("/upload/api")
    async def upload_api(data: APIUploadRequest):
        """Загрузка данных из внешнего API."""
        try:
            response = requests.get(data.url, headers=data.headers)
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to fetch data from API")
            source_type = "api_data"
            save_to_data_lake(response.json(), source_type, source_type)
            return {"message": "Data uploaded successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/upload/file")
    async def upload_file(file: UploadFile = File(...)):
        """Загрузка данных из файла."""
        try:
            filename = file.filename
            contents = await file.read()

            if filename.endswith(".csv"):
                data = pd.read_csv(io.StringIO(contents.decode("utf-8")))
                source_type = "csv_data"
            elif filename.endswith(".json"):
                data = pd.read_json(io.StringIO(contents.decode("utf-8")))
                source_type = "json_data"
            elif filename.endswith(".xml"):
                root = ET.fromstring(contents)
                data = [{child.tag: child.text for child in item} for item in root]
                data = pd.DataFrame(data)
                source_type = "xml_data"
            else:
                raise HTTPException(status_code=400, detail="Unsupported file type")

            save_to_data_lake(data, source_type, source_type)
            return {"message": "File uploaded successfully"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/status")
    async def status():
        """Проверка статуса системы."""
        return {"status": "OK"}

    app.include_router(router)
