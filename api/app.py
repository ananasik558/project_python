from fastapi import FastAPI
from routes.client_routes import router as client_router
from routes.scoring_routes import router as scoring_router

app = FastAPI(
    title="Bank Data API",
    description="Центр данных для банковских интеллектуальных сервисов",
    version="1.0"
)

# Регистрация маршрутов
app.include_router(client_router)
app.include_router(scoring_router)
