from fastapi import FastAPI
from routes import configure_routes
import uvicorn

app = FastAPI()
configure_routes(app)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)