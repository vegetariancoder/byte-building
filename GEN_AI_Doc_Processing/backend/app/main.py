from fastapi import FastAPI

from backend.app.api.routes.documents_route import documents_router
from backend.app.api.routes.questions_route import questions_router

app = FastAPI(title="Assignment Intelligence Platform")

app.include_router(documents_router)
app.include_router(questions_router)


@app.get("/health")
def health_check():
    return {"status": "healthy"}