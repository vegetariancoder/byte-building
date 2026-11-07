from fastapi import FastAPI

from backend.app.api.routes.document_analysis_route import document_analysis_router
from backend.app.api.routes.document_extraction_route import document_extraction_router
from backend.app.api.routes.documents_route import documents_router
from backend.app.api.routes.questions_route import questions_router

app = FastAPI(title="Assignment Intelligence Platform")

app.include_router(documents_router)
app.include_router(questions_router)
app.include_router(document_extraction_router)
app.include_router(document_analysis_router)


@app.get("/health")
def health_check():
    return {"status": "healthy"}