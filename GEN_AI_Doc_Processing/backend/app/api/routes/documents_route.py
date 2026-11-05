from fastapi import APIRouter, File, UploadFile

from backend.app.schemas.document import DocumentUploadResponse
from backend.app.services.document_service import process_document

documents_router = APIRouter(prefix="/documents", tags=["Documents"])


@documents_router.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(file: UploadFile = File(...)):
    result = await process_document(file)
    return result