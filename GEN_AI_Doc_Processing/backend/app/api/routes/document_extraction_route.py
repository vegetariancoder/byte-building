from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.app.schemas.document_extraction import (
    DocumentExtractionResponse,
)
from backend.app.services.document_extractor import extract_document

document_extraction_router = APIRouter(prefix="/documents", tags=["Documents"])


@document_extraction_router.post(
    "/extract",
    response_model=DocumentExtractionResponse,
)
async def extract_uploaded_document(file: UploadFile = File(...)):
    filename = file.filename or ""

    try:
        content = await file.read(10 * 1024 * 1024 + 1)

        if not content:
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty.",
            )

        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(
                status_code=413,
                detail="File size exceeds the 10 MB limit.",
            )

        result = extract_document(filename, content)

        return {
            "filename": filename,
            "extraction_result": result,
        }

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc