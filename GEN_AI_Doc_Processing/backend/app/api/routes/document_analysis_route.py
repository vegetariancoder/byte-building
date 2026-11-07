from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.app.integrations.document_intelligence_client import analyze_layout

document_analysis_router = APIRouter(
    prefix="/documents",
    tags=["Documents"],
)


@document_analysis_router.post("/analyze-layout")
async def analyze_document_layout(
    file: UploadFile = File(...),
):
    filename = file.filename or ""
    extension = filename.rsplit(".", 1)[-1].lower()

    if extension not in {"pdf", "docx"}:
        raise HTTPException(
            status_code=400,
            detail="Only PDF and DOCX files are supported.",
        )

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

    content_type = (
        "application/pdf"
        if extension == "pdf"
        else "application/vnd.openxmlformats-officedocument."
             "wordprocessingml.document"
    )

    try:
        result = analyze_layout(content, content_type)

        return {
            "filename": filename,
            "analysis": result,
        }

    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc