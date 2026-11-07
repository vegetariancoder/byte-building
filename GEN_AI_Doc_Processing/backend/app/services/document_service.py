import os
from uuid import uuid4

from fastapi import HTTPException, UploadFile

from backend.app.integrations.adls_client import upload_file
from backend.app.integrations.ai_search import get_search_client
from backend.app.integrations.document_intelligence_client import analyze_layout
from backend.app.rag.chunking import chunk_document
from backend.app.rag.embeddings import embed_chunks, get_embedding_client
from backend.app.rag.indexing import index_chunks

ALLOWED_EXTENSIONS = {".pdf", ".docx"}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


def _extract_with_document_intelligence(
    filename: str,
    content: bytes,
) -> dict:
    extension = "." + filename.rsplit(".", 1)[-1].lower()
    content_type = (
        "application/pdf"
        if extension == ".pdf"
        else "application/vnd.openxmlformats-officedocument."
        "wordprocessingml.document"
    )
    analysis = analyze_layout(content, content_type)

    if extension == ".pdf":
        return {
            "file_type": "pdf",
            "pages": [
                {
                    "page_number": page["page_number"],
                    "text": "\n".join(page["lines"]),
                }
                for page in analysis["pages"]
            ],
        }

    paragraphs = [
        line.strip()
        for line in analysis["content"].splitlines()
        if line.strip()
    ]

    for table in analysis["tables"]:
        rows = {}
        for cell in table["cells"]:
            rows.setdefault(cell["row_index"], []).append(
                (cell["column_index"], cell["content"])
            )
        paragraphs.extend(
            " | ".join(value for _, value in sorted(cells))
            for _, cells in sorted(rows.items())
        )

    return {
        "file_type": "docx",
        "paragraphs": paragraphs,
    }


async def process_document(file: UploadFile) -> dict:
    filename = file.filename or ""

    extension = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="Only PDF and DOCX files are supported.",
        )

    content = await file.read(MAX_FILE_SIZE + 1)

    if not content:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is empty.",
        )

    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail="File size exceeds the 10 MB limit.",
        )

    storage_path = upload_file(content, filename)
    extraction_result = _extract_with_document_intelligence(
        filename,
        content,
    )
    chunks = chunk_document(extraction_result)

    if not chunks:
        raise HTTPException(
            status_code=422,
            detail=(
                "No readable text was returned by Document Intelligence "
                "for this document."
            ),
        )

    embedding_deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")

    if not embedding_deployment:
        raise ValueError(
            "Azure OpenAI embedding deployment is not configured."
        )

    document_id = uuid4().hex
    embedding_client = get_embedding_client()
    embedded_chunks = embed_chunks(
        chunks,
        embedding_client,
        embedding_deployment,
    )
    indexed_count = index_chunks(
        embedded_chunks,
        get_search_client(),
        document_id,
        filename,
    )

    if indexed_count != len(chunks):
        raise RuntimeError("Not all document chunks were indexed.")

    return {
        "status": "received",
        "document_id": document_id,
        "filename": filename,
        "content_type": file.content_type or "unknown",
        "file_size": len(content),
        "storage_path": storage_path,
        "message": "Document uploaded and indexed successfully.",
    }