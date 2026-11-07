from pydantic import BaseModel


class DocumentUploadResponse(BaseModel):
    status: str
    document_id: str
    filename: str
    content_type: str
    file_size: int
    storage_path: str
    message: str