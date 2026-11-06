from typing import Any

from pydantic import BaseModel


class DocumentExtractionResponse(BaseModel):
    filename: str
    extraction_result: dict[str, Any]