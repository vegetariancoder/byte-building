from typing import Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(
        min_length=2,
        max_length=2000,
    )
    document_id: str = Field(
        min_length=1,
        max_length=100,
    )


class ChatSource(BaseModel):
    file_name: str
    page_number: Optional[int]
    paragraph_number: Optional[int]
