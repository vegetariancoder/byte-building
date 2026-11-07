from typing import Optional

from pydantic import BaseModel


class SourceReference(BaseModel):
    file_name: str
    page_number: Optional[int]
    paragraph_number: Optional[int]


class GroundedAnswer(BaseModel):
    answer: str
    sources: list[SourceReference]
    grounded: bool