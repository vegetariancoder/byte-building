from pydantic import BaseModel, Field


class QuestionRequest(BaseModel):
    question: str = Field(
        min_length=5,
        max_length=2000,
        description="Question about an uploaded assignment",
    )


class QuestionResponse(BaseModel):
    status: str
    original_question: str
    prepared_question: str
