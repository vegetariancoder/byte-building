from fastapi import APIRouter

from backend.app.schemas.question import (
    QuestionRequest,
    QuestionResponse,
)
from backend.app.services.question_service import prepare_question

questions_router = APIRouter(prefix="/questions", tags=["Questions"])


@questions_router.post(
    "/prepare",
    response_model=QuestionResponse,
)
def prepare_assignment_question(request: QuestionRequest):
    return prepare_question(request.question)