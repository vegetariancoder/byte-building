
from fastapi import FastAPI

from backend.app.schemas.question import QuestionRequest, QuestionResponse
from backend.app.services.question_service import prepare_question

app = FastAPI(title="Assignment Intelligence Platform")


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/questions/prepare", response_model=QuestionResponse)
def prepare_assignment_question(question: QuestionRequest):
    return prepare_question(question.question)
