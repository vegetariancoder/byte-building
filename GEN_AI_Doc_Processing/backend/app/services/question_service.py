from backend.app.schemas.question import QuestionResponse


def prepare_question(question: str) -> QuestionResponse:
    cleaned_question = question.strip()

    return QuestionResponse(
        status="received",
        original_question=question,
        prepared_question=cleaned_question,
    )
