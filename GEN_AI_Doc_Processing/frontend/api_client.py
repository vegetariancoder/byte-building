import json
import os

import httpx

API_BASE_URL = os.getenv("BACKEND_API_URL", "http://127.0.0.1:8000")


def _raise_for_status(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        try:
            detail = exc.response.json().get("detail")
        except (AttributeError, ValueError):
            detail = None

        if detail:
            raise RuntimeError(str(detail)) from exc
        raise


def get_health() -> dict:
    response = httpx.get(
        f"{API_BASE_URL}/health",
        timeout=5.0,
    )
    _raise_for_status(response)
    return response.json()


def upload_document(filename: str, content: bytes, content_type: str) -> dict:
    response = httpx.post(
        f"{API_BASE_URL}/documents/upload",
        files={"file": (filename, content, content_type)},
        timeout=300.0,
    )
    _raise_for_status(response)
    return response.json()


def stream_chat_events(
    question: str,
    document_id: str,
):
    with httpx.stream(
        "POST",
        f"{API_BASE_URL}/chat",
        json={
            "question": question,
            "document_id": document_id,
        },
        timeout=120.0,
    ) as response:

        _raise_for_status(response)

        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue

            payload = json.loads(line[6:])
            yield payload
