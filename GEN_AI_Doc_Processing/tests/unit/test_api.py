from io import BytesIO

import pytest
from docx import Document
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.document_extractor import extract_document

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_prepare_question_success():
    response = client.post(
        "/questions/prepare",
        json={"question": "Explain Python inheritance"},
    )

    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "received"
    assert data["original_question"] == "Explain Python inheritance"
    assert data["prepared_question"] == "Explain Python inheritance"


def test_prepare_question_trims_whitespace():
    response = client.post(
        "/questions/prepare",
        json={"question": "   Explain Python inheritance   "},
    )

    assert response.status_code == 200
    assert response.json()["prepared_question"] == (
        "Explain Python inheritance"
    )


def test_prepare_question_rejects_short_question():
    response = client.post(
        "/questions/prepare",
        json={"question": "Hi"},
    )

    assert response.status_code == 422


def test_prepare_question_rejects_missing_question():
    response = client.post(
        "/questions/prepare",
        json={},
    )

    assert response.status_code == 422


def test_upload_document_success():
    response = client.post(
        "/documents/upload",
        files={"file": ("assignment.pdf", b"sample PDF content", "application/pdf")},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "received",
        "filename": "assignment.pdf",
        "content_type": "application/pdf",
        "file_size": len(b"sample PDF content"),
        "message": "Document uploaded successfully.",
    }


def test_upload_document_rejects_unsupported_file_type():
    response = client.post(
        "/documents/upload",
        files={"file": ("assignment.txt", b"plain text", "text/plain")},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Only PDF and DOCX files are supported."
    }

def test_extract_docx():
    document = Document()
    document.add_paragraph("Python is a programming language.")
    document.add_paragraph("It supports object-oriented programming.")

    buffer = BytesIO()
    document.save(buffer)

    result = extract_document("assignment.docx", buffer.getvalue())

    assert result["file_type"] == "docx"
    assert result["paragraph_count"] == 2
    assert "Python is a programming language." in result["paragraphs"]

def test_unsupported_file():
    with pytest.raises(ValueError):
        extract_document("assignment.txt", b"Some text")
