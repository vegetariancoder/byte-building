from io import BytesIO
from types import SimpleNamespace

import pytest
from docx import Document
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.rag.answer_generation import stream_grounded_answer
from backend.app.rag.chunking import chunk_document, split_text
from backend.app.services.chat_service import stream_chat
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


def test_upload_document_success(monkeypatch):
    indexed_document_ids = []
    pipeline_steps = []
    monkeypatch.setenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "test-embedding")

    def upload_to_storage(content, filename):
        pipeline_steps.append("storage")
        return "documents/test.pdf"

    def analyze_with_document_intelligence(content, content_type):
        pipeline_steps.append("document_intelligence")
        assert content_type == "application/pdf"
        return {
            "content": "Test document content.",
            "pages": [
                {"page_number": 1, "lines": ["Test document content."]}
            ],
            "tables": [],
        }

    monkeypatch.setattr(
        "backend.app.services.document_service.upload_file",
        upload_to_storage,
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.analyze_layout",
        analyze_with_document_intelligence,
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.chunk_document",
        lambda extraction: [
            {
                "chunk_id": "chunk-1",
                "text": "Test document content.",
                "metadata": {"file_type": "pdf", "page_number": 1},
            }
        ],
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.get_embedding_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.embed_chunks",
        lambda chunks, client, deployment: (
            pipeline_steps.append("embeddings") or chunks
        ),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.get_search_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.index_chunks",
        lambda chunks, search_client, document_id, filename: (
            pipeline_steps.append("search_index")
            or indexed_document_ids.append(document_id)
            or len(chunks)
        ),
    )
    response = client.post(
        "/documents/upload",
        files={"file": ("assignment.pdf", b"sample PDF content", "application/pdf")},
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data["document_id"]) == 32
    assert indexed_document_ids == [data["document_id"]]
    assert data["status"] == "received"
    assert data["filename"] == "assignment.pdf"
    assert data["content_type"] == "application/pdf"
    assert data["file_size"] == len(b"sample PDF content")
    assert data["storage_path"] == "documents/test.pdf"
    assert data["message"] == "Document uploaded and indexed successfully."
    assert pipeline_steps == [
        "storage",
        "document_intelligence",
        "embeddings",
        "search_index",
    ]


def test_upload_document_rejects_documents_without_readable_text(
    monkeypatch,
):
    monkeypatch.setattr(
        "backend.app.services.document_service.upload_file",
        lambda content, filename: "documents/empty.pdf",
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.analyze_layout",
        lambda content, content_type: {
            "content": "",
            "pages": [{"page_number": 1, "lines": []}],
            "tables": [],
        },
    )

    response = client.post(
        "/documents/upload",
        files={"file": ("scanned.pdf", b"pdf content", "application/pdf")},
    )

    assert response.status_code == 422
    assert "No readable text" in response.json()["detail"]


def test_upload_docx_uses_document_intelligence(monkeypatch):
    observed_content_types = []

    monkeypatch.setattr(
        "backend.app.services.document_service.analyze_layout",
        lambda content, content_type: (
            observed_content_types.append(content_type)
            or {
                "content": "Document Intelligence extracted content.",
                "pages": [],
                "tables": [
                    {
                        "cells": [
                            {
                                "row_index": 0,
                                "column_index": 0,
                                "content": "Table value",
                            }
                        ]
                    }
                ],
            }
        ),
    )
    monkeypatch.setenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "test-embedding")
    monkeypatch.setattr(
        "backend.app.services.document_service.get_embedding_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.embed_chunks",
        lambda chunks, client, deployment: chunks,
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.get_search_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.index_chunks",
        lambda chunks, search_client, document_id, filename: len(chunks),
    )
    monkeypatch.setattr(
        "backend.app.services.document_service.upload_file",
        lambda content, filename: "documents/test.docx",
    )

    response = client.post(
        "/documents/upload",
        files={
            "file": (
                "test.docx",
                b"docx content",
                "application/vnd.openxmlformats-officedocument."
                "wordprocessingml.document",
            )
        },
    )

    assert response.status_code == 200
    assert observed_content_types == [
        "application/vnd.openxmlformats-officedocument."
        "wordprocessingml.document"
    ]
    assert response.json()["message"] == (
        "Document uploaded and indexed successfully."
    )


def test_chat_searches_only_the_selected_document(monkeypatch):
    selected_ids = []
    retrieved = [
        {
            "chunk_id": "doc-1-chunk-1",
            "document_id": "doc-1",
            "file_name": "assignment.docx",
            "content": "The answer is in this document.",
            "page_number": None,
            "paragraph_number": 1,
        }
    ]
    monkeypatch.setenv("AZURE_OPENAI_CHAT_DEPLOYMENT", "test-chat")
    monkeypatch.setattr(
        "backend.app.services.chat_service.get_embedding_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.chat_service.get_search_client",
        lambda: object(),
    )
    monkeypatch.setattr(
        "backend.app.services.chat_service.generate_embeddings",
        lambda questions, client, deployment: [[0.1, 0.2]],
    )

    def search_chunks_for_document(**kwargs):
        selected_ids.append(kwargs["document_id"])
        return retrieved

    monkeypatch.setattr(
        "backend.app.services.chat_service.search_chunks",
        search_chunks_for_document,
    )
    monkeypatch.setattr(
        "backend.app.services.chat_service.stream_grounded_answer",
        lambda **kwargs: iter(["The answer is in this document."]),
    )

    events = list(stream_chat("What is the answer?", "doc-1"))

    assert selected_ids == ["doc-1"]
    assert events[0] == {
        "type": "token",
        "text": "The answer is in this document.",
    }
    assert events[1] == {
        "type": "sources",
        "items": [
            {
                "file_name": "assignment.docx",
                "page_number": None,
                "paragraph_number": 1,
            }
        ],
    }


def test_grounded_answer_includes_context_and_defined_system_prompt():
    captured = {}
    event = SimpleNamespace(
        choices=[
            SimpleNamespace(delta=SimpleNamespace(content="Grounded answer"))
        ]
    )

    class FakeClient:
        def __init__(self):
            self.chat = self
            self.completions = self

        def create(self, **kwargs):
            captured.update(kwargs)
            return [event]

    answer = list(
        stream_grounded_answer(
            question="What is stated?",
            retrieved_chunks=[
                {
                    "file_name": "assignment.pdf",
                    "page_number": 2,
                    "content": "The document context.",
                }
            ],
            client=FakeClient(),
            deployment_name="test-chat",
        )
    )

    assert answer == ["Grounded answer"]
    assert "only the supplied document context" in captured["messages"][0][
        "content"
    ]
    assert "The document context." in captured["messages"][1]["content"]
    assert captured["max_completion_tokens"] == 700
    assert "max_tokens" not in captured
    assert "temperature" not in captured


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
    table = document.add_table(rows=1, cols=2)
    table.cell(0, 0).text = "Language"
    table.cell(0, 1).text = "Python"

    buffer = BytesIO()
    document.save(buffer)

    result = extract_document("assignment.docx", buffer.getvalue())

    assert result["file_type"] == "docx"
    assert result["paragraph_count"] == 3
    assert "Python is a programming language." in result["paragraphs"]
    assert "Language | Python" in result["paragraphs"]

def test_unsupported_file():
    with pytest.raises(ValueError):
        extract_document("assignment.txt", b"Some text")


def test_split_text_creates_multiple_chunks():
    text = " ".join(f"word{i}" for i in range(500))

    chunks = split_text(text)

    assert len(chunks) == 3


def test_split_text_handles_empty_text():
    assert split_text("") == []


def test_chunk_pdf_preserves_page_metadata():
    extraction_result = {
        "file_type": "pdf",
        "page_count": 1,
        "pages": [
            {
                "page_number": 1,
                "text": "Explain Python inheritance.",
            }
        ],
    }

    chunks = chunk_document(extraction_result)

    assert len(chunks) == 1
    assert chunks[0]["metadata"]["page_number"] == 1
    assert "Python inheritance" in chunks[0]["text"]


def test_chunk_docx_preserves_paragraph_metadata():
    extraction_result = {
        "file_type": "docx",
        "paragraph_count": 2,
        "paragraphs": [
            "Python supports inheritance.",
            "Python supports polymorphism.",
        ],
    }

    chunks = chunk_document(extraction_result)

    assert len(chunks) == 2
    assert chunks[0]["metadata"]["paragraph_number"] == 1
    assert chunks[1]["metadata"]["paragraph_number"] == 2


def test_chunk_document_rejects_unsupported_type():
    import pytest

    with pytest.raises(ValueError):
        chunk_document({"file_type": "txt"})
