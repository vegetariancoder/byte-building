from fastapi.testclient import TestClient

from backend.app.main import app

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
