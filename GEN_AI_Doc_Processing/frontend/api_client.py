import httpx

API_BASE_URL = "http://127.0.0.1:8000"


def get_health() -> dict:
    response = httpx.get(
        f"{API_BASE_URL}/health",
        timeout=5.0,
    )
    response.raise_for_status()
    return response.json()
