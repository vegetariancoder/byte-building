
from fastapi import FastAPI

app = FastAPI(title="Assignment Intelligence Platform")


@app.get("/health")
def health_check():
    return {"status": "healthy"}
