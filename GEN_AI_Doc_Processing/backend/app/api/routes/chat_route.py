
import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.app.schemas.chat import ChatRequest
from backend.app.services.chat_service import stream_chat

chat_router = APIRouter(prefix="/chat", tags=["Chat"])


@chat_router.post("")
def chat(request: ChatRequest):

    def event_stream():
        try:
            for event in stream_chat(
                question=request.question,
                document_id=request.document_id,
            ):
                yield f"data: {json.dumps(event)}\n\n"

            yield 'data: {"type":"done"}\n\n'

        except Exception:
            # Log the actual exception server-side in production.
            yield (
                'data: {"type":"error",'
                '"message":"Unable to process your question."}\n\n'
            )

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
