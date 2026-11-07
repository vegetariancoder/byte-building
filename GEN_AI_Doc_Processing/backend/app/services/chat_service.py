
import os

from backend.app.integrations.ai_search import get_search_client
from backend.app.rag.embeddings import (
    generate_embeddings,
    get_embedding_client,
)
from backend.app.rag.retrieval import search_chunks
from backend.app.rag.answer_generation import stream_grounded_answer


def stream_chat(
    question: str,
    document_id: str,
):
    client = get_embedding_client()
    search_client = get_search_client()

    # 1. Convert the question into an embedding.
    query_embedding = generate_embeddings(
        [question],
        client,
        os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"],
    )[0]

    # 2. Retrieve chunks from the selected document only.
    retrieved_chunks = search_chunks(
        query=question,
        query_embedding=query_embedding,
        search_client=search_client,
        top_k=5,
        document_id=document_id,
    )

    # 3. Return a fallback if no relevant content is found.
    if not retrieved_chunks:
        yield {
            "type": "answer",
            "text": (
                "I couldn't find relevant information "
                "in the uploaded assignment."
            ),
        }
        yield {
            "type": "sources",
            "items": [],
        }
        return

    # 4. Stream the generated answer.
    for token in stream_grounded_answer(
        question=question,
        retrieved_chunks=retrieved_chunks,
        client=client,
        deployment_name=os.environ[
            "AZURE_OPENAI_CHAT_DEPLOYMENT"
        ],
    ):
        yield {
            "type": "token",
            "text": token,
        }

    # 5. Send actual source metadata separately.
    sources = []
    seen = set()

    for chunk in retrieved_chunks:
        source = {
            "file_name": chunk.get("file_name", "Unknown file"),
            "page_number": chunk.get("page_number"),
            "paragraph_number": chunk.get("paragraph_number"),
        }

        key = (
            source["file_name"],
            source["page_number"],
            source["paragraph_number"],
        )

        if key not in seen:
            sources.append(source)
            seen.add(key)

    yield {
        "type": "sources",
        "items": sources,
    }
