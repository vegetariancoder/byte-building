
import os
from typing import Union

from openai import AzureOpenAI, OpenAI

from backend.app.config import load_project_env

load_project_env()


def get_embedding_client() -> Union[AzureOpenAI, OpenAI]:
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION")

    if not endpoint or not api_key:
        raise ValueError("Azure OpenAI configuration is missing.")

    if endpoint.rstrip("/").endswith("/openai/v1"):
        return OpenAI(
            base_url=f"{endpoint.rstrip('/')}/",
            api_key=api_key,
        )

    if not api_version:
        raise ValueError("Azure OpenAI API version is missing.")

    return AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
    )


def generate_embeddings(
    texts: list[str],
    client: AzureOpenAI,
    deployment_name: str,
) -> list[list[float]]:

    if not texts:
        return []

    response = client.embeddings.create(
        model=deployment_name,
        input=texts,
    )

    sorted_results = sorted(
        response.data,
        key=lambda item: item.index,
    )

    return [item.embedding for item in sorted_results]


def embed_chunks(
    chunks: list[dict],
    client: AzureOpenAI,
    deployment_name: str,
) -> list[dict]:

    if not chunks:
        return []

    texts = [chunk["text"] for chunk in chunks]

    embeddings = generate_embeddings(
        texts,
        client,
        deployment_name,
    )

    if len(embeddings) != len(chunks):
        raise ValueError("Embedding count does not match chunk count.")

    return [
        {
            **chunk,
            "embedding": embedding,
        }
        for chunk, embedding in zip(chunks, embeddings)
    ]
