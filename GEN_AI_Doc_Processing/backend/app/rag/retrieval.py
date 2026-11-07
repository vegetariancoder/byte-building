from azure.search.documents.models import VectorizedQuery


def search_chunks(
    query: str,
    query_embedding: list[float],
    search_client,
    top_k: int = 5,
    document_id: str = None,
) -> list[dict]:

    vector_query = VectorizedQuery(
        vector=query_embedding,
        k_nearest_neighbors=top_k,
        fields="content_vector",
    )

    search_filter = None

    if document_id:
        safe_document_id = document_id.replace("'", "''")
        search_filter = f"document_id eq '{safe_document_id}'"

    results = search_client.search(
        search_text=query,
        vector_queries=[vector_query],
        select=[
            "chunk_id",
            "document_id",
            "file_name",
            "content",
            "file_type",
            "page_number",
            "paragraph_number",
        ],
        filter=search_filter,
        top=top_k,
    )

    return [
        {
            "chunk_id": result["chunk_id"],
            "document_id": result["document_id"],
            "file_name": result["file_name"],
            "content": result["content"],
            "score": result.get("@search.score"),
            "page_number": result.get("page_number"),
            "paragraph_number": result.get("paragraph_number"),
        }
        for result in results
    ]