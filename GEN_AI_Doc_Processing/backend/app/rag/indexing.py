def index_chunks(
    chunks: list[dict],
    search_client,
    document_id: str,
    file_name: str,
) -> int:
    documents = []

    for chunk in chunks:
        metadata = chunk["metadata"]

        document = {
            "chunk_id": f"{document_id}-{chunk['chunk_id']}",
            "document_id": document_id,
            "file_name": file_name,
            "content": chunk["text"],
            "content_vector": chunk["embedding"],
            "file_type": metadata["file_type"],
            "page_number": metadata.get("page_number"),
            "paragraph_number": metadata.get("paragraph_number"),
        }

        documents.append(document)

    if not documents:
        return 0

    results = search_client.upload_documents(documents=documents)

    if not all(result.succeeded for result in results):
        raise RuntimeError("One or more chunks failed to index.")

    return len(results)