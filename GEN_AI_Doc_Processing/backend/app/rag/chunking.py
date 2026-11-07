def split_text(
    text: str,
    max_words: int = 220,
    overlap_words: int = 40,
) -> list[str]:
    words = text.split()

    if not words:
        return []

    chunks = []
    start = 0

    while start < len(words):
        end = min(start + max_words, len(words))

        chunk = " ".join(words[start:end])
        chunks.append(chunk)

        if end == len(words):
            break

        start = end - overlap_words

    return chunks


def chunk_document(extraction_result: dict) -> list[dict]:
    file_type = extraction_result["file_type"]
    blocks = []

    if file_type == "pdf":
        for page in extraction_result["pages"]:
            blocks.append({
                "text": page["text"],
                "page_number": page["page_number"],
            })

    elif file_type == "docx":
        for index, paragraph in enumerate(
            extraction_result["paragraphs"],
            start=1,
        ):
            blocks.append({
                "text": paragraph,
                "paragraph_number": index,
            })

    else:
        raise ValueError("Unsupported document format.")

    chunks = []

    for block in blocks:
        text = block["text"].strip()

        if not text:
            continue

        for part in split_text(text):
            chunks.append({
                "chunk_id": f"chunk-{len(chunks) + 1}",
                "text": part,
                "metadata": {
                    "file_type": file_type,
                    **{
                        key: value
                        for key, value in block.items()
                        if key != "text"
                    },
                },
            })

    return chunks