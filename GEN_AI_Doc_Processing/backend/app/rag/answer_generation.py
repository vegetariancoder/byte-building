from openai import AzureOpenAI


def stream_grounded_answer(
    question: str,
    retrieved_chunks: list[dict],
    client: AzureOpenAI,
    deployment_name: str,
):
    if not retrieved_chunks:
        yield (
            "I couldn't find relevant information "
            "in the uploaded assignment."
        )
        return

    context_parts = []

    for index, chunk in enumerate(retrieved_chunks, start=1):
        source = chunk.get("file_name", "Unknown file")

        location = ""
        if chunk.get("page_number") is not None:
            location = f", page {chunk['page_number']}"
        elif chunk.get("paragraph_number") is not None:
            location = f", paragraph {chunk['paragraph_number']}"

        context_parts.append(
            f"[Source {index}: {source}{location}]\n"
            f"{chunk['content']}"
        )

    context = "\n\n".join(context_parts)

    response = client.chat.completions.create(
        model=deployment_name,
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": (
                    f"Assignment context:\n{context}\n\n"
                    f"Student question:\n{question}"
                ),
            },
        ],
        temperature=0,
        max_tokens=700,
        stream=True,
    )

    for event in response:
        if not event.choices:
            continue

        token = event.choices[0].delta.content

        if token:
            yield token
