import os
from io import BytesIO

from azure.ai.documentintelligence import (
    DocumentIntelligenceClient,
)
from azure.core.credentials import AzureKeyCredential

from backend.app.config import load_project_env

load_project_env()


def analyze_layout(content: bytes, content_type: str) -> dict:
    endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_KEY")

    if not endpoint or not key:
        raise ValueError(
            "Document Intelligence configuration is missing."
        )

    client = DocumentIntelligenceClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(key),
    )

    poller = client.begin_analyze_document(
        "prebuilt-layout",
        body=BytesIO(content),
        content_type=content_type,
    )

    result = poller.result()

    pages = []

    for page in result.pages:
        pages.append({
            "page_number": page.page_number,
            "lines": [
                line.content for line in (page.lines or [])
            ],
        })

    tables = []

    for table in result.tables or []:
        tables.append({
            "row_count": table.row_count,
            "column_count": table.column_count,
            "cells": [
                {
                    "row_index": cell.row_index,
                    "column_index": cell.column_index,
                    "content": cell.content,
                }
                for cell in table.cells
            ],
        })

    return {
        "content": result.content or "",
        "page_count": len(pages),
        "pages": pages,
        "tables": tables,
    }