from io import BytesIO

from docx import Document
from pypdf import PdfReader


def extract_pdf(content: bytes) -> dict:
    reader = PdfReader(BytesIO(content))

    pages = []

    for page_number, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""

        pages.append({
            "page_number": page_number,
            "text": text,
        })

    return {
        "file_type": "pdf",
        "page_count": len(pages),
        "pages": pages,
    }


def extract_docx(content: bytes) -> dict:
    document = Document(BytesIO(content))

    paragraphs = []

    for paragraph in document.paragraphs:
        text = paragraph.text.strip()

        if text:
            paragraphs.append(text)

    for table in document.tables:
        for row in table.rows:
            text = " | ".join(
                cell.text.strip()
                for cell in row.cells
                if cell.text.strip()
            )
            if text:
                paragraphs.append(text)

    return {
        "file_type": "docx",
        "paragraph_count": len(paragraphs),
        "paragraphs": paragraphs,
    }


def extract_document(filename: str, content: bytes) -> dict:
    extension = filename.rsplit(".", 1)[-1].lower()

    if extension == "pdf":
        return extract_pdf(content)

    if extension == "docx":
        return extract_docx(content)

    raise ValueError("Unsupported document format.")