import os

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

from backend.app.config import load_project_env

load_project_env()


def get_search_client():
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

    if not endpoint or not admin_key:
        raise ValueError("Azure AI Search configuration is missing.")

    return SearchClient(
        endpoint=endpoint,
        index_name="assignment-chunks",
        credential=AzureKeyCredential(admin_key),
    )