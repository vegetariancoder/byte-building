import os

from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswAlgorithmConfiguration,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)

from backend.app.config import load_project_env

load_project_env()


def create_search_index():
    endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
    admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")

    if not endpoint or not admin_key:
        raise ValueError("Azure AI Search configuration is missing.")

    from azure.core.credentials import AzureKeyCredential

    client = SearchIndexClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(admin_key),
    )

    fields = [
        SimpleField(
            name="chunk_id",
            type=SearchFieldDataType.String,
            key=True,
        ),
        SimpleField(
            name="document_id",
            type=SearchFieldDataType.String,
            filterable=True,
        ),
        SearchableField(
            name="file_name",
            type=SearchFieldDataType.String,
            filterable=True,
        ),
        SearchableField(
            name="content",
            type=SearchFieldDataType.String,
        ),
        SimpleField(
            name="file_type",
            type=SearchFieldDataType.String,
            filterable=True,
        ),
        SimpleField(
            name="page_number",
            type=SearchFieldDataType.Int32,
            filterable=True,
        ),
        SimpleField(
            name="paragraph_number",
            type=SearchFieldDataType.Int32,
            filterable=True,
        ),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(
                SearchFieldDataType.Single
            ),
            searchable=True,
            vector_search_dimensions=1536,
            vector_search_profile_name="assignment-vector-profile",
        ),
    ]

    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="assignment-hnsw"
            )
        ],
        profiles=[
            VectorSearchProfile(
                name="assignment-vector-profile",
                algorithm_configuration_name="assignment-hnsw",
            )
        ],
    )

    index = SearchIndex(
        name="assignment-chunks",
        fields=fields,
        vector_search=vector_search,
    )

    return client.create_or_update_index(index)