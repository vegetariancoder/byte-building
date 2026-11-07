import os
from pathlib import Path
from uuid import uuid4

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from backend.app.config import load_project_env

load_project_env()


def get_filesystem_client():
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    filesystem_name = os.getenv("AZURE_STORAGE_FILESYSTEM")

    if not account_name or not filesystem_name:
        raise ValueError(
            "ADLS configuration is missing. Set AZURE_STORAGE_ACCOUNT_NAME "
            "and AZURE_STORAGE_FILESYSTEM."
        )

    account_url = (
        f"https://{account_name}.dfs.core.windows.net"
    )

    credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(
        account_url=account_url,
        credential=credential,
    )

    return service_client.get_file_system_client(
        file_system=filesystem_name
    )


def upload_file(content: bytes, original_filename: str) -> str:
    filesystem_client = get_filesystem_client()

    extension = Path(original_filename).suffix.lower()
    unique_filename = f"{uuid4().hex}{extension}"

    print(f"Uploading {unique_filename}...")
    file_path = f"documents/{unique_filename}"
    print(f"Uploading {original_filename} to {file_path}")

    directory_client = filesystem_client.get_directory_client("documents")
    try:
        directory_client.create_directory()
    except ResourceExistsError:
        pass

    file_client = filesystem_client.get_file_client(file_path)

    file_client.upload_data(
        content,
        overwrite=True,
    )

    return file_path