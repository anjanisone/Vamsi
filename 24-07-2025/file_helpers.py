import uuid
import re
import hashlib
import logging
import time
from contextlib import contextmanager

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import (
    Open,
    CreateOptions,
    ShareAccess,
    FilePipePrinterAccessMask,
    CreateDisposition,
    ImpersonationLevel,
)

logger = logging.getLogger(__name__)
_secret_cache = {}


def get_secrets(secret_name: str) -> str:
    """Fetch secret from Azure Key Vault with caching."""
    if secret_name not in _secret_cache:
        key_vault_url = "https://dv-kv-npsc-01.vault.azure.net/"
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=key_vault_url, credential=credential)
        _secret_cache[secret_name] = client.get_secret(secret_name).value
    return _secret_cache[secret_name]


def validate_smb_path(path: str) -> None:
    """Ensure SMB path follows UNC format."""
    if not re.match(r"^\\\\[^\\]+(\\[^\\]+)+$", path):
        raise ValueError(f"Invalid SMB file path: {path}")


@contextmanager
def smb_connection_context(server: str, username: str, password: str):
    """Context manager for SMB connection + session."""
    conn = Connection(uuid=str(uuid.uuid4()), server=server, port=445)
    conn.connect(timeout=10)
    session = Session(conn, username=username, password=password)
    session.connect()
    try:
        yield conn, session
    finally:
        session.disconnect(True)
        conn.disconnect(True)


def stream_file_to_blob(
    smb_path: str,
    container_name: str,
    blob_path: str,
    connection_string: str,
    chunk_size: int = 4 * 1024 * 1024,
) -> dict:
    """
    Streams file from SMB to Azure Blob using generator,
    with inline SHA256 checksum and post-upload size/hash validation.
    Deletes the blob if validation fails.
    """
    logger.info(f"Streaming {smb_path} → {container_name}/{blob_path}")
    start_time = time.time()

    match = re.match(r"^\\\\([^\\]+)\\([^\\]+)\\(.+)$", smb_path)
    if not match:
        raise ValueError("Invalid SMB path format.")

    server, share, relative_path = match.groups()
    username = get_secrets("sftp-username")
    password = get_secrets("sftp-password")

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)

    try:
        with smb_connection_context(server, username, password) as (_, session):
            tree = TreeConnect(session, fr"\\{server}\{share}")
            tree.connect()

            file_open = Open(
                tree,
                relative_path,
                access=FilePipePrinterAccessMask.GENERIC_READ,
                options=CreateOptions.FILE_NON_DIRECTORY_FILE,
                share=ShareAccess.FILE_SHARE_READ,
                disposition=CreateDisposition.FILE_OPEN,
                impersonation_level=ImpersonationLevel.Impersonation,
            )
            file_open.create()

            file_size = file_open.end_of_file
            sha256_hash = hashlib.sha256()

            def file_generator():
                offset = 0
                while offset < file_size:
                    chunk = file_open.read(offset=offset, length=min(chunk_size, file_size - offset))
                    sha256_hash.update(chunk)
                    offset += len(chunk)
                    yield chunk

            blob_client.upload_blob(
                data=file_generator(),
                overwrite=True,
                timeout=600,
                max_concurrency=4,
            )

            file_open.close()

            downloaded_blob = blob_client.download_blob().readall()
            downloaded_size = len(downloaded_blob)
            downloaded_hash = hashlib.sha256(downloaded_blob).hexdigest()
            original_hash = sha256_hash.hexdigest()

            if downloaded_size != file_size or downloaded_hash != original_hash:
                logger.warning("Upload validation failed. Deleting blob.")
                blob_client.delete_blob()
                return {
                    "success": False,
                    "error": "Upload failed validation. Blob deleted.",
                    "expected_size": file_size,
                    "actual_size": downloaded_size,
                    "expected_hash": original_hash,
                    "actual_hash": downloaded_hash,
                }

            duration = time.time() - start_time
            return {
                "success": True,
                "source_path": smb_path,
                "blob_path": blob_path,
                "total_bytes": file_size,
                "processing_time_seconds": round(duration, 3),
                "throughput_mbps": round((file_size / 1024 / 1024) / duration, 2) if duration > 0 else 0,
                "sha256": original_hash,
            }

    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise RuntimeError(f"Failed to stream file {smb_path}: {str(e)}")
