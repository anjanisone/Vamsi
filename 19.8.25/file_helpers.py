import uuid
import re
import hashlib
import logging
import time
import os
from contextlib import contextmanager
from typing import Generator

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


class UnauthorizedError(Exception):
    """Raised when SMB authentication/authorization fails (map to HTTP 401)."""
    pass


def get_secrets(secret_name: str) -> str:
    """Fetch secret from Azure Key Vault with caching."""
    if secret_name not in _secret_cache:
        key_vault_url = os.getenv("KEY_VAULT_URL")
        if not key_vault_url:
            raise RuntimeError("KEY_VAULT_URL env var not set")

        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=key_vault_url, credential=credential)
        _secret_cache[secret_name] = client.get_secret(secret_name).value
    return _secret_cache[secret_name]


def validate_smb_path(full_path: str) -> None:
    """Validate SMB path by checking actual existence on the SMB server.
    Raises:
      - FileNotFoundError if the file does not exist
      - UnauthorizedError for auth/permission issues
    """
    # Fetch SMB secrets
    server = get_secrets('smb_server')
    username = get_secrets('smb_username')
    password = get_secrets('smb_password')
    # Parse share + relative path from the provided full path
    parts = full_path.strip('\\').split('\\')
    if len(parts) < 2:
        raise ValueError(f"Invalid SMB path format: {full_path}")
    share = parts[1] if parts[0].lower() == server.lower() else parts[1]
    relative_path = '\\'.join(parts[2:]) if len(parts) > 2 else ''
    if not relative_path:
        raise ValueError(f"SMB path missing file component: {full_path}")
    # Connect and try to open the file
    try:
        with smb_connection_context(server, username, password) as session:
            tree = TreeConnect(session, fr"\\{server}\{share}")
            tree.connect()
            try:
                file_open = Open(tree, relative_path)
                file_open.create(
                    impersonation_level=ImpersonationLevel.Impersonation,
                    desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                    file_attributes=0,
                    share_access=ShareAccess.FILE_SHARE_READ,
                    create_disposition=CreateDisposition.FILE_OPEN,
                    create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
                )
            except Exception as e:
                msg = str(e).lower()
                # Map common SMB errors
                if 'object_name_not_found' in msg or 'no such file' in msg or 'not found' in msg:
                    raise FileNotFoundError(f"Path not found: {full_path}")
                if 'access_denied' in msg or 'logon_failure' in msg or 'permission denied' in msg:
                    raise UnauthorizedError(str(e))
                raise
            finally:
                try:
                    file_open.close()
                except Exception:
                    pass
                try:
                    tree.disconnect()
                except Exception:
                    pass
    except UnauthorizedError:
        raise
    except Exception as e:
        # If the underlying exception indicates auth issues, surface as 401
        msg = str(e).lower()
        if 'access_denied' in msg or 'logon_failure' in msg or 'permission denied' in msg:
            raise UnauthorizedError(str(e))
        raise


@contextmanager
def smb_connection_context(server: str, username: str, password: str)-> Generator[Session, None, None]:
    """Context manager for SMB connection + session."""
    conn = Connection(uuid.uuid4(), server, 445)
    conn.connect()
    session = Session(conn, username=username, password=password)
    session.connect()
    try:
        yield session
    finally:
        try:
            session.disconnect()
        finally:
            conn.disconnect()


def stream_file_to_blob(
    smb_path: str,
    blob_path: str,
    secrets: dict,
    blob_overwrite: bool = True,
    chunk_size: int = 1024 * 1024
) -> dict:
    """Stream a file from SMB directly to Azure Blob Storage in chunks with hashing."""
    server = secrets["smb_server"]
    username = secrets["smb_username"]
    password = secrets["smb_password"]
    connection_string = secrets["storage_conn"]
    container_name = secrets["container"]

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)

    try:
        path_parts = smb_path.strip('\\').split('\\')
        share = path_parts[1]
        relative_path = '\\'.join(path_parts[2:])
        
        with smb_connection_context(server, username, password) as (session):
            tree = TreeConnect(session, fr"\\{server}\{share}")
            tree.connect()
            
            try:
                file_open = Open(tree, relative_path)
                file_open.create(
                    impersonation_level=ImpersonationLevel.Impersonation,
                    desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                    file_attributes=0,
                    share_access=ShareAccess.FILE_SHARE_READ,
                    create_disposition=CreateDisposition.FILE_OPEN,
                    create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
                )

                try:
                    file_size = file_open.end_of_file
                    sha256_hash = hashlib.sha256()

                    def file_generator():
                        offset = 0
                        while offset < file_size:
                            chunk = file_open.read(offset=offset, length=min(chunk_size, file_size - offset))
                            sha256_hash.update(chunk)
                            offset += len(chunk)
                            yield chunk

                    # Upload stream
                    blob_client.upload_blob(
                        data=file_generator(),
                        overwrite=blob_overwrite,
                    )

                    return {
                        "status": "Success",
                        "status_code": 200,
                        "hash": sha256_hash.hexdigest(),
                        "source_path": smb_path,
                        "blob_path": blob_path,
                        "total_bytes": file_size
                       
                    }

                finally:
                    file_open.close()
                    
            finally:
                tree.disconnect()

    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise RuntimeError(f"Failed to stream file {smb_path}: {str(e)}")
