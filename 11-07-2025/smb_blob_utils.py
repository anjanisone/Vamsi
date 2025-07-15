import uuid
import os
import re
import hashlib
from contextlib import contextmanager
from azure.storage.blob import BlobServiceClient
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateOptions, ShareAccess, FilePipePrinterAccessMask, CreateDisposition, ImpersonationLevel
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

_secret_cache = {}

def _get_secret(secret_name: str) -> str:
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]
    key_vault_url = os.getenv("AZURE_KEY_VAULT_URL", "")
    if not key_vault_url:
        raise RuntimeError("AZURE_KEY_VAULT_URL environment variable is not set.")
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    value = secret_client.get_secret(secret_name).value
    _secret_cache[secret_name] = value
    return value

def validate_smb_path(full_path: str):
    regex_patterns = [
        os.getenv("SMB_ALLOWED_PATH_REGEX_1", ""),
        os.getenv("SMB_ALLOWED_PATH_REGEX_2", ""),
    ]
    regex_patterns = [pat for pat in regex_patterns if pat.strip()]
    if not regex_patterns:
        raise RuntimeError("No valid SMB path regex patterns found in environment variables.")
    normalized_path = full_path.replace("/", "\\").strip()
    for pattern in regex_patterns:
        if re.match(pattern, normalized_path, re.IGNORECASE):
            return
    raise ValueError(f"Provided path '{full_path}' does not match allowed SMB path patterns.")

@contextmanager
def smb_connection_context(server: str, username: str, password: str, share: str, relative_path: str):
    conn = None
    session = None
    tree = None
    file_open = None
    try:
        conn = Connection(uuid.uuid4(), server, 445)
        conn.connect()
        session = Session(conn, username=username, password=password)
        session.connect()
        tree = TreeConnect(session, fr"\\{server}\{share}")
        tree.connect()
        file_open = Open(tree, relative_path)
        file_open.create(
            impersonation_level=ImpersonationLevel.Impersonation,
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,
            file_attributes=0,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_OPEN,
            create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
        )
        yield file_open
    finally:
        if file_open:
            try: file_open.close()
            except: pass
        if tree:
            try: tree.disconnect()
            except: pass
        if session:
            try: session.disconnect()
            except: pass
        if conn:
            try: conn.disconnect()
            except: pass

@contextmanager
def blob_upload_context(blob_path: str, storage_conn: str, container_name: str):
    blob_service_client = BlobServiceClient.from_connection_string(storage_conn)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    yield blob_client

def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def read_file_from_server_cached(full_path: str, secrets: dict) -> bytes:
    path_parts = full_path.strip("\\").split("\\")
    share = path_parts[1]
    relative_path = "\\".join(path_parts[2:])
    with smb_connection_context(secrets["smb_server"], secrets["smb_username"], secrets["smb_password"], share, relative_path) as file_open:
        return file_open.read(0, file_open.end_of_file)

def upload_to_blob_cached(file_bytes: bytes, blob_path: str, secrets: dict):
    original_hash = compute_sha256(file_bytes)
    with blob_upload_context(blob_path, secrets["storage_conn"], secrets["container"]) as blob_client:
        blob_client.upload_blob(file_bytes, overwrite=True)
        downloaded_blob = blob_client.download_blob().readall()
        downloaded_hash = compute_sha256(downloaded_blob)
    if original_hash != downloaded_hash:
        raise ValueError(f"Hash mismatch: upload failed or corrupted. Original: {original_hash}, Uploaded: {downloaded_hash}")
