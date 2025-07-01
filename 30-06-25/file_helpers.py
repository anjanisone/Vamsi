import uuid
from contextlib import contextmanager

from azure.storage.blob import BlobServiceClient
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateOptions, ShareAccess, FilePipePrinterAccessMask, CreateDisposition, ImpersonationLevel
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# Setup Key Vault
def _get_secret(secret_name: str) -> str:
    key_vault_url = ""
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    return secret_client.get_secret(secret_name).value


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
            try:
                file_open.close()
            except Exception:
                pass

        if tree:
            try:
                tree.disconnect()
            except Exception:
                pass

        if session:
            try:
                session.disconnect()
            except Exception:
                pass

        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass


def read_file_from_server(full_path: str) -> bytes:
    path_parts = full_path.strip("\\").split("\\")
    share = path_parts[1]
    relative_path = "\\".join(path_parts[2:])
    smb_server = _get_secret("dev-fileshare-server")
    smb_username = _get_secret("dev-fileshare-username")
    smb_password = _get_secret("dev-fileshare-password")

    with smb_connection_context(smb_server, smb_username, smb_password, share, relative_path) as file_open:
        return file_open.read(0, file_open.end_of_file)


@contextmanager
def blob_upload_context(blob_path: str):
    blob_conn_str = _get_secret("storageacct-sce1dvstorum001-connstring")
    container_name = _get_secret("fhirlite-container-name")

    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    yield blob_client 

    # No cleanup is required for BlobClient; it does not hold persistent open connections.


def upload_to_blob(file_bytes: bytes, blob_path: str):
    with blob_upload_context(blob_path) as blob_client:
        blob_client.upload_blob(file_bytes, overwrite=True)
