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
    conn = Connection(uuid.uuid4(), server, 445)
    conn.connect()
    session = Session(conn, username=username, password=password)
    session.connect()
    tree = TreeConnect(session, fr"\\{server}\{share}")
    tree.connect()
    file_open = Open(tree, relative_path)
    try:
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
        file_open.close()


def read_file_from_server(full_path: str) -> bytes:
    path_parts = full_path.strip("\\").split("\\")
    share = path_parts[1]
    relative_path = "\\".join(path_parts[2:])
    smb_server = _get_secret("dev-fileshare-server")
    smb_username = _get_secret("dev-fileshare-username")
    smb_password = _get_secret("dev-fileshare-password")

    with smb_connection_context(smb_server, smb_username, smb_password, share, relative_path) as file_open:
        file_data = file_open.read(0, file_open.end_of_file)

    return file_data

def upload_to_blob(file_bytes: bytes, blob_path: str):
    azure_storage_connection_string = _get_secret("storageacct-sce1dvstorum001-connstring")
    azure_container_name = _get_secret("fhirlite-container-name")  
    
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
    blob_client = blob_service_client.get_blob_client(container=azure_container_name, blob=blob_path)
    blob_client.upload_blob(file_bytes, overwrite=True)
