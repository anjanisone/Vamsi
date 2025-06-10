import os
import io
import json
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateOptions, ShareAccess, FilePipePrinterAccessMask, CreateDisposition
from smbprotocol.structure import ImpersonationLevel

# Set from environment variables or hardcode for local testing
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "auth-files")

# SMB credentials
SMB_SERVER = "fsdevca1.headquarters.newcenturyhealth.com"
SMB_USERNAME = os.getenv("SMB_USERNAME", "DOMAIN\\yourusername")
SMB_PASSWORD = os.getenv("SMB_PASSWORD", "yourpassword")


def read_file_from_smb(full_path: str) -> bytes:
    # Extract share and relative path
    path_parts = full_path.strip("\\").split("\\")
    share = path_parts[1]
    relative_path = "\\".join(path_parts[2:])

    conn = Connection(uuid="", server=SMB_SERVER, port=445)
    conn.connect()
    session = Session(conn, username=SMB_USERNAME, password=SMB_PASSWORD)
    session.connect()
    tree = TreeConnect(session, fr"\\{SMB_SERVER}\{share}")
    tree.connect()

    file_open = Open(tree, relative_path,
                     access=FilePipePrinterAccessMask.GENERIC_READ,
                     share=ShareAccess.FILE_SHARE_READ,
                     disposition=CreateDisposition.FILE_OPEN,
                     options=CreateOptions.FILE_NON_DIRECTORY_FILE)
    file_open.create(ImpersonationLevel.Impersonation)
    file_data = file_open.read(0, file_open.query_info().end_of_file)
    file_open.close()
    return file_data


def upload_to_blob(file_bytes: bytes, blob_path: str):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_path)
    blob_client.upload_blob(file_bytes, overwrite=True)


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        file_path = req_body.get("file_path")
        auth_id = req_body.get("carepro_AuthrequestId")
        annotation_id = req_body.get("carepro_DocumentBase_Annotation_Id")

        if not all([file_path, auth_id, annotation_id]):
            return func.HttpResponse(
                json.dumps({"status": "Error", "message": "Missing required fields."}),
                status_code=400,
                mimetype="application/json"
            )

        file_name = os.path.basename(file_path)
        blob_path = f"{auth_id}/{file_name}"

        file_data = read_file_from_smb(file_path)
        upload_to_blob(file_data, blob_path)

        response = {
            "status": "Success",
            "status_code": 200,
            "auth_request_id": auth_id,
            "annotation_id": annotation_id,
            "file_name": file_name,
            "source_file_path": file_path,
            "azure_blob_path": f"{AZURE_CONTAINER_NAME}/{blob_path}"
        }
        return func.HttpResponse(json.dumps(response), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception("Error in file copy function")
        return func.HttpResponse(
            json.dumps({"status": "Failed", "status_code": 500, "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
