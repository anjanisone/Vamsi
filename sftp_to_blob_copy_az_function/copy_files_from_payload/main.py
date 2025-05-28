from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateOptions, ShareAccess, FilePipePrinterAccessMask
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import os
import csv
import io

def copy_files_batch(payload: list):
    server = "fsdevca1.headquarters.newcenturyhealth.com"
    username = "DOMAIN\\yourusername"
    password = "yourpassword"

    # SMB connection and session
    conn = Connection(uuid="", server=server, port=445)
    conn.connect()
    session = Session(conn, username=username, password=password)
    session.connect()

    # Pre-connect to both shares
    tree_qa = TreeConnect(session, fr"\\{server}\QAFileShare")
    tree_qa.connect()

    tree_qa1 = TreeConnect(session, fr"\\{server}\QAFileShare1")
    tree_qa1.connect()

    # Azure Blob connection
    blob_service_client = BlobServiceClient.from_connection_string("<AZURE_STORAGE_CONNECTION_STRING>")
    container_client = blob_service_client.get_container_client("your-container")

    log_rows = []
    error_rows = []
    timestamp_now = datetime.utcnow().isoformat()

    for item in payload:
        path = item.get("full_path")
        auth_id = item.get("auth_request_id")
        annotation_id = item.get("annotation_id")

        try:
            if "\\QAFileShare1\\" in path:
                tree = tree_qa1
                relative_path = path.split("QAFileShare1\\", 1)[1]
            elif "\\QAFileShare\\" in path:
                tree = tree_qa
                relative_path = path.split("QAFileShare\\", 1)[1]
            else:
                print(f"Unknown share in path: {path}")
                continue

            file_open = Open(tree, relative_path, access=FilePipePrinterAccessMask.GENERIC_READ,
                             options=CreateOptions.FILE_NON_DIRECTORY_FILE,
                             share=ShareAccess.FILE_SHARE_READ)
            file_open.create()
            data = file_open.read(0, file_open.query_info().end_of_file)
            file_open.close()

            blob_name = os.path.basename(path)
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)

            log_rows.append([blob_name, path, auth_id, annotation_id, timestamp_now])
        except Exception as e:
            blob_name = os.path.basename(path)
            error_rows.append([blob_name, path, auth_id, annotation_id, str(e)])
            print(f"Error copying {path}: {e}")

    # Upload audit log
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['file_name', 'full_path', 'auth_request_id', 'annotation_id', 'uploaded_at'])
    writer.writerows(log_rows)

    audit_blob_name = f"audit-logs/uploaded_{timestamp_now.replace(':', '_')}.csv"
    container_client.upload_blob(audit_blob_name, output.getvalue(), overwrite=True)

    # Clean up
    tree_qa.disconnect()
    tree_qa1.disconnect()
    session.disconnect()
    conn.disconnect()