import os
import json
import logging
import azure.functions as func

from file_helpers import validate_smb_path, get_secrets, stream_file_to_blob

# Configure logging
logger = logging.getLogger(__name__)

# Azure Function App instance
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name("sftp_to_blob_copy_file_http_function")
@app.route(route="sftp-to-blob-copy-file", methods=["POST"])
def sftp_to_blob_copy_file_http_function(req: func.HttpRequest) -> func.HttpResponse:
    mime_type = "application/json"

    try:
        req_body = req.get_json()
        file_path = req_body.get("file_path")

        if not file_path:
            return func.HttpResponse(
                json.dumps({
                    "status": "Error",
                    "message": "Missing 'file_path' in request body."
                }),
                status_code=400,
                mimetype=mime_type
            )

        # Validate SMB path format
        validate_smb_path(file_path)

        # Load secrets once
        secrets = {
            "storage_conn": get_secrets("storageacct-sce1dvstorum001-connstring"),
            "container": get_secrets("fhirlite-container-name"),
            "smb_server": get_secrets("dev-fileshare-server"),
            "smb_username": get_secrets("dev-fileshare-username"),
            "smb_password": get_secrets("dev-fileshare-password")
        }

        # Extract filename and define blob path
        file_name = file_path.split("\\")[-1]
        blob_path = f"FHIRLite/{file_name}"

        # Perform streaming upload to Azure Blob
        stream_file_to_blob(
            full_path=file_path,
            blob_path=blob_path,
            secrets=secrets,
            chunk_size=1024 * 1024  # 1MB chunks
        )

        response = {
            "status": "Success",
            "status_code": 200,
            "file_name": file_name,
            "source_file_path": file_path,
            "azure_blob_path": f"{secrets['container']}/{blob_path}"
        }

        return func.HttpResponse(
            json.dumps(response),
            status_code=200,
            mimetype=mime_type
        )

    except Exception as e:
        logger.exception("Error in sftp_to_blob_copy_file_http_function")
        return func.HttpResponse(
            json.dumps({
                "status": "Failed",
                "status_code": 500,
                "message": str(e)
            }),
            status_code=500,
            mimetype=mime_type
        )
