import os
import json
import logging
import azure.functions as func

from smb_blob_utils import (
    read_file_from_server_cached,
    upload_to_blob_cached,
    validate_smb_path,
    _get_secret
)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name("sftp_to_blob_copy_file_http_function")
@app.route(route="sftp-to-blob-copy-file", methods=["POST"])
def sftp_to_blob_copy_file_http_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        file_path = req_body.get("file_path")
        mime_type = "application/json"

        if not file_path:
            return func.HttpResponse(
                json.dumps({"status": "Error", "message": "Missing required fields."}),
                status_code=400,
                mimetype=mime_type
            )

        # Validate SMB path using regex
        validate_smb_path(file_path)

        # Get secrets only once
        secrets = {
            "storage_conn": _get_secret("storageacct-sce1dvstorum001-connstring"),
            "container": _get_secret("fhirlite-container-name"),
            "smb_server": _get_secret("dev-fileshare-server"),
            "smb_username": _get_secret("dev-fileshare-username"),
            "smb_password": _get_secret("dev-fileshare-password"),
        }

        file_name = os.path.basename(file_path)
        blob_path = f"FHIRLite/{file_name}"

        file_data = read_file_from_server_cached(file_path, secrets)
        upload_to_blob_cached(file_data, blob_path, secrets)

        response = {
            "status": "Success",
            "status_code": 200,
            "file_name": file_name,
            "source_file_path": file_path,
            "azure_blob_path": f"{secrets['container']}/{blob_path}"
        }

        return func.HttpResponse(json.dumps(response), status_code=200, mimetype=mime_type)

    except Exception as e:
        logging.exception("Error in file copy function")
        return func.HttpResponse(
            json.dumps({"status": "Failed", "status_code": 500, "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
