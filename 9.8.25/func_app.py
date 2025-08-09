import os
import json
import logging
import azure.functions as func

from file_helpers import validate_smb_path, get_secrets, stream_file_to_blob


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.function_name("sftp_to_blob_copy_file_http_function")
@app.route(route="sftp-to-blob-copy-file", methods=["POST"])
def sftp_to_blob_copy_file_http_function(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered function that copies a file from SMB/SFTP path to Azure Blob.
    Returns 404 if the source file/path does not exist.
    Expected JSON body:
    {
        "file_path": "\\\\server\\share\\folder\\file.ext",
        "ar_number": "AR12345",
        "mime_type": "application/pdf"   # optional
    }
    """
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(
            json.dumps({
                "status": "Failed",
                "status_code": 400,
                "message": "Invalid JSON payload"
            }),
            status_code=400,
            mimetype="application/json",
        )

    file_path = body.get("file_path")
    ar_number = body.get("ar_number")
    mime_type = body.get("mime_type") or "application/json"

    if not file_path or not ar_number:
        return func.HttpResponse(
            json.dumps({
                "status": "Failed",
                "status_code": 400,
                "message": "Missing required fields: file_path and ar_number"
            }),
            status_code=400,
            mimetype="application/json",
        )

    try:
        secrets = {
            "container": get_secrets("fhirlite-container-name"),
            "smb_server": get_secrets("dev-fileshare-server"),
            "smb_username": get_secrets("dev-fileshare-username"),
            "smb_password": get_secrets("dev-fileshare-password"),
            "storage_account_endpoint": get_secrets("fhirlite-storageaccount-endpoint"),
        }

        # --- Existence check: return 404 if not found ---
        # validate_smb_path should return a boolean or raise for inaccessible paths.
        try:
            exists = validate_smb_path(file_path)
        except Exception as e:
            msg = str(e)
            if "no such file" in msg.lower() or "not found" in msg.lower() or "path does not exist" in msg.lower():
                return func.HttpResponse(
                    json.dumps({
                        "status": "Failed",
                        "status_code": 404,
                        "message": f"File not found at SFTP path: {file_path}"
                    }),
                    status_code=404,
                    mimetype="application/json",
                )
            # If it's a permission/connectivity error, surface it as 500
            raise

        if not exists:
            return func.HttpResponse(
                json.dumps({
                    "status": "Failed",
                    "status_code": 404,
                    "message": f"File not found at SFTP path: {file_path}"
                }),
                status_code=404,
                mimetype="application/json",
            )

        file_name = file_path.split('\\')[-1] if '\\' in file_path else os.path.basename(file_path)
        blob_path = f"FHIRLite/{ar_number}/{file_name}"

        # Stream/copy source file to blob storage
        result = stream_file_to_blob(
            smb_path=file_path,
            blob_path=blob_path,
            secrets=secrets,
        )

        # Normalize return (stream_file_to_blob may return URL or dict)
        if isinstance(result, dict):
            azure_blob_path = result.get("blob_url") or result.get("blob_path") or blob_path
            extra = result
        else:
            azure_blob_path = result or blob_path
            extra = {"result": result}

        response = {
            "status": "Success",
            "status_code": 200,
            "message": "File copied to Blob successfully",
            "azure_blob_path": azure_blob_path,
            "ar_number": ar_number,
            "file_name": file_name,
            **extra
        }
        return func.HttpResponse(json.dumps(response), status_code=200, mimetype=mime_type)

    except Exception as e:
        logging.exception("Error in file copy function")
        return func.HttpResponse(
            json.dumps({
                "status": "Failed",
                "status_code": 500,
                "message": str(e)
            }),
            status_code=500,
            mimetype="application/json",
        )
