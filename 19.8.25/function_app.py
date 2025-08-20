import os
import json
import logging
import azure.functions as func

from file_helpers import validate_smb_path, get_secrets, stream_file_to_blob, UnauthorizedError



#creates an instance of the Azure Function application using the Azure Functions Python library
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name("sftp_to_blob_copy_file_http_function")
@app.route(route="sftp-to-blob-copy-file", methods=["POST"])

def sftp_to_blob_copy_file_http_function(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        file_path = body.get("file_path")
        mime_type = body.get("mime_type", "application/json")

        if not file_path or not isinstance(file_path, str):
            return func.HttpResponse(
                json.dumps({"status": "Failed", "status_code": 400, "message": "file_path is required"}),
                status_code=400,
                mimetype="application/json"
            )

        validate_smb_path(file_path)

        secrets = {
            "smb_server": get_secrets("smb_server"),
            "smb_username": get_secrets("smb_username"),
            "smb_password": get_secrets("smb_password"),
            "storage_conn": get_secrets("storage_conn"),
            "container": get_secrets("container"),
            "storage_account_endpoint": os.getenv("azure_bloblite-storageaccount-endpoint")
        }

        file_name = file_path.split('\\')[-1]
        ar_number = body.get("ar_number", "default")
        blob_path = f"FHIRLite/{ar_number}/{file_name}"

        result = stream_file_to_blob(
            smb_path=file_path,
            blob_path=blob_path,
            secrets=secrets,
            blob_overwrite=True,
            chunk_size=1024 * 1024
        )

        response = {
            "status": "Success",
            "status_code": 200,
            "file_name": file_name,
            "source_file_path": file_path,
            "azure_blob_path": f"{secrets['storage_account_endpoint']}{secrets['container']}/{blob_path}",
        }
        return func.HttpResponse(json.dumps(response), status_code=200, mimetype=mime_type)

    except FileNotFoundError as e:
        return func.HttpResponse(
            json.dumps({"status": "Failed", "status_code": 404, "message": str(e)}),
            status_code=404,
            mimetype="application/json"
        )
    except UnauthorizedError as e:
        return func.HttpResponse(
            json.dumps({"status": "Failed", "status_code": 401, "message": str(e)}),
            status_code=401,
            mimetype="application/json"
        )
    except Exception as e:
        logging.exception("Error in file copy function")
        return func.HttpResponse(
            json.dumps({"status": "Failed", "status_code": 500, "message": str(e) if e else "Unknown Error"}),
            status_code=500,
            mimetype="application/json"
        )
