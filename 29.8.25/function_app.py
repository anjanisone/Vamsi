import os
import json
import logging
import uuid
import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from evolent.dataplatform.evolent_utils.loggers.function_logger import FunctionLogger

# --- structured logger (same as orch_event.py) ---
logger = FunctionLogger(logging.DEBUG)
logger.update_props({"env": os.getenv("EV_FUNCTION_ENVIRONMENT", "local")})

from file_helpers import (
    validate_smb_path,
    get_secrets,
    stream_file_to_blob,
    create_error_response,
)

# creates an instance of the Azure Function application using the Azure Functions Python library
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name("sftp_to_blob_copy_file_http_function")
@app.route(route="sftp-to-blob-copy-file", methods=["POST"])
def sftp_to_blob_copy_file_http_function(req: func.HttpRequest) -> func.HttpResponse:
    # set static props for this function
    logger.update_props({
        "component": "function_app",
        "function": "sftp_to_blob_copy_file_http_function",
    })

    # correlate logs across the pipeline; set separately so it's reusable later
    corr_id = req.headers.get("x-correlation-id") or str(uuid.uuid4())
    logger.update_props({"correlation_id": corr_id})

    try:
        req_body = req.get_json()
        logger.info(f"Received request body: {json.dumps(req_body)[:1000]}")

        # Required fields
        smb_path = req_body.get("smb_path")
        blob_path = req_body.get("blob_path")
        secret_name = req_body.get("secret_name")
        blob_overwrite = bool(req_body.get("blob_overwrite", False))

        if not all([smb_path, blob_path, secret_name]):
            logger.warning("Missing one or more required fields (smb_path, blob_path, secret_name)")
            return create_error_response(
                "Bad Request", 400, "Missing required fields: smb_path, blob_path, secret_name"
            )

        # Validate SMB path against allowed patterns
        validate_smb_path(smb_path)

        # Get secrets (SMB + Blob)
        secrets_raw = get_secrets(secret_name)

        # Stream file to blob
        blob_url = stream_file_to_blob(
            smb_path, blob_path, json.loads(secrets_raw), blob_overwrite
        )

        logger.info(f"Copy successful. Blob URL: {blob_url}")
        return func.HttpResponse(
            json.dumps({
                "status": "Success",
                "status_code": 200,
                "blob_url": blob_url,
                "correlation_id": corr_id
            }),
            status_code=200,
            mimetype="application/json",
        )

    except FileNotFoundError as e:
        logger.warning(f"File not found: {str(e)}")
        return create_error_response("Not Found", 404, str(e))

    except PermissionError as e:
        logger.error(f"Access denied: {str(e)}")
        return create_error_response("Forbidden", 403, str(e))

    except ResourceNotFoundError as e:
        logger.error(f"Key Vault or Blob resource not found: {e}")
        return create_error_response("Key Not Found", 404, f"Required configuration not found: {str(e)}")

    except ValueError as e:
        # Invalid SMB path from validate_smb_path
        logger.warning(f"Invalid request: {e}")
        return create_error_response("Bad Request", 400, str(e))

    except Exception as e:
        logger.exception("Error in file copy function")
        error_message = str(e) if str(e) else "Unspecified error"
        return create_error_response("Failed", 500, error_message)
