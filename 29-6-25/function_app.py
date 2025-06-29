import logging
import os
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    key_vault_url = os.environ.get("KEY_VAULT_URL")
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    smb_username = secret_client.get_secret("smb-username").value
    smb_password = secret_client.get_secret("smb-password").value
    blob_connection_string = secret_client.get_secret("blob-connection-string").value

    try:
        req_body = req.get_json()
        file_path = req_body.get("file_path")
        auth_id = req_body.get("carepro_AuthrequestId")
        annotation_id = req_body.get("carepro_DocumentBase_Annotation_Id")

        if not (file_path and auth_id and annotation_id):
            return func.HttpResponse("Missing required fields", status_code=400)

        from file_helpers import copy_file_to_blob

        blob_path = copy_file_to_blob(
            file_path=file_path,
            auth_id=auth_id,
            annotation_id=annotation_id,
            smb_username=smb_username,
            smb_password=smb_password,
            blob_connection_string=blob_connection_string
        )

        return func.HttpResponse(
            body=json.dumps({
                "status": "Success",
                "azure_blob_path": blob_path
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        return func.HttpResponse(f"Internal Server Error: {str(e)}", status_code=500)
