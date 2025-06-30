import os
import json
import logging
import azure.functions as func

from file_helpers import read_file_from_server, upload_to_blob, azure_container_name



#creates an instance of the Azure Function application using the Azure Functions Python library
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
                json.dumps({"status": "Error", 
                            "message": "Missing required fields."}),
                status_code=400,
                mimetype=mime_type
            )

        file_name = os.path.basename(file_path)
        blob_path = f"FHIRLite/{file_name}"

        file_data = read_file_from_server(file_path)
        #print("azure blob path: ", blob_path)
        upload_to_blob(file_data, blob_path)

        response = {
            "status": "Success",
            "status_code": 200,
            "file_name": file_name,
            "source_file_path": file_path,
            "azure_blob_path": f"{azure_container_name}/{blob_path}"
        }
        return func.HttpResponse(json.dumps(response), status_code=200, mimetype=mime_type)

    except Exception as e:
        logging.exception("Error in file copy function")
        return func.HttpResponse(
            json.dumps({"status": "Failed",
                         "status_code": 500, 
                         "message": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
    

