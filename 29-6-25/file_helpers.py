def copy_file_to_blob(file_path, auth_id, annotation_id, smb_username, smb_password, blob_connection_string):
    import os
    from smb.SMBConnection import SMBConnection
    from azure.storage.blob import BlobServiceClient

    smb_server = os.environ.get("SMB_SERVER", "fsdevca1.headquarters.newcenturyhealth.com")
    smb_share = os.environ.get("SMB_SHARE", "QAFileShare")

    domain = "HEADQUARTERS"
    machine_name = os.environ.get("MACHINE_NAME", "client-machine")

    conn = SMBConnection(
        smb_username, smb_password, machine_name, smb_server, domain=domain, use_ntlm_v2=True
    )
    assert conn.connect(smb_server, 139), "Could not connect to SMB server"

    path_parts = file_path.split("\\")
    dir_path = path_parts[:-1]
    file_name = path_parts[-1]
    smb_path = "/".join(dir_path)

    with open(file_name, "wb") as f:
        conn.retrieveFile(smb_share, f"/{smb_path}/{file_name}", f)

    blob_client = BlobServiceClient.from_connection_string(blob_connection_string)
    container = "carepro-files"
    blob_path = f"{auth_id}/{annotation_id}/{file_name}"

    with open(file_name, "rb") as data:
        blob_client.get_blob_client(container=container, blob=blob_path).upload_blob(data, overwrite=True)

    os.remove(file_name)
    return f"https://{blob_client.account_name}.blob.core.windows.net/{container}/{blob_path}"
