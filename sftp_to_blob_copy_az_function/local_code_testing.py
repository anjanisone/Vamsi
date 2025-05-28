import os
import csv
import io
from datetime import datetime
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateOptions, ShareAccess, FilePipePrinterAccessMask

def copy_files_from_csv(input_csv_path: str, output_dir: str):
    server = "fsdevca1.headquarters.newcenturyhealth.com"
    username = "DOMAIN\\yourusername"
    password = "yourpassword"

    # Connect to SMB server
    conn = Connection(uuid="", server=server, port=445)
    conn.connect()
    session = Session(conn, username=username, password=password)
    session.connect()

    # Connect to both shares
    tree_qa = TreeConnect(session, fr"\\{server}\QAFileShare")
    tree_qa.connect()

    tree_qa1 = TreeConnect(session, fr"\\{server}\QAFileShare1")
    tree_qa1.connect()

    os.makedirs(output_dir, exist_ok=True)
    audit_log_path = os.path.join(output_dir, "audit_log.csv")

    log_rows = []
    timestamp_now = datetime.utcnow().isoformat()

    # Read CSV input
    with open(input_csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            path = row["full_path"]
            auth_id = row["auth_request_id"]
            annotation_id = row["annotation_id"]

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

                file_open = Open(tree, relative_path,
                            FilePipePrinterAccessMask.GENERIC_READ,
                            FilePipePrinterAccessMask.GENERIC_READ,
                            ShareAccess.FILE_SHARE_READ,
                            CreateOptions.FILE_NON_DIRECTORY_FILE)
                file_open.create()
                data = file_open.read(0, file_open.query_info().end_of_file)
                file_open.close()

                file_name = os.path.basename(path)
                local_file_path = os.path.join(output_dir, file_name)

                with open(local_file_path, 'wb') as f:
                    f.write(data)

                log_rows.append([file_name, path, auth_id, annotation_id, timestamp_now])
            except Exception as e:
                print(f"Error copying {path}: {e}")

    # Write audit log
    with open(audit_log_path, 'w', newline='', encoding='utf-8') as logfile:
        writer = csv.writer(logfile)
        writer.writerow(['file_name', 'full_path', 'auth_request_id', 'annotation_id', 'uploaded_at'])
        writer.writerows(log_rows)

    # Cleanup
    tree_qa.disconnect()
    tree_qa1.disconnect()
    session.disconnect()
    conn.disconnect()

    print(f"✅ Files saved to: {output_dir}")
    print(f"📄 Audit log saved to: {audit_log_path}")