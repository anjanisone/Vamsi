from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open
from smbprotocol.open import FileInformationClass

def main(name: str) -> list:
    server = "fsdevca1.headquarters.newcenturyhealth.com"
    share = "QAFileShare"
    base_path = "AuthorizationRequest"
    username = "DOMAIN\\yourusername"
    password = "yourpassword"

    conn = Connection(uuid="", server=server, port=445)
    conn.connect()
    session = Session(conn, username=username, password=password)
    session.connect()
    tree = TreeConnect(session, fr"\\{server}\{share}")
    tree.connect()

    result = []

    def walk(path):
        directory = Open(tree, path)
        directory.create()
        entries = directory.query_directory("*", FileInformationClass.FILE_DIRECTORY_INFORMATION)
        directory.close()

        for entry in entries:
            name = entry['file_name'].get_value()
            if name in ['.', '..']:
                continue
            full_path = f"{path}\{name}"
            if entry['file_attributes'].get_value() & 0x10:
                walk(full_path)
            else:
                if full_path.lower().endswith(('.pdf', '.docx', '.txt')):
                    result.append(full_path)

    walk(base_path)

    tree.disconnect()
    session.disconnect()
    conn.disconnect()

    return result
