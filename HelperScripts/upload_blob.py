import base64
from pathlib import Path
from azure.storage.blob import BlobClient
from azure.storage.queue import QueueClient

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
blob = BlobClient.from_connection_string(conn_str=conn_str, container_name="invoices-raw", blob_name="adobe_test")

file_path = Path("..") / "Invoices" / "Foundation_Wide.pdf"

with open(file_path, "rb") as f:
    blob.upload_blob(f, overwrite=True)

print("Invoice blob POWER House.pdf was uploaded successfully!")
# payload = "HELLO WORLD BLOB 2"
# blob.upload_blob(payload)
# print("Blob uploaded successfully!")

queue_conn = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
queue = QueueClient.from_connection_string(conn_str=queue_conn, queue_name="invoices")

queue_payload = f"PDF file at [{file_path}] is ready for processing!"
encoded = base64.b64encode(queue_payload.encode('utf-8')).decode('utf-8')
queue.send_message(encoded)

print("Queue message uploaded successfully!")