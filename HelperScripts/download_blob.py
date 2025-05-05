from azure.storage.blob import BlobClient
from pathlib import Path

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

blob = BlobClient.from_connection_string(conn_str=conn_str, container_name="invoices-raw", blob_name="invoice_test")

file = Path("..") / "Invoices" / "Downloaded" / "invoice_test.pdf"


with open(file, "wb") as f:
    stream = blob.download_blob()
    for chuck in stream.chunks():
        stream.readinto(f)

# blob_data = blob.download_blob()
# print(blob_data.readall().decode('utf-8'))
print("Blob downloaded successfully!")