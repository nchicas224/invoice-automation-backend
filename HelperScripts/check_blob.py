from azure.storage.blob import BlobClient

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

blob = BlobClient.from_connection_string(conn_str=conn_str, container_name="invoices-raw", blob_name="invoice_test")

blob_content = blob.download_blob().readall()
print(blob_content[:5])
print(len(blob_content))
