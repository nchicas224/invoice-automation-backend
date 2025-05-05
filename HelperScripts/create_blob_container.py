from azure.storage.blob import BlobServiceClient

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

service = BlobServiceClient.from_connection_string(conn_str)
service.create_container("invoices-raw")

container = service.get_container_client("invoices-raw")
print(f"Blob storage container {container.container_name} has been created!")