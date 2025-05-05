from azure.storage.queue import QueueServiceClient
from azure.core.exceptions import ResourceNotFoundError

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
queue = QueueServiceClient.from_connection_string(conn_str).get_queue_client("invoices")

try:
    props = queue.get_queue_properties()
    print(f"Connected! Queue name: {props['name']}, Queue length: {props["approximate_message_count"]}")
except ResourceNotFoundError:
    print("Queue not found!")