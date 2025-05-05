import base64
from azure.storage.queue import QueueServiceClient

conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"

queue = QueueServiceClient.from_connection_string(conn_str).get_queue_client("invoices")

payload = "Hello World!"
encoded = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
queue.send_message(encoded)

print("Message sent successfully")