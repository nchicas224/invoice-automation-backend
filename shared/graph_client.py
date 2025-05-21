import logging, os
from azure.identity.aio import DefaultAzureCredential, ClientSecretCredential
from msgraph import GraphServiceClient

env = os.getenv("ENV")
_graph_client = None

## Graph Client
async def get_graph_client():
    global _graph_client
    if _graph_client is None:
        if env == "dev":
            credentials = ClientSecretCredential(client_id=os.environ["AZURE_CLIENT_ID"],
                                        tenant_id=os.environ["AZURE_TENANT_ID"],
                                        client_secret=os.environ["AZURE_CLIENT_SECRET"])
        else:
            credentials = DefaultAzureCredential()
        
        try: 
            _graph_client = GraphServiceClient(credentials=credentials, scopes=["https://graph.microsoft.com/.default"])
            builder = _graph_client.users.by_user_id(os.environ["INVOICES_MAILBOX_ID"])
            response = await builder.get()
            if response:
                logging.info(f"Connection Successful to Mailbox: {response.display_name}")
            else:
                logging.warning("Reponse was empty...")
        except ValueError as e:
            logging.error(f"Failed Graph connection: {e}")
    return _graph_client