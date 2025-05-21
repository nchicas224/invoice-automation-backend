import os, logging
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from azure.identity import DefaultAzureCredential

env = os.getenv("ENV")
_database = None

def get_db_client():
    global _database
    if _database == None:
        # 1) Configuration for DB
        endpoint = os.environ["DB_ENDPOINT"]
        key = os.environ["DB_KEY"]
        database_name  = "InvoiceDB"

        # 2) Init client & container
        if env == ("staging"):
            db_connection_string = os.environ["COSMOS_DB_CONNECTION_STRING"]
            client = CosmosClient.from_connection_string(conn_str=db_connection_string)
        elif env == ("prod"):
            credential = DefaultAzureCredential()
            client = CosmosClient(endpoint, credential=credential)
        elif env == ("dev"):
            client = CosmosClient(endpoint, key)
        else:
            logging.error(f"Unknown environment: {env}")
        _database  = client.get_database_client(database_name)

        # 1) List all databases
        logging.info("Databases:")
        for db in client.list_databases():
            logging.info(db["id"])
            
        # 2) If InvoiceDB is in that list, list its containers
        db_name = "InvoiceDB"
        if any(db["id"] == db_name for db in client.list_databases()):
            logging.info(f"Containers in {db_name}:")
            for coll in _database.list_containers():
                logging.info(coll["id"])
        else:
            logging.error(f"ERROR: Database '{db_name}' not found.")
    return _database