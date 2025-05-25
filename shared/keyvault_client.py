from azure.keyvault.secrets.aio import SecretClient
from azure.identity.aio import DefaultAzureCredential
import logging, os

_keyvault_client = None

async def get_keyvault_client():
    global _keyvault_client
    if _keyvault_client is None:
        logging.warning("Keyvault client not found. Initializing...")
        try:
            credentials = DefaultAzureCredential()
            _keyvault_client = SecretClient(credential=credentials, vault_url=os.environ["KEYVAULT_CONNECTION_URL"])
        except Exception as e:
            logging.error(f"Failed to get keyvault client: {e}")
    return _keyvault_client

