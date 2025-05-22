
import os, logging, json, uuid
logging.info("function_app.py loaded!")
import secrets
#import HelperScripts as hs
import azure.functions as func
import azure.durable_functions as df
import asyncio
from datetime import datetime, timedelta, timezone
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, metrics
from azure.identity import ClientSecretCredential
from azure.identity.aio import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.subscription import Subscription
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError
from openai import AzureOpenAI
from shared.graph_client import get_graph_client
from shared.database_client import get_db_client
from shared.azure_monitor import failsafe_counter, tracer, meter, initialize_logger

app = func.FunctionApp()

# Webhook Notification for new emails landing in invoices@lcf
@app.function_name(name="NotifyNewMail")
@app.route(
    route="NotifyNewMail",
    methods=["GET","POST"],
    auth_level=func.AuthLevel.ANONYMOUS)
#@app.durable_client_input(client_name="client")
async def notify_new_mail(req: func.HttpRequest) -> func.HttpResponse:
    initialize_logger()
    # Graph API handshake
    if req.method == "GET" and "validationToken" in req.params:
        return func.HttpResponse(req.params["validationToken"], status_code=200)
    
    #Validate ClientState
    body = req.get_json()
    for note in body.get("value",[]):
        if note.get("ClientState") != os.environ["CLIENT_STATE"]:
            return func.HttpResponse(status_code=401)
    logging.info("ClientState verified")

    # Validate Subscription
    database_client = get_db_client()
    cosmos_container = database_client.get_container_client("Subscriptions")
    try:
        doc = cosmos_container.read_item(item="subscription", partition_key="subscription")
        sub_id = doc["subId"]
        next_renewal = datetime.fromisoformat(doc["nextFailSafe"])
        next_renewal.replace(tzinfo=timezone.utc)

        if datetime.now(timezone.utc) >= next_renewal:
            logging.info("Running subscription renewal failsafe")
            #failsafe_counter.add(1)
            next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat() + "Z"
            graph_client = await get_graph_client()
            graph_client.subscriptions.by_subscription_id(sub_id).patch(body={"expirationDateTime": next_expiry})

            doc["nextFailSafe"] = (datetime.now(timezone.utc) + timedelta(days=3) - timedelta(hours=12)).isoformat()
            cosmos_container.replace_item(items="subscription", body=doc)
    except CosmosResourceNotFoundError:
        logging.warning("Error running failsafe: Updating subscription...")
        await renew_subscription(func.TimerRequest)
    # Send request JSON to upload_blob
    ## TODO

    # Return Accepted -> Processing status to Graph API
    return func.HttpResponse(status_code=202)

@app.function_name(name="SubscriptionRenewalTimer")
@app.timer_trigger(schedule="0 */1 * * * *",
                   arg_name="timer")
async def renew_subscription(timer: func.TimerRequest) -> None:
    initialize_logger()
    if timer.past_due:
        logging.warning("Renewal Timer is past due! Check failsafe.")
    utc_timestamp = datetime.now().replace(tzinfo=timezone.utc).isoformat()

    client_state = None
    if os.environ["ENV"] == "dev":
        client_state = os.environ["CLIENT_STATE"]
    client_state = secrets.token_urlsafe(32)
    next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    next_failsafe = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    logging.warning(next_expiry)

    request_body = Subscription(
        change_type = "created",
        notification_url= "invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
        resource= "/users/mailFolders('Inbox')/messages",
        expiration_date_time= next_expiry,
        client_state= client_state,
        latest_supported_tls_version= "v1_2"
    )

    result = None
    graph_client = await get_graph_client()
    try:
        result = await graph_client.subscriptions.post(request_body) #-> Bad Request 400 on the subscription post...Check JSON body. Also need logging connection to App insights
        logging.info(result)
    except Exception as e:
        logging.error(f"Failed to create or update webhook renewal: {e}")
    
    if result and result.application_id:
        try:
            cosmos_container = get_db_client().get_container_client("Subscriptions")
            cosmos_container.upsert_item({
                "id": "subscription",
                "subId": result.application_id,
                "notifcationUrl": "invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
                "resource": "/users/mailFolders('Inbox')/messages",
                "expirationDateTime": next_expiry,
                "nextFailSafe": next_failsafe
            })
            logging.info(f"Successfully uploaded Subscription record to: {cosmos_container.id}")
        except CosmosHttpResponseError as e:
            logging.warning(f"Failed to upload Subscription to {cosmos_container.id}: {e.message}")
    else:
        logging.warning("Failed to upsert Subscription: 'result' or 'application.id' was not found.")
    logging.info(f"Renewal timer trigger successfully ran at: {utc_timestamp}")    

# @app.queue_trigger(arg_name="azqueue", queue_name="invoices",
#                                connection="8043d5_STORAGE") 
# def InvoiceTrigger(azqueue: func.QueueMessage):
#     logging.info('Python Queue trigger processed a message: %s',
#                 azqueue.get_body().decode('utf-8'))        