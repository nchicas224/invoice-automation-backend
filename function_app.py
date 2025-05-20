import os, logging, json, uuid
from datetime import datetime, timedelta, timezone
import HelperScripts as hs
import azure.functions as func
import azure.durable_functions as df
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, metrics
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.subscription import Subscription
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from openai import AzureOpenAI

## Initialize runtime clients

## Graph Client
async def get_graph_client():
    credentials = ClientSecretCredential(client_id=os.environ["AZURE_CLIENT_ID"],
                                   tenant_id=os.environ["AZURE_TENANT_ID"],
                                   client_secret=os.environ["AZURE_CLIENT_SECRET"])
    try: 
        graph_client = GraphServiceClient(credentials=credentials, scopes=["https://graph.microsoft.com/.default"])
        builder = graph_client.users.by_user_id(os.environ["INVOICES_MAILBOX_ID"])
        response = await builder.get()
        if response:
            logging.info(f"Connection Successful to Mailbox: {response.display_name}")
        else:
            logging.warning("Reponse was empty...")
    except ValueError as e:
        logging.error(f"Failed Graph connection: {e}")
    client = await graph_client
    return client

## Database Client
def get_db_client():
    # 1) Configuration for DB
    endpoint = os.environ["DB_ENDPOINT"]
    key = os.environ["DB_KEY"]
    database_name  = "InvoiceDB"

    # 2) Init client & container
    client    = CosmosClient(endpoint, key)
    database  = client.get_database_client(database_name)

    # 1) List all databases
    logging.info("Databases:")
    for db in client.list_databases():
        logging.info(db["id"])
        
    # 2) If InvoiceDB is in that list, list its containers
    db_name = "InvoiceDB"
    if any(db["id"] == db_name for db in client.list_databases()):
        logging.info(f"Containers in {db_name}:")
        for coll in database.list_containers():
            logging.info(coll["id"])
    else:
        logging.error(f"ERROR: Database '{db_name}' not found.")
    return database

## Application Insights Initalize
configure_azure_monitor(connection_string=os.environ["APP_INSIGHT_CONNECTION_STRING"])
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
failsafe_counter = meter.create_counter(
    name="failsafe_runs",
    description="Number of failsafe subscription renewals",
    unit="1"
)

graph_client = get_graph_client()
database_client = get_db_client()

app = func.FunctionApp()

# Webhook Notification for new emails landing in invoices@lcf
@app.function_name(name="NotifyNewMail")
@app.route(
    route="NotifyNewMail",
    methods=["GET","POST"],
    auth_level=func.AuthLevel.ANONYMOUS)
#@app.durable_client_input(client_name="client")
async def notify_new_mail(req: func.HttpRequest) -> func.HttpResponse:
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
    cosmos_container = database_client.get_container_client("Subscriptions")
    doc = cosmos_container.read_item(item="subscription", partition_key="subscription")
    sub_id = doc["subId"]
    next_renewal = datetime.fromisoformat(doc["nextFailSafe"])
    next_renewal.replace(tzinfo=timezone.utc)

    if datetime.now(timezone.utc) >= next_renewal:
        logging.info("Running subscription renewal failsafe")
        failsafe_counter.add(1)
        next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat() + "Z"
        graph_client.subscriptions.by_subscription_id(sub_id).patch(body={"expirationDateTime": next_expiry})

        doc["nextFailSafe"] = (datetime.now(timezone.utc) + timedelta(days=3) - timedelta(hours=12)).isoformat()
        cosmos_container.replace_item(items="subscription", body=doc)

    # Send request JSON to upload_blob
    ## TODO

    # Return Accepted -> Processing status to Graph API
    return func.HttpResponse(status_code=202)

@app.function_name(name="SubscriptionRenewalTimer")
@app.timer_trigger(schedule="0 */1 * * * *",
                   arg_name="timer")
async def renew_subscription(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("Renewal Timer is past due! Check failsafe.")
    utc_timestamp = datetime.now().replace(tzinfo=timezone.utc).isoformat()

    graph_client = await get_graph_client()
    client_state = os.environ["CLIENT_STATE"]
    next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    next_failsafe = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    logging.warning(next_expiry)

    request_body = Subscription(
        change_type = "created",
        notification_url= "http://localhost:7071/api/NotifyNewMail",
        resource= "/users/mailFolders('Inbox')/messages",
        expiration_date_time= next_expiry,
        client_state= client_state,
        latest_supported_tls_version= "v1_2"
    )

    result = None
    try:
        result = await graph_client.subscriptions.post(request_body)
        logging.info(result)
    except Exception as e:
        logging.error(f"Failed to create or update webhook renewal: {e}")
    
    if result and result.application_id:
        try:
            cosmos_container = get_db_client().get_container_client("Subscriptions")
            cosmos_container.upsert_item({
                "id": "subscription",
                "subId": result.application_id,
                "notifcationUrl": "http://localhost:7071/api/NotifyNewMail",
                "resource": "/users/mailFolders('Inbox')/messages",
                "expirationDateTime": next_expiry,
                "nextFailSafe": next_failsafe
            })
            logging.info(f"Successfully uploaded Subscription record to: {cosmos_container.id}")
        except CosmosHttpResponseError as e:
            logging.warning(f"Failed to upload Subscription to {cosmos_container.id}: {e.message}")
    else:
        logging.warning("Failed to upsert Subscription: 'result' or 'application.id' was not found.")
    logging.info(f"Renewal timer trigger successfull ran at: {utc_timestamp}")    

# @app.queue_trigger(arg_name="azqueue", queue_name="invoices",
#                                connection="8043d5_STORAGE") 
# def InvoiceTrigger(azqueue: func.QueueMessage):
#     logging.info('Python Queue trigger processed a message: %s',
#                 azqueue.get_body().decode('utf-8'))        