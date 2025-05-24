
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
    # Graph API handshake
    if req.method == "POST" and req.params.get("validationToken"):
        logging.warning("[notify_new_mail]:Checking Validation Token...")
        if req.params["validationToken"]:
             logging.warning("[notify_new_mail]:Validation token found...")
             return func.HttpResponse(req.params["validationToken"], status_code=200)
        else:
            logging.warning("[notify_new_mail]:Token validation failed.")
            return func.HttpResponse(status_code=404)

    initialize_logger()

    #Validate ClientState
    ## CHECK WITH SUB EXPIRY TIMESTAMP IN JWT FOR CORRECT SECRET BEFORE PULLING SECRET ---> NOT IN CORRECT SUBSCRIPTION...DETERMINE FALLBACK
    ## IF TIMESTAMP MATCHES SUB ID SUBSCRIPTION EXPIRY DATE
        ## PULL SECRET FROM KEY VAULT
        ## VALIDATE JWT CLIENTSTATE
            ## VALIDATED? CONTINUE
            ## ERROR? REJECT ---> DETERMINE FALLBACK OR LOGS
    ## ELSE:
        ## TODO -> NOT IN CORRECT SUBSCRIPTION...DETERMINE FALLBACK.

    logging.info("[notify_new_mail]:Validating ClientState...")
    try:
        body = await req.get_json()
        for note in body.get("value",[]):
            if note.get("ClientState") != os.environ["CLIENT_STATE"]: ## ---> Must implement other logic to persist this variable, JWT or HMAC
                logging.error("Returning 401, ClientState not authorized or missing.")
                return func.HttpResponse(status_code=401)
    except Exception as e:
        logging.error(f"JSON body parse or ClientState check failed {e}")
        return func.HttpResponse(status_code=400)
    logging.info("[notify_new_mail]:ClientState verified")

    # Validate Subscription
    logging.info("[notify_new_mail]:Validating Subscription Webhook...")
    try:
        body = await req.get_json()
        value_list = body.get("value")
        if not isinstance(value_list,list) or not value_list:
            raise ValueError("Missing or empty 'value' array.")

        first = value_list[0]
        sub_id = first.get("subscriptionId")
        sub_expiry = first.get("subscriptionExpirationDateTime")

        current_time = datetime.now(timezone.utc)
        sub_renewal = datetime.fromisoformat(sub_expiry) - timedelta(hours=12)

        if current_time >= sub_renewal:
            logging.warning("[notify_new_mail]:Failsafe:Updating subscription...")
            await renew_subscription(sub_id)
    except Exception as e:
        logging.error(f"[notify_new_mail]:Error retrieving subscription id and expiry date: {e}")
        logging.error("[notify_new_mail]:Triggering subscription renewal...")
        await renew_subscription(sub_id)

    # Send request JSON to upload_blob
    ## TODO

    # Return Accepted -> Processing status to Graph API
    return func.HttpResponse(status_code=202)

#Subscription Timer Trigger Function
@app.function_name(name="SubscriptionRenewalTimer")
@app.timer_trigger(schedule="0 */2 * * * *",
                   arg_name="timer")
async def renew_subscription(timer: func.TimerRequest, sub_id: str) -> None:
    initialize_logger()
    if timer.past_due:
        logging.warning("Renewal Timer is past due! Check failsafe.")
    utc_timestamp = datetime.now().replace(tzinfo=timezone.utc).isoformat()

    ## CALL DB FOR LATEST SUBSCRIPTION (DB SHOULD ONLY STORE THE CURRENT ACTIVE DB) --> MAYBE ARCHIVE THE OLD IF NEW CREATED
    ## CHECK JWT --> RETURN 401 IF MISMATCH
    ## CHECK SUB AGAINST EXPIRY --> IF EXPIRED ? CREATE/ARCHIVE OLD : RETURN
    ## EXPIRED -> RUN ASYNC
        #CREATE NEW SECRET
        #UPDATE SECRET TO KEY 
        #CREATE NEW JWT WITH SIGNED EXPIRY TIMESTAMP
        #CREATE NEW SUBSCRIPTION WITH JWT
        #ARCHIVE OLD SUBSCRIPTION VIA SUB ID
    ## ELSE -> RETURN

    if not os.environ["ENV"] == "dev":
        os.environ["CLIENT_STATE"] = secrets.token_urlsafe(32) ## ---> Must implement other logic to persist this variable, JWT or HMAC

    next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    next_failsafe = (datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat()
    logging.info(f"[renew_subscription]:New expiration date -> {next_expiry}")

    
    request_body = Subscription(
        change_type = "created",
        notification_url= "https://invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
        resource= f"users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages",
        expiration_date_time= next_expiry,
        client_state= f"{os.environ["CLIENT_STATE"]}", ## ---> Must implement other logic to persist this variable, JWT or HMAC
        latest_supported_tls_version= "v1_2"
    )

    result = None
    graph_client = await get_graph_client()
    logging.info("[renew_subscription]:Submitting new subscription...")
    try:
        result = await graph_client.subscriptions.post(request_body)
        logging.info(f"[renew_subscription]: {result}")
    except Exception as e:
        logging.error(f"Failed to create or update webhook renewal: {e}")
    
    if result and result.id:
        try:
            cosmos_container = get_db_client().get_container_client("Subscriptions")
            cosmos_container.upsert_item({
                "id": result.id,
                "subscription": "subscription",
                "notifcationUrl": "invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
                "resource": f"/users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages",
                "expirationDateTime": next_expiry,
                "nextFailSafe": next_failsafe
            })
            logging.info(f"[renew_subscription]:Successfully uploaded Subscription record to: {cosmos_container.id}")
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