
import os, logging, json, uuid
logging.info("function_app.py loaded!")
import secrets
#import HelperScripts as hs
import azure.functions as func
import azure.durable_functions as df
import asyncio
import jwt
from datetime import datetime, timedelta, timezone
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, metrics
from azure.identity import ClientSecretCredential
from azure.identity.aio import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.subscription import Subscription
from azure.cosmos import CosmosClient, ContainerProxy, DatabaseProxy, CosmosDict
from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError
from openai import AzureOpenAI
from shared.graph_client import get_graph_client
from shared.database_client import get_db_client
from shared.keyvault_client import get_keyvault_client
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
    if req.params.get("validationToken"):
        logging.warning("[notify_new_mail]:Checking Validation Token...")
        if req.params["validationToken"]:
             logging.warning("[notify_new_mail]:Validation token found...")
             return func.HttpResponse(req.params["validationToken"], status_code=200, mimetype="text/plain")
        else:
            logging.warning("[notify_new_mail]:Token validation failed.")
            logging.warning(f"[notify_new_mail]:METHOD={req.method}, from User-Agent={req.headers.get("User-Agent")}")
            return func.HttpResponse(status_code=404)

    if req.method != "POST":
        return func.HttpResponse(status_code=405)
    
    initialize_logger()

    # Validate ClientState
    body = await validate_clientState(req) ## IMPLEMENT TRY LOGIC

    # Validate Subscription
    await validate_subscription(body) ## IMPLEMENT TRY LOGIC

    # Send request JSON to upload_blob
    ## TODO

    # Return Accepted -> Processing status to Graph API
    return func.HttpResponse(status_code=202)

#Subscription Timer Trigger Function
@app.function_name(name="SubscriptionRenewalTimer")
@app.timer_trigger(schedule="0 */5 * * * *",
                   arg_name="timer")
async def renew_subscription(timer: func.TimerRequest) -> None:
    initialize_logger()
    if timer.past_due:
        logging.warning("Renewal Timer is past due! Check failsafe.")
    utc_timestamp = datetime.now().replace(tzinfo=timezone.utc).isoformat()

    creation_results = await create_subscription()
    logging.info("Results obtained!")
    result_id = creation_results["result_id"]
    logging.info("Subscription Result assigned!")
    if result_id:
        logging.info("Running subscription database update function...")
        await update_db_subscription(creation_results)
    else:
        logging.warning("Failed to upsert Subscription: 'result' or 'application.id' was not found.")
    logging.info(f"Renewal timer trigger successfully ran at: {utc_timestamp}")    

async def validate_clientState(req: func.HttpRequest) -> json: 
    logging.info("[notify_new_mail]:Validating ClientState...")
    try:
        body = req.get_json()
        if not isinstance(body.get("value"), list):
            return func.HttpResponse(status_code=400)
        for note in body.get("value",[]):
            if note.get("ClientState"):
                clientState = note["ClientState"]
                keyvault_client = await get_keyvault_client()
                secret = await keyvault_client.get_secret("client-state-secret")
                sub_expiry_raw = note["subscriptionExpirationDateTime"]
                sub_expiry = int(datetime.fromisoformat(sub_expiry_raw).timestamp())

                payload = jwt.decode(
                    clientState,
                    secret,
                    algorithm = "HS256",
                    options = {"require_exp": True}
                )
                jwt_expiry = payload["expiry"]
                if not jwt_expiry == sub_expiry:
                    logging.error("Returning 401, ClientState not authorized or missing.")
                    return func.HttpResponse(status_code=401)
    except Exception as e:
        logging.error(f"JSON body parse or ClientState check failed {e}")
        return func.HttpResponse(status_code=400)
    logging.info("[notify_new_mail]:ClientState verified")
    return body

async def validate_subscription(body: json):
    logging.info("[notify_new_mail]:Validating Subscription Webhook...")
    try:
        value_list = body.get("value")
        if not isinstance(value_list,list) or not value_list:
            raise ValueError("Missing or empty 'value' array.")

        first = value_list[0]
        sub_id = first.get("subscriptionId")
        sub_expiry = first.get("subscriptionExpirationDateTime")

        current_time = datetime.now(timezone.utc)
        sub_renewal = datetime.fromisoformat(sub_expiry) - timedelta(hours=72)

        if current_time >= sub_renewal:
            logging.warning("[notify_new_mail]:Failsafe:Updating subscription...")
            await create_subscription(sub_id=sub_id)
    except Exception as e:
        logging.error(f"[notify_new_mail]:Error retrieving subscription id and expiry date: {e}")
        logging.error("[notify_new_mail]:Triggering subscription renewal...")
        await create_subscription(sub_id=sub_id)

async def create_subscription(**kwargs) -> dict:
    ## CALL DB FOR LATEST SUBSCRIPTION (DB SHOULD ONLY STORE THE CURRENT ACTIVE DB) --> MAYBE ARCHIVE THE OLD IF NEW CREATED
    db_client = get_db_client()
    sub_id = kwargs.get("sub_id")
    cosmos_container = db_client.get_container_client("Subscriptions")
    try:
        if sub_id:
            latest_sub = cosmos_container.read_item(item=sub_id, partition_key="subscription")
        else:
            query = """
                SELECT TOP 1 *
                FROM c
                WHERE c.subscription = @pk
                ORDER BY c.init_at DESC
            """
            params = [dict(name="@pk", value="subscription")]

            iterator = cosmos_container.query_items(
                query=query,
                parameters=params,
                enable_cross_partition_query=False,
                partition_key="subscription"
            )
            latest_sub = next(iterator, None)
            logging.info(f"Latest sub from subscription creation....{latest_sub}")
    except Exception as e:
        logging.warning(f"Failed to get latest_sub...{e}")
        latest_sub = None

    logging.warning("[renew_subscription]:Creating new subscription...")
    keyvault_client = await get_keyvault_client()
    secret = secrets.token_urlsafe(32)
    logging.info("Getting client secret...")
    await keyvault_client.set_secret("client-state-secret", secret)
    next_expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()

    jwt_expiry = int(datetime.fromisoformat(next_expiry).timestamp())
    logging.info(f"[renew_subscription]:New expiration date -> {next_expiry}")
    init_time = datetime.now(timezone.utc).isoformat()
    jwt_token = jwt.encode({"expiry": jwt_expiry}, secret, algorithm="HS256")

    request_body = Subscription(
        change_type = "created",
        notification_url= "https://invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
        resource= f"users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages",
        expiration_date_time= next_expiry,
        client_state= jwt_token,
        latest_supported_tls_version= "v1_2"
    )

    result = None
    graph_client = await get_graph_client()
    logging.info("[renew_subscription]:Submitting new subscription...")
    try:
        result = await graph_client.subscriptions.post(request_body) ## MIGHT NEED TO ADD RETRY LOGIC HERE TO PREVENT LOAD BALANCER ISSUES
        logging.info(f"[renew_subscription]: {result}")
    except Exception as e:
        logging.error(f"Failed to create or update webhook renewal: {e}")
    logging.info("Returning dictionary results...")
    return {
        "result_id": result.id,
        "latest_sub": latest_sub,
        "init_time": init_time,
        "next_expiry": next_expiry
        }

async def update_db_subscription(creation_results: dict):
    db_client = get_db_client()
    cosmos_container = db_client.get_container_client("Subscriptions")
    result_id = creation_results["result_id"]
    latest_sub: CosmosDict = creation_results["latest_sub"]
    logging.info(f"Latest sub from update sub function: {latest_sub}")
    init_time = creation_results["init_time"]
    next_expiry: str = creation_results["next_expiry"]
    try:
        cosmos_container.upsert_item({
            "id": result_id,
            "subscription": "subscription",
            "notifcationUrl": "invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
            "resource": f"/users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages",
            "expirationDateTime": next_expiry,
            "init_at": init_time
        })
        logging.info(f"[renew_subscription]:Successfully uploaded Subscription record to: {cosmos_container.id}")
    except CosmosHttpResponseError as e:
        logging.warning(f"Failed to upload Subscription to {cosmos_container.id}: {e.message}")

    ## MOVE OLD TO ARCHIVE IN DB
    try:
        logging.info("Retrieving old sub information...")
        if latest_sub: ## LATEST SUB IS EMPTY>>>>>>>>>>>>>
            logging.info(f"latest_sub: {latest_sub}")
            if latest_sub["id"]:
                logging.info(f"old_sub_id: {latest_sub["id"]}")
                old_sub_id = latest_sub["id"]
                if latest_sub["expirationDateTime"]:
                    logging.info(f"old_sub_expiry: {latest_sub["expirationDateTime"]}")
                    old_sub_expiry = latest_sub["expirationDateTime"]
                    if latest_sub["init_at"]:
                        logging.info(f"old_init: {latest_sub["init_at"]}")
                        old_init = latest_sub["init_at"]
        else:
            raise ValueError("latest_sub is None or Empty")  
        logging.warning(f"[renew_subscription]: Succesfully obtained subscription to archive")

    except Exception as e:
        logging.warning(f"[renew_subscription]: Failed to obtain subscription to archive: {e}")
        logging.warning(f"latest_sub: {latest_sub}, old_sub_id: {old_sub_id}, old_sub_expiry: {old_sub_expiry}, old_init: {old_init}")

    if latest_sub:
        try:
            logging.info("Upserting old sub into archive...")
            ## ADD OLD SUB TO ARCHIVE
            cosmos_container_archive = db_client.get_container_client("Archived Subscriptions")
            cosmos_container_archive.upsert_item({
                "id": old_sub_id,
                "archive_sub_id": "archived",
                "notifcationUrl": "invoice-automation-app-staging.azurewebsites.net/api/NotifyNewMail",
                "resource": f"/users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages",
                "expirationDateTime": old_sub_expiry,
                "init_at": old_init
            })
            logging.info(f"[renew_subscription]:Successfully added Archive Subscription record to: {cosmos_container_archive.id}")

            ## REMOVE OLD SUB FROM ACTIVE
            logging.info("Removing old sub from active...")
            cosmos_container.delete_item(item=old_sub_id, partition_key="subscription")
            logging.info(f"[renew_subscription]:Successfully removed Subscription record from: {cosmos_container.id}")
        except CosmosHttpResponseError as e:
            logging.warning(f"Failed to update Subscription from {cosmos_container.id}: {e.message}")
    else:
        logging.warning("Latest_sub not found. Skipping db update...")

    if latest_sub:
        try:
            graph_client = await get_graph_client()
            await graph_client.subscriptions.by_subscription_id(latest_sub["id"]).delete()
            logging.info("Expired subscription deleted from graph.")
        except Exception as e:
            logging.warning(f"[renew_subscription]:Failed to delete expired subscription")
    else:
        logging.warning("Latest_sub not found. Skipping sub deletion...")

    # Delete all old graph subscriptions to target
    from msgraph.generated.subscriptions.subscriptions_request_builder import SubscriptionsRequestBuilder as srb
    from kiota_abstractions.base_request_configuration import BaseRequestConfiguration

    target_resource = f"/users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages"
    escaped_resource = target_resource.replace("'","''")
    odata_filter = f"resource eq '{escaped_resource}'"

    try:
        logging.info("Setting up URL Builder...")
        query_params = srb.SubscriptionsRequestBuilderGetQueryParameters(filter=odata_filter)
        request_config = BaseRequestConfiguration(query_parameters=query_params)

        logging.info("Intantiating Builder...")
        builder = srb(request_adapter=graph_client.request_adapter, path_parameters={})
        logging.info("Requesting Response")
        page = await builder.get()
    except Exception as e:
        logging.warning(f"Failed to build target source subscription URL: {e}")
        return

    logging.warning("Deleting existing subscriptions...")
    while page:
        logging.info("Existing subscriptions")
        for sub in page.value:
            if sub.resource == target_resource:
                if sub.id == result_id:
                    logging.warning("[DEBUG]:SAVE SUBSCRIPTION")
                    logging.info(f"Subscription id: {sub.id}, Resource: {sub.resource}")

                logging.warning("[DEBUG]: DELETE SUBSCRIPTION")
                logging.info(f"Subscription id: {sub.id}, Resource: {sub.resource}")
        if not page.odata_next_link:
            logging.warning("No further pages found, breaking loop...")
            break
        logging.warning("New odata link page...")
        page = await builder.with_url(page.odata_next_link).get()
    

# @app.queue_trigger(arg_name="azqueue", queue_name="invoices",
#                                connection="8043d5_STORAGE") 
# def InvoiceTrigger(azqueue: func.QueueMessage):
#     logging.info('Python Queue trigger processed a message: %s',
#                 azqueue.get_body().decode('utf-8'))        