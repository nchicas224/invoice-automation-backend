
import os, logging, json, uuid, base64
logging.info("function_app.py loaded!")
import secrets
#import HelperScripts as hs
import azure.functions as func
import azure.durable_functions as df
import asyncio
import jwt
from typing import List,Dict,Any
from datetime import datetime, timedelta, timezone
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace, metrics
from azure.identity import ClientSecretCredential
from azure.identity.aio import DefaultAzureCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.subscription import Subscription
from msgraph.generated.users.item.messages.item.message_item_request_builder import MessageItemRequestBuilder
from msgraph.generated.users.item.messages.item.reply.reply_post_request_body import ReplyPostRequestBody
from msgraph.generated.models.message import Message
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.email_address import EmailAddress
from msgraph.generated.models.attachment import Attachment
from msgraph.generated.models.attachment_collection_response import AttachmentCollectionResponse
from msgraph.generated.models.file_attachment import FileAttachment
from azure.functions import HttpResponse
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.cosmos import CosmosClient, ContainerProxy, DatabaseProxy, CosmosDict
from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from openai import AzureOpenAI
from pypdf import PdfWriter
from io import BytesIO
from shared.graph_client import get_graph_client
from shared.database_client import get_db_client
from shared.keyvault_client import get_keyvault_client
from shared.di_process import get_invoice_fields
from shared.process_ai_results import process_ai_results
from shared.populate_checkform import fillout_form
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
    try:
        check = await validate_clientState(req)
        if isinstance(check, (HttpResponse)):
            return check
        body = check
    except Exception:
        logging.exception("Unexpected error during ClientState Validation")
        return func.HttpRequest(status_code=500, body="Server Error")
    
    # Validate Subscription
    try:
       check = await validate_subscription(body)
       if isinstance(check, HttpResponse):
           return check
    except Exception as e:
        logging.exception(e)
        return func.HttpResponse(status_code=500, body="Failed to Validate Subscription")

    # Start processes
    try:
        await start(body=body)
    except Exception as e:
        logging.exception(f"Failed to process invoice(s): {e}")
        logging.error(f"Discarding notification...")
        return func.HttpResponse(status_code=200)

    # Return Accepted -> Processing status to Graph API
    return func.HttpResponse(status_code=200)

#Subscription Timer Trigger Function
@app.function_name(name="SubscriptionRenewalTimer")
@app.timer_trigger(schedule="0 */30 * * * *",
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

async def validate_clientState(req: func.HttpRequest) -> Dict[str,List[Dict[str,Any]]]: 
    logging.info("[notify_new_mail]:Validating ClientState...")
    try:
        body: Dict[str,List[Dict[str,Any]]] = req.get_json()
        if not isinstance(body.get("value"), list):
            logging.error("Value is not an iterable list.")
            return func.HttpResponse(status_code=400)
        try:
            for note in body.get("value",[]):
                if note.get("clientState"):
                    clientState = note["clientState"]
                    keyvault_client = await get_keyvault_client()
                    secret = await keyvault_client.get_secret("client-state-secret")
                    sub_expiry_raw = note["subscriptionExpirationDateTime"]
                    sub_expiry = int(datetime.fromisoformat(sub_expiry_raw).timestamp())

                    payload = jwt.decode(
                        clientState,
                        secret.value,
                        algorithms = "HS256",
                        options = {"require_exp": True}
                    )
                    jwt_expiry = payload["expiry"]
                    if not jwt_expiry == sub_expiry:
                        logging.error("Returning 401, ClientState not authorized or missing.")
                        return func.HttpResponse(status_code=401)
                else:
                    logging.error("ClientState not found in response body.")
                    return func.HttpResponse(status_code=400, body="ClientState not found")
        except Exception as e:
            e.add_note("Error during ClientState validation")
            raise e
    except ValueError as v:
        logging.error(f"JSON body parse: {v}")
        return func.HttpResponse(status_code=400)
    logging.info("[notify_new_mail]:ClientState verified")
    return body

async def validate_subscription(body: Dict[str,List[Dict[str,Any]]]): ### NEED TO FIX POSSIBLE DUPLICATE CALLINGS
    logging.info("[notify_new_mail]:Validating Subscription Webhook...")
    try:
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
            creation_results = await create_subscription(sub_id=sub_id)
            await update_db_subscription(creation_results)

    except Exception as e:
        logging.error(f"[notify_new_mail]:Error retrieving subscription id and expiry date: {e}")
        logging.error("[notify_new_mail]:Triggering subscription renewal...")
        creation_results = await create_subscription(sub_id=sub_id)
        await update_db_subscription(creation_results)

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

async def update_db_subscription(creation_results: dict): ### NEED TO UNBLOAT THIS FUNCTION
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

    target_resource = f"users/{os.environ["INVOICES_MAILBOX_ID"]}/mailFolders('Inbox')/messages"
    try:
        logging.info("Intantiating Builder...")
        builder = srb(request_adapter=graph_client.request_adapter, path_parameters={})
        logging.info("Requesting Response")
        page = await builder.get()
    except Exception as e:
        logging.warning(f"Failed to build target source subscription URL: {e}")
        return

    logging.warning("Deleting existing subscriptions...")
    while page:
        for sub in page.value:
            if sub.resource == target_resource:
                if sub.id == result_id:
                    logging.warning("Current subscription found, skipping deletion...")
                    logging.info(f"Subscription id: {sub.id}, Resource: {sub.resource}")
                else:
                    logging.warning("Deleting Subscription...")
                    logging.info(f"Subscription id: {sub.id}, Resource: {sub.resource}")
                    await graph_client.subscriptions.by_subscription_id(sub.id).delete()
        if not page.odata_next_link:
            logging.warning("No further pages found, breaking loop...")
            break
        logging.warning("New odata link page...")
        page = await builder.with_url(page.odata_next_link).get()
    
async def start(body: Dict[str,List[Dict[str,Any]]]) -> None:
    graph_client = await get_graph_client()

    try:
        message_id = body.get("value")[0].get("resourceData").get("id")
        logging.info(f"Request Message ID: {message_id}")
    except Exception as e:
        logging.error(f"Message ID was not found: {e}")
        return

    req_builder: MessageItemRequestBuilder = graph_client.users.by_user_id(
        os.environ["INVOICES_MAILBOX_ID"]).messages.by_message_id(message_id=message_id)
    
    message = await req_builder.get()
    try:
        message_info = await process_message(message)
    except ValueError as v:
        logging.error(v)
        return
    
    attachments_pairs: List[Dict[str,Any]] = await upload_to_blob(message_info=message_info, req_builder=req_builder)

    request_body: ReplyPostRequestBody = await return_mail(attachment_pairs=attachments_pairs, message_info=message_info)

    try:
        await req_builder.reply.post(body=request_body)
        logging.info("Reply Posted!")
    except Exception as e:
        logging.error(f"Failed to post reply: {e}")
        return

async def process_message(message: Message) -> dict:
    
    if message.has_attachments:
        message_info = {
            "message": message,
            "id": message.id,
            "conversation_id": message.conversation_id,
            "sender": message.sender.email_address.address,
            "cc": message.cc_recipients,
            "body": message.body,
            "subject": message.subject,
        }
        return message_info
    else:
        raise ValueError("Message does not have attachments, skipping processing...")

async def upload_to_blob(message_info: dict, req_builder: MessageItemRequestBuilder):
    attachment_return: AttachmentCollectionResponse = await req_builder.attachments.get()

    attachments: List[Dict[str,Any]] = [] 
    for attach in attachment_return.value:
        if not isinstance(attach, FileAttachment):
            raise ValueError("ItemAttachment found: FileAttachment needed.")
        if not attach.content_type == "application/pdf":
            raise ValueError("Content Type is not PDF")
        attachment = {
            "name": attach.name,
            "bytes": attach.content_bytes
            }
        attachments.append(attachment)
        
    sender: str = message_info.get("sender")

    container_name_clean = sender.lower().split("@")[0]
    invoice_container_name=f"{container_name_clean}-invoices-raw"
    cr_container_name=f"{container_name_clean}-checkrequests-raw"
    conn_str = os.environ["BLOB_CONNECTION_STRING"]

    blob_sv_client = BlobServiceClient.from_connection_string(conn_str=conn_str)

    invoice_container = blob_sv_client.get_container_client(invoice_container_name)
    cr_container = blob_sv_client.get_container_client(cr_container_name)

    test = [invoice_container, cr_container]
    for i, container in enumerate(test):
        try:
            container.create_container()
        except ResourceExistsError:
            pass

    attachment_pairs = []
    for pdf in attachments:
        inv_name = pdf.get("name")
        b64_inv_bytes: bytes = pdf.get("bytes")
        inv_bytes: bytes = base64.b64decode(b64_inv_bytes)

        logging.warning(f"PDF Raw Bytes: {inv_bytes}")

        ai_results = get_invoice_fields(inv_bytes)
        invoice_info = await process_ai_results(ai_results)

        invoice_fields: dict = invoice_info.get("invoice_fields")
        table_fields: list = invoice_info.get("table_fields")

        invoice_id = invoice_fields.get("InvoiceId")
        vendor_name = invoice_fields.get("VendorName")
        invoice_blob_name = f"invoice_{inv_name}.pdf"
        cr_blob_name = f"check_request_{inv_name}.pdf"

        check_rq_buffer = BytesIO()
        writer: PdfWriter = fillout_form(invoice_fields=invoice_fields, table_fields=table_fields)
        writer.write(check_rq_buffer)
        check_rq_buffer.seek(0)
        cr_bytes: bytes = check_rq_buffer.getvalue()
        
        pair = {
            "invoice_id": invoice_id,
            "vendor_name": vendor_name,
            "invoice": {
                "bytes": inv_bytes,
                "blob_name": invoice_blob_name,
            },
            "check_request": {
                "bytes": cr_bytes,
                "blob_name": cr_blob_name,
            }
        }

        attachment_pairs.append(pair)
        
        invoice_blob_client = invoice_container.get_blob_client(invoice_blob_name)
        checkrequest_blob_client = cr_container.get_blob_client(cr_blob_name)

        upload_list = [
            {
                "blob_client": invoice_blob_client,
                "container": invoice_container,
                "bytes": inv_bytes
            },
            {
                "blob_client": checkrequest_blob_client,
                "container": cr_container,
                "bytes": cr_bytes
            }
        ]

        for upload in upload_list:
            blob_client: BlobClient = upload.get("blob_client")
            container_client: ContainerClient = upload.get("container")
            r_bytes: bytes = upload.get("bytes")
            try:
                blob_client.upload_blob(r_bytes, overwrite=True)
            except ResourceNotFoundError:
                container_client.create_container()
                blob_client.upload_blob(r_bytes, overwrite=True)

    return attachment_pairs

async def return_mail(attachment_pairs: List[Dict[str,Any]], message_info: dict):
    attachments: List[Attachment] = []
    for pair in attachment_pairs:
        invoice_obj: Dict = pair.get("invoice")
        inv_name = invoice_obj.get("blob_name")
        inv_bytes: bytes = invoice_obj.get("bytes")

        cr_obj: Dict = pair.get("check_request")
        cr_name = cr_obj.get("blob_name")
        cr_bytes: bytes = cr_obj.get("bytes")

        inv_attachment = FileAttachment(
            odata_type= "#microsoft.graph.fileAttachment",
            name= inv_name,
            content_type= "application/pdf",
            content_bytes= inv_bytes
        )
        cr_attachment = FileAttachment(
            odata_type= "#microsoft.graph.fileAttachment",
            name= cr_name,
            content_type= "application/pdf",
            content_bytes= cr_bytes
        )
        attachments.append(inv_attachment)
        attachments.append(cr_attachment)

    request_body = ReplyPostRequestBody(
        message = Message(
            subject = f"Check Request(s): {message_info.get("subject")}",
            body = ItemBody(
                content_type= BodyType.Text,
                content = "Please review the Check Request for Approval."
            ),
            to_recipients= [
                Recipient(
                    email_address = EmailAddress(
                        address = message_info.get("sender")
                    ),
                ),
            ],
            attachments = attachments,
            has_attachments=True
        )
    )

    return request_body


# @app.queue_trigger(arg_name="azqueue", queue_name="invoices",
#                                connection="8043d5_STORAGE") 
# def InvoiceTrigger(azqueue: func.QueueMessage):
#     logging.info('Python Queue trigger processed a message: %s',
#                 azqueue.get_body().decode('utf-8'))        