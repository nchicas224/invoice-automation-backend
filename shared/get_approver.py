import logging
from shared.azure_monitor import initialize_logger
from shared.database_client import get_db_client
from shared.graph_client import get_graph_client
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError


async def get_user_approver(userId:str, tenantId:str, resolved_time:str):
    initialize_logger()
    db_client = get_db_client()
    cosmos_profiles_container = db_client.get_container_client("UserProfiles")

    try:
        approver = cosmos_profiles_container.read_item(item="profile", partition_key=[tenantId, userId])
        return {"status": "ok", "cache":"hit", "approver":approver.get("approver")}
    except CosmosResourceNotFoundError as e:
        logging.info("Missed Cache, running Graph API call...")

    graph_client = await get_graph_client()
    user_call =  graph_client.users.by_user_id(user_id=userId)
    query_params = user_call.UserItemRequestBuilderGetQueryParameters(select=["customSecurityAttributes"])
    req_config = user_call.UserItemRequestBuilderGetRequestConfiguration(query_parameters=query_params)
    user_obj = await user_call.get(request_configuration=req_config)
    
    csa = getattr(user_obj, "custom_security_attributes", None)
    if not csa:
        csa = (getattr(user_obj, "additional_data", {}) or {}).get("customSecurityAttributes")
        logging.info("CSA grabbed from additional data, missing from user_obj standalone attribute")

    logging.info(f"CSA: {csa}")
    invoice_p = (csa or {}).get("Invoice") or {}
    approver_upn = invoice_p.get("Approver") or None

    approver_obj = {
            "upn": approver_upn,
            "displayName": userId.lower().split("@")[0]
        } if approver_upn else "Approver Not Found"

    payload = {
        "id": "profile",
        "tenantId": tenantId,
        "userId": userId,
        "approver": approver_obj,
        "source": "MsGraph Call",
        "resolvedAtUtc": resolved_time
    }

    try:
        cosmos_profiles_container.create_item(payload)
        logging.info("Item successfully created by this worker, returning.")
        return {"status": "ok", "cache":"miss", "approver":approver_upn}
    except CosmosHttpResponseError as e:
        if e.status_code == 409:
            logging.info("UserProfiles race-won-by-peer, returning.")
            approver = cosmos_profiles_container.read_item(item="profile", partition_key=[tenantId, userId])
            return {"status": "ok", "cache":"raced", "approver":approver.get("approver")}
        raise