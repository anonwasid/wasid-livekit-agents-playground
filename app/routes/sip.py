"""SIP telephony routes"""

import json
import logging
import os
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Optional, Dict, Any
from urllib.parse import quote
from pydantic import BaseModel, Field

from app.services.livekit import LiveKitClient, get_livekit_client
from app.security.basic_auth import requires_admin, get_current_user
from app.security.csrf import get_csrf_token, verify_csrf_token

logger = logging.getLogger(__name__)

router = APIRouter()


class OutboundCallPayload(BaseModel):
    sip_trunk_id: str = Field(default="ST_7DxmGrQdRtgT")
    sip_call_to: Optional[str] = None
    target_phone: Optional[str] = None
    lead_id: Optional[str] = None
    contact_id: Optional[str] = None
    customer_id: Optional[str] = None
    caller_did: Optional[str] = None
    agent_name: Optional[str] = None
    tenant_id: str = Field(default="wasid-hq")
    is_admin_override: bool = False
    voice_mode: str = Field(default="realtime")
    call_context: Optional[Dict[str, Any]] = None


@router.post("/api/v1/sip/outbound-call")
async def api_create_outbound_sip_call(
    payload: OutboundCallPayload,
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Programmatic API to dispatch outbound SIP call with server-side target ownership validation and deduplication."""
    if not lk.sip_enabled:
        raise HTTPException(status_code=503, detail="LiveKit SIP service is not enabled")

    # 1. Resolve tenant_id
    if not payload.tenant_id:
        raise HTTPException(status_code=400, detail="Missing required tenant_id")
    tenant_id = payload.tenant_id.strip()

    # 2. Resolve destination phone
    destination = (payload.sip_call_to or payload.target_phone or "").strip()

    from app.services.db import telephony_db

    # 3. Concurrency-Safe Deduplication Check: reject if active call exists for same tenant and destination/target
    active_call = await telephony_db.check_active_call_exists(
        tenant_id=tenant_id,
        destination=destination,
        lead_id=payload.lead_id or payload.customer_id,
    )
    if active_call:
        logger.warning(
            "Duplicate outbound call rejected for tenant '%s' and destination '%s' (Existing call_id: %s, status: %s)",
            tenant_id, destination, active_call.get("call_id"), active_call.get("status")
        )
        raise HTTPException(
            status_code=409,
            detail=f"DUPLICATE_ACTIVE_CALL: An active call already exists for destination '{destination}' under tenant '{tenant_id}' (call_id: {active_call.get('call_id')})."
        )

    # 4. Server-Side Target Ownership Validation
    ownership = await telephony_db.verify_target_ownership(
        tenant_id=tenant_id,
        phone=destination,
        lead_id=payload.lead_id,
        contact_id=payload.contact_id,
        customer_id=payload.customer_id,
        is_admin_override=payload.is_admin_override,
    )

    if not ownership.get("allowed"):
        err_status = ownership.get("status")
        err_msg = ownership.get("error", "Outbound target validation failed")
        logger.warning("Outbound call blocked by target ownership validation: %s", err_msg)
        if err_status in ("tenant_mismatch", "cross_tenant_conflict"):
            raise HTTPException(status_code=403, detail=err_msg)
        else:
            raise HTTPException(status_code=400, detail=err_msg)

    # Update destination from validated database record if resolved
    if ownership.get("phone"):
        destination = ownership["phone"]

    # 5. Outbound Caller ID / DID Selection
    if payload.caller_did:
        caller_did = payload.caller_did.strip()
    else:
        assigned_did = await telephony_db.get_tenant_outbound_did(tenant_id)
        caller_did = assigned_did or "+918065355408"

    # 6. Agent Selection
    from app.services.sip_routing import sip_routing_service, CANONICAL_MASTER_AGENT, CANONICAL_CUSTOMER_AGENT
    if payload.agent_name:
        target_agent = payload.agent_name.strip()
    else:
        target_agent = CANONICAL_MASTER_AGENT if tenant_id in ("wasid-hq", "WAS12345678", "ADMIN") else CANONICAL_CUSTOMER_AGENT

    # 7. Merge trusted outbound context & verification state
    enriched_context: Dict[str, Any] = dict(payload.call_context or {})
    enriched_context.update({
        "direction": "outbound",
        "call_direction": "OUTBOUND",
        "verification_state": "OUTBOUND_VERIFIED",
        "tenant_id": tenant_id,
        "customer_name": ownership.get("customer_name") or enriched_context.get("customer_name") or "",
        "target_type": ownership.get("target_type") or "contact",
        "lead_id": payload.lead_id or ownership.get("target_id") or enriched_context.get("lead_id") or "",
        "contact_id": payload.contact_id or enriched_context.get("contact_id") or "",
    })

    try:
        res = await sip_routing_service.initiate_outbound_call(
            lk=lk,
            sip_trunk_id=payload.sip_trunk_id.strip(),
            sip_call_to=destination,
            agent_name=target_agent,
            tenant_id=tenant_id,
            caller_did=caller_did,
            voice_mode=payload.voice_mode.strip().lower() if payload.voice_mode else "realtime",
            call_context=enriched_context,
        )
        return {
            "success": True,
            "call_id": res["call_id"],
            "room_name": res["room_name"],
            "caller_did": caller_did,
            "callee": destination,
            "agent_name": target_agent,
            "tenant_id": tenant_id,
            "verification_state": "OUTBOUND_VERIFIED",
            "voice_mode": payload.voice_mode,
            "message": f"Outbound call placed to {destination} in room '{res['room_name']}' with caller DID {caller_did}."
        }
    except Exception as e:
        logger.error("Failed to place outbound SIP call via API: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to place outbound call: {str(e)}")


@router.get("/api/v1/sip/call-context")
async def api_get_call_context(
    room_name: Optional[str] = None,
    call_id: Optional[str] = None,
):
    """Retrieve structured call & lead context for agent runtime fallback."""
    from app.services.db import telephony_db
    target_id = room_name or call_id
    if not target_id:
        raise HTTPException(status_code=400, detail="Must provide room_name or call_id")

    ctx = await telephony_db.get_call_context(target_id)
    if not ctx.get("found"):
        raise HTTPException(status_code=404, detail=ctx.get("error", "Call context not found"))
    return ctx


@router.get("/api/v1/sip/caller-lookup")
async def api_lookup_caller(
    phone: str,
):
    """Identify if incoming caller is an existing lead or registered customer."""
    from app.services.db import telephony_db
    if not phone:
        raise HTTPException(status_code=400, detail="Phone number is required")

    return await telephony_db.lookup_caller(phone)



@router.get("/sip-outbound", response_class=HTMLResponse, dependencies=[Depends(requires_admin)])
async def sip_outbound_index(
    request: Request,
    lk: LiveKitClient = Depends(get_livekit_client),
    flash_message: Optional[str] = None,
    flash_type: Optional[str] = None,
):
    """SIP outbound calls page"""
    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    trunks = await lk.list_sip_trunks()
    current_user = get_current_user(request)
    from app.services.sip_routing import sip_routing_service
    routing_matrix = await sip_routing_service.get_voice_routing_matrix(lk)

    return request.app.state.templates.TemplateResponse(request, 
        "sip/outbound.html.j2",
        {
            "request": request,
            "trunks": trunks,
            "routing_matrix": routing_matrix,
            "current_user": current_user,
            "csrf_token": get_csrf_token(request),
            "flash_message": flash_message,
            "flash_type": flash_type,
        },
    )


@router.post("/sip-outbound/create", dependencies=[Depends(requires_admin)])
async def create_sip_call(
    request: Request,
    csrf_token: str = Form(...),
    sip_trunk_id: str = Form(...),
    sip_call_to: str = Form(...),
    room_name: Optional[str] = Form(None),
    participant_identity: Optional[str] = Form(None),
    agent_name: Optional[str] = Form("wasid-ai-automation-master"),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Create an outbound SIP call enforcing unique room and agent dispatch."""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        from app.services.sip_routing import sip_routing_service, CANONICAL_MASTER_AGENT
        target_agent = agent_name.strip() if agent_name else CANONICAL_MASTER_AGENT
        clean_room = room_name.strip() if room_name else None

        if not clean_room:
            res = await sip_routing_service.initiate_outbound_call(
                lk=lk,
                sip_trunk_id=sip_trunk_id.strip(),
                sip_call_to=sip_call_to.strip(),
                agent_name=target_agent,
                participant_identity=participant_identity.strip() if participant_identity else None,
            )
            success_msg = quote(f"Outbound call placed to {sip_call_to} in unique room '{res['room_name']}' with agent '{target_agent}'.")
            return RedirectResponse(url=f"/sip-outbound?flash_message={success_msg}&flash_type=success", status_code=303)
        else:
            try:
                await lk.create_dispatch(agent_name=target_agent, room=clean_room)
            except Exception:
                pass
            await lk.create_sip_participant(
                sip_trunk_id=sip_trunk_id.strip(),
                sip_call_to=sip_call_to.strip(),
                room_name=clean_room,
                participant_identity=participant_identity.strip() if participant_identity else f"sip-{sip_call_to}",
            )
            success_msg = quote(f"Outbound call placed to {sip_call_to} in room '{clean_room}' with agent '{target_agent}'.")
            return RedirectResponse(url=f"/sip-outbound?flash_message={success_msg}&flash_type=success", status_code=303)
    except Exception as e:
        logger.warning("Error creating SIP call: %s", e)
        encoded_error = quote(f"Failed to place outbound call: {str(e)}")
        return RedirectResponse(url=f"/sip-outbound?flash_message={encoded_error}&flash_type=danger", status_code=303)


@router.post("/sip-outbound/trunk/create", dependencies=[Depends(requires_admin)])
async def create_sip_trunk(
    request: Request,
    csrf_token: str = Form(...),
    trunk_name: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    transport: Optional[str] = Form(None),
    numbers: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    destination_country: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    headers: Optional[str] = Form(None),
    headers_to_attributes: Optional[str] = Form(None),
    json_data: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Create a new SIP outbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # If JSON editor was used, extract fields from it
        if json_data and json_data.strip():
            try:
                jd = json.loads(json_data)
                trunk_name = jd.get("name", trunk_name)
                address = jd.get("address", address)
                transport = jd.get("transport", transport)
                raw_numbers = jd.get("numbers", None)
                if raw_numbers is not None:
                    numbers = ",".join(raw_numbers) if isinstance(raw_numbers, list) else raw_numbers
                username = jd.get("auth_username", username)
                password = jd.get("auth_password", password)
                destination_country = jd.get("destination_country", destination_country)
                metadata = jd.get("metadata", metadata)
                h = jd.get("headers", None)
                if h is not None:
                    headers = json.dumps(h)
                h2a = jd.get("headers_to_attributes", None)
                if h2a is not None:
                    headers_to_attributes = json.dumps(h2a)
            except json.JSONDecodeError:
                pass

        # Parse numbers if provided
        numbers_list = None
        if numbers:
            numbers_list = [n.strip() for n in numbers.split(",") if n.strip()]

        # Parse JSON fields
        headers_dict = None
        if headers:
            try:
                headers_dict = json.loads(headers)
            except json.JSONDecodeError:
                pass

        headers_to_attrs_dict = None
        if headers_to_attributes:
            try:
                headers_to_attrs_dict = json.loads(headers_to_attributes)
            except json.JSONDecodeError:
                pass

        # Carrier outbound SIP proxy default (Vobiz carrier / LiveKit requirement)
        resolved_address = (address.strip() if address else None) or os.getenv("VOBIZ_SIP_OUTBOUND_ADDRESS", "sip.vobiz.com")

        result = await lk.create_sip_trunk(
            name=trunk_name,
            address=resolved_address,
            transport=transport,
            numbers=numbers_list,
            auth_username=username,
            auth_password=password,
            destination_country=destination_country,
            metadata=metadata,
            headers=headers_dict,
            headers_to_attributes=headers_to_attrs_dict,
        )

        # Success message
        trunk_display_name = trunk_name or "Trunk"
        success_msg = quote(f"Successfully created trunk: {trunk_display_name}")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error creating SIP trunk: %s", e)
        import traceback

        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to create trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-outbound/trunk/update", dependencies=[Depends(requires_admin)])
async def update_sip_trunk(
    request: Request,
    csrf_token: str = Form(...),
    sip_trunk_id: str = Form(...),
    trunk_name: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    transport: Optional[str] = Form(None),
    numbers: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    destination_country: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    headers: Optional[str] = Form(None),
    headers_to_attributes: Optional[str] = Form(None),
    json_data: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Update an existing SIP outbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # If JSON editor was used, extract fields from it
        if json_data and json_data.strip():
            try:
                jd = json.loads(json_data)
                trunk_name = jd.get("name", trunk_name)
                address = jd.get("address", address)
                transport = jd.get("transport", transport)
                raw_numbers = jd.get("numbers", None)
                if raw_numbers is not None:
                    numbers = ",".join(raw_numbers) if isinstance(raw_numbers, list) else raw_numbers
                username = jd.get("auth_username", username)
                password = jd.get("auth_password", password)
                destination_country = jd.get("destination_country", destination_country)
                metadata = jd.get("metadata", metadata)
                h = jd.get("headers", None)
                if h is not None:
                    headers = json.dumps(h)
                h2a = jd.get("headers_to_attributes", None)
                if h2a is not None:
                    headers_to_attributes = json.dumps(h2a)
            except json.JSONDecodeError:
                pass

        # Parse numbers if provided
        numbers_list = None
        if numbers:
            numbers_list = [n.strip() for n in numbers.split(",") if n.strip()]

        # Parse JSON fields
        headers_dict = None
        if headers:
            try:
                headers_dict = json.loads(headers)
            except json.JSONDecodeError:
                pass

        headers_to_attrs_dict = None
        if headers_to_attributes:
            try:
                headers_to_attrs_dict = json.loads(headers_to_attributes)
            except json.JSONDecodeError:
                pass

        logger.debug(
            "Updating trunk %s: name=%s address=%s transport=%s numbers=%s username=%s country=%s metadata=%s",
            sip_trunk_id, trunk_name, address, transport, numbers_list, username, destination_country, metadata,
        )

        await lk.update_sip_trunk(
            sip_trunk_id=sip_trunk_id,
            name=trunk_name,
            address=address,
            transport=transport,
            numbers=numbers_list,
            auth_username=username,
            auth_password=password if password else None,
            destination_country=destination_country,
            metadata=metadata,
            headers=headers_dict,
            headers_to_attributes=headers_to_attrs_dict,
        )

        # Success message
        trunk_display_name = trunk_name or sip_trunk_id[:16]
        success_msg = quote(f"Successfully updated trunk: {trunk_display_name}")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error updating SIP trunk: %s", e)
        import traceback

        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to update trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-outbound/trunk/delete", dependencies=[Depends(requires_admin)])
async def delete_sip_trunk(
    request: Request,
    csrf_token: str = Form(...),
    sip_trunk_id: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Delete a SIP outbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        await lk.delete_sip_trunk(sip_trunk_id=sip_trunk_id)

        # Success message
        trunk_id_short = sip_trunk_id[:16] if len(sip_trunk_id) > 16 else sip_trunk_id
        success_msg = quote(f"Successfully deleted trunk: {trunk_id_short}...")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error deleting SIP trunk: %s", e)
        import traceback

        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to delete trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-outbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.get("/sip-inbound", response_class=HTMLResponse, dependencies=[Depends(requires_admin)])
async def sip_inbound_index(
    request: Request,
    lk: LiveKitClient = Depends(get_livekit_client),
    flash_message: Optional[str] = None,
    flash_type: Optional[str] = None,
):
    """SIP inbound rules page"""
    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    rules = await lk.list_sip_dispatch_rules()
    trunks = await lk.list_sip_inbound_trunks()
    current_user = get_current_user(request)
    from app.services.sip_routing import sip_routing_service
    routing_matrix = await sip_routing_service.get_voice_routing_matrix(lk)

    return request.app.state.templates.TemplateResponse(request, 
        "sip/inbound.html.j2",
        {
            "request": request,
            "rules": rules,
            "trunks": trunks,
            "routing_matrix": routing_matrix,
            "current_user": current_user,
            "csrf_token": get_csrf_token(request),
            "flash_message": flash_message,
            "flash_type": flash_type,
        },
    )


@router.post("/sip-inbound/trunk/create", dependencies=[Depends(requires_admin)])
async def create_sip_inbound_trunk(
    request: Request,
    csrf_token: str = Form(...),
    trunk_name: Optional[str] = Form(None),
    numbers: Optional[str] = Form(None),
    allowed_addresses: Optional[str] = Form(None),
    allowed_numbers: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    headers_to_attributes: Optional[str] = Form(None),
    attributes_to_headers: Optional[str] = Form(None),
    include_headers: Optional[str] = Form(None),
    ringing_timeout: Optional[str] = Form(None),
    max_call_duration: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Create a new SIP inbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # Parse comma-separated lists
        numbers_list = None
        if numbers:
            numbers_list = [n.strip() for n in numbers.split(",") if n.strip()]

        allowed_addresses_list = None
        if allowed_addresses:
            allowed_addresses_list = [a.strip() for a in allowed_addresses.split(",") if a.strip()]

        allowed_numbers_list = None
        if allowed_numbers:
            allowed_numbers_list = [n.strip() for n in allowed_numbers.split(",") if n.strip()]

        headers_to_attrs_dict = None
        if headers_to_attributes:
            try:
                headers_to_attrs_dict = json.loads(headers_to_attributes)
            except json.JSONDecodeError:
                pass

        attrs_to_headers_dict = None
        if attributes_to_headers:
            try:
                attrs_to_headers_dict = json.loads(attributes_to_headers)
            except json.JSONDecodeError:
                pass

        result = await lk.create_sip_inbound_trunk(
            name=trunk_name,
            numbers=numbers_list,
            allowed_addresses=allowed_addresses_list,
            allowed_numbers=allowed_numbers_list,
            auth_username=username,
            auth_password=password,
            metadata=metadata,
            headers_to_attributes=headers_to_attrs_dict,
            attributes_to_headers=attrs_to_headers_dict,
            include_headers=int(include_headers) if include_headers else None,
            ringing_timeout=int(ringing_timeout) if ringing_timeout else None,
            max_call_duration=int(max_call_duration) if max_call_duration else None,
        )

        # Success message
        trunk_display_name = trunk_name or "Inbound Trunk"
        success_msg = quote(f"Successfully created inbound trunk: {trunk_display_name}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error creating SIP inbound trunk: %s", e)
        import traceback
        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to create inbound trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/trunk/update", dependencies=[Depends(requires_admin)])
async def update_sip_inbound_trunk(
    request: Request,
    csrf_token: str = Form(...),
    sip_trunk_id: str = Form(...),
    trunk_name: Optional[str] = Form(None),
    numbers: Optional[str] = Form(None),
    allowed_addresses: Optional[str] = Form(None),
    allowed_numbers: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    headers_to_attributes: Optional[str] = Form(None),
    attributes_to_headers: Optional[str] = Form(None),
    include_headers: Optional[str] = Form(None),
    ringing_timeout: Optional[str] = Form(None),
    max_call_duration: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Update an existing SIP inbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # Parse comma-separated lists
        numbers_list = None
        if numbers:
            numbers_list = [n.strip() for n in numbers.split(",") if n.strip()]

        allowed_addresses_list = None
        if allowed_addresses:
            allowed_addresses_list = [a.strip() for a in allowed_addresses.split(",") if a.strip()]

        allowed_numbers_list = None
        if allowed_numbers:
            allowed_numbers_list = [n.strip() for n in allowed_numbers.split(",") if n.strip()]

        headers_to_attrs_dict = None
        if headers_to_attributes:
            try:
                headers_to_attrs_dict = json.loads(headers_to_attributes)
            except json.JSONDecodeError:
                pass

        attrs_to_headers_dict = None
        if attributes_to_headers:
            try:
                attrs_to_headers_dict = json.loads(attributes_to_headers)
            except json.JSONDecodeError:
                pass

        await lk.update_sip_inbound_trunk(
            sip_trunk_id=sip_trunk_id,
            name=trunk_name,
            numbers=numbers_list,
            allowed_addresses=allowed_addresses_list,
            allowed_numbers=allowed_numbers_list,
            auth_username=username,
            auth_password=password if password else None,
            metadata=metadata,
            headers_to_attributes=headers_to_attrs_dict,
            attributes_to_headers=attrs_to_headers_dict,
            include_headers=int(include_headers) if include_headers else None,
            ringing_timeout=int(ringing_timeout) if ringing_timeout else None,
            max_call_duration=int(max_call_duration) if max_call_duration else None,
        )

        # Success message
        trunk_display_name = trunk_name or sip_trunk_id[:16]
        success_msg = quote(f"Successfully updated inbound trunk: {trunk_display_name}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error updating SIP inbound trunk: %s", e)
        import traceback
        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to update inbound trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/trunk/delete", dependencies=[Depends(requires_admin)])
async def delete_sip_inbound_trunk(
    request: Request,
    csrf_token: str = Form(...),
    sip_trunk_id: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Delete a SIP inbound trunk"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        await lk.delete_sip_trunk(sip_trunk_id=sip_trunk_id)

        # Success message
        trunk_id_short = sip_trunk_id[:16] if len(sip_trunk_id) > 16 else sip_trunk_id
        success_msg = quote(f"Successfully deleted inbound trunk: {trunk_id_short}...")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error deleting SIP inbound trunk: %s", e)
        import traceback
        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to delete inbound trunk: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/rule/create", dependencies=[Depends(requires_admin)])
async def create_dispatch_rule(
    request: Request,
    csrf_token: str = Form(...),
    rule_name: Optional[str] = Form(None),
    trunk_ids: Optional[str] = Form(None),
    dispatch_rule_type: str = Form("direct"),
    room_name: Optional[str] = Form(None),
    room_prefix: Optional[str] = Form(None),
    pin: Optional[str] = Form(None),
    randomize: bool = Form(False),
    hide_phone_number: bool = Form(False),
    agent_name: Optional[str] = Form(None),
    agent_metadata: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    plain_json: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Create a new SIP dispatch rule"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # Parse trunk IDs
        trunk_ids_list = None
        if trunk_ids:
            trunk_ids_list = [tid.strip() for tid in trunk_ids.split(",") if tid.strip()]

        result = await lk.create_sip_dispatch_rule(
            name=rule_name,
            trunk_ids=trunk_ids_list,
            dispatch_rule_type=dispatch_rule_type,
            room_name=room_name,
            room_prefix=room_prefix,
            pin=pin,
            randomize=randomize,
            hide_phone_number=hide_phone_number,
            agent_name=agent_name,
            agent_metadata=agent_metadata,
            metadata=metadata,
            plain_json=plain_json,
        )

        # Success message
        rule_display_name = rule_name or "Dispatch Rule"
        success_msg = quote(f"Successfully created dispatch rule: {rule_display_name}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        # Extract user-friendly error message
        error_msg = str(e)
        if hasattr(e, 'message'):
            error_msg = e.message
        elif hasattr(e, 'args') and e.args:
            error_msg = str(e.args[0])
        
        logger.warning("Error creating SIP dispatch rule: %s", e)
        import traceback
        traceback.print_exc()

        # Error message - make it more user-friendly
        if "cannot connect" in error_msg.lower() or "connectionkey" in error_msg.lower() or "connect call failed" in error_msg.lower():
            error_msg = f"Cannot connect to LiveKit server at {os.environ.get('LIVEKIT_URL', 'unknown')}"
        elif "missing rule" in error_msg.lower():
            error_msg = "Invalid dispatch rule configuration. Please check your settings."
        elif "invalid_argument" in error_msg.lower():
            error_msg = "Invalid configuration. Please verify all required fields are filled."
        
        encoded_error = quote(f"Failed to create dispatch rule: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/rule/update", dependencies=[Depends(requires_admin)])
async def update_dispatch_rule(
    request: Request,
    csrf_token: str = Form(...),
    sip_dispatch_rule_id: str = Form(...),
    rule_name: Optional[str] = Form(None),
    trunk_ids: Optional[str] = Form(None),
    dispatch_rule_type: Optional[str] = Form(None),
    room_name: Optional[str] = Form(None),
    room_prefix: Optional[str] = Form(None),
    pin: Optional[str] = Form(None),
    randomize: bool = Form(False),
    hide_phone_number: bool = Form(False),
    agent_name: Optional[str] = Form(None),
    agent_metadata: Optional[str] = Form(None),
    metadata: Optional[str] = Form(None),
    plain_json: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Update an existing SIP dispatch rule"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        # Parse trunk IDs
        trunk_ids_list = None
        if trunk_ids:
            trunk_ids_list = [tid.strip() for tid in trunk_ids.split(",") if tid.strip()]

        await lk.update_sip_dispatch_rule(
            sip_dispatch_rule_id=sip_dispatch_rule_id,
            name=rule_name,
            trunk_ids=trunk_ids_list,
            dispatch_rule_type=dispatch_rule_type,
            room_name=room_name,
            room_prefix=room_prefix,
            pin=pin,
            randomize=randomize,
            hide_phone_number=hide_phone_number,
            agent_name=agent_name,
            agent_metadata=agent_metadata,
            metadata=metadata,
            plain_json=plain_json,
        )

        # Success message
        rule_display_name = rule_name or sip_dispatch_rule_id[:16]
        success_msg = quote(f"Successfully updated dispatch rule: {rule_display_name}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error updating SIP dispatch rule: %s", e)
        import traceback
        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to update dispatch rule: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/rule/delete", dependencies=[Depends(requires_admin)])
async def delete_dispatch_rule(
    request: Request,
    csrf_token: str = Form(...),
    sip_dispatch_rule_id: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Delete a SIP dispatch rule"""
    await verify_csrf_token(request)

    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        await lk.delete_sip_dispatch_rule(sip_dispatch_rule_id=sip_dispatch_rule_id)

        # Success message
        rule_id_short = sip_dispatch_rule_id[:16] if len(sip_dispatch_rule_id) > 16 else sip_dispatch_rule_id
        success_msg = quote(f"Successfully deleted dispatch rule: {rule_id_short}...")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={success_msg}&flash_type=success", status_code=303
        )
    except Exception as e:
        error_msg = str(e)
        logger.warning("Error deleting SIP dispatch rule: %s", e)
        import traceback
        traceback.print_exc()

        # Error message
        encoded_error = quote(f"Failed to delete dispatch rule: {error_msg}")
        return RedirectResponse(
            url=f"/sip-inbound?flash_message={encoded_error}&flash_type=danger", status_code=303
        )


@router.post("/sip-inbound/provision-vobiz", dependencies=[Depends(requires_admin)])
async def provision_vobiz_inbound(
    request: Request,
    csrf_token: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Ensure canonical Vobiz individual dispatch rule is configured."""
    await verify_csrf_token(request)
    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)
    try:
        from app.services.sip_routing import sip_routing_service
        res = await sip_routing_service.provision_vobiz_canonical_rule(lk)
        msg = quote(f"Vobiz SIP Inbound Rule '{res['name']}' provisioned -> target: {res['agent_name']} ({res['room_strategy']}).")
        return RedirectResponse(url=f"/sip-inbound?flash_message={msg}&flash_type=success", status_code=303)
    except Exception as e:
        err = quote(f"Failed to provision Vobiz rule: {str(e)}")
        return RedirectResponse(url=f"/sip-inbound?flash_message={err}&flash_type=danger", status_code=303)


@router.post("/sip-inbound/did/assign", dependencies=[Depends(requires_admin)])
async def assign_did_routing(
    request: Request,
    csrf_token: str = Form(...),
    did: str = Form(...),
    agent_name: str = Form(...),
    tenant_id: Optional[str] = Form(None),
    tenant_name: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Assign an inbound DID to a canonical agent in PostgreSQL and sync to LiveKit dispatch rule."""
    await verify_csrf_token(request)
    if not lk.sip_enabled:
        return RedirectResponse(url="/", status_code=303)

    try:
        from app.services.sip_routing import sip_routing_service
        updated = await sip_routing_service.reassign_did_routing(
            lk=lk,
            did=did.strip(),
            agent_name=agent_name.strip(),
            tenant_id=tenant_id.strip() if tenant_id else None,
            tenant_name=tenant_name.strip() if tenant_name else None,
        )
        msg = quote(f"DID {did} successfully mapped to '{agent_name}' in PostgreSQL & LiveKit.")
        return RedirectResponse(url=f"/sip-inbound?flash_message={msg}&flash_type=success", status_code=303)
    except Exception as e:
        logger.warning("Error assigning DID: %s", e)
        err = quote(f"Failed to assign DID: {str(e)}")
        return RedirectResponse(url=f"/sip-inbound?flash_message={err}&flash_type=danger", status_code=303)
