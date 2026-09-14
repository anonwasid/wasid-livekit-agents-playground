"""LiveKit Webhook Receiver for Automated Telephony Call Recording.

Listens for:
  - room_started / participant_joined: Auto-starts Cloudflare R2 audio recording.
  - egress_ended: Finalizes recording metadata (duration, file size, status).
  - room_finished: Reconciles call lifecycles.
"""

import datetime
import json
import logging
import os
import uuid
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from livekit import api

from app.services.db import telephony_db
from app.services.livekit import LiveKitClient, get_livekit_client
from app.services.storage_r2 import storage_r2

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize Webhook Receiver with LiveKit credentials
_receiver: Optional[api.WebhookReceiver] = None


def get_webhook_receiver() -> api.WebhookReceiver:
    global _receiver
    if _receiver is None:
        api_key = os.getenv("LIVEKIT_API_KEY", "")
        api_secret = os.getenv("LIVEKIT_API_SECRET", "")
        if not api_key or not api_secret:
            logger.warning("LIVEKIT_API_KEY or LIVEKIT_API_SECRET missing for webhook verification")
        verifier = api.TokenVerifier(api_key, api_secret)
        _receiver = api.WebhookReceiver(verifier)
    return _receiver


@router.post("/api/webhooks/livekit")
async def livekit_webhook(
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """Receive and process real-time webhooks from LiveKit Server."""
    body_bytes = await request.body()
    body_str = body_bytes.decode("utf-8")

    # Validate signature if authorization header is provided
    receiver = get_webhook_receiver()
    try:
        if authorization:
            event = receiver.receive(body_str, authorization)
        else:
            # Fallback for internal direct calls without authorization header
            raw_data = json.loads(body_str)
            event_type = raw_data.get("event")
            logger.warning("LiveKit webhook received without authorization header: %s", event_type)
            # Create a mock or wrap raw dict
            return Response(status_code=200, content="OK (unverified)")
    except Exception as e:
        logger.warning("Webhook verification failed: %s", e)
        # In case of local/internal network delivery with relaxed verification
        try:
            raw_data = json.loads(body_str)
            logger.info("Proceeding with payload parsing for event: %s", raw_data.get("event"))
            return await process_raw_webhook(raw_data)
        except Exception:
            raise HTTPException(status_code=401, detail=f"Invalid webhook signature: {e}")

    return await process_verified_event(event)


async def auto_start_room_recording(room_name: str, lk: LiveKitClient) -> Optional[str]:
    """Start audio-only room composite egress recording to Cloudflare R2 for a call room."""
    is_inbound = room_name.startswith("sip-in-")
    is_outbound = room_name.startswith("call-out-") or room_name.startswith("sip-out-")
    if not (is_inbound or is_outbound):
        return None

    # Check if a recording already exists or is active for this room
    existing = await telephony_db.get_recording_by_room(room_name)
    if existing and existing.get("status") in ("recording", "completed"):
        logger.debug("Recording already exists for room %s (id: %s)", room_name, existing.get("recording_id"))
        return existing.get("recording_id")

    direction = "inbound" if is_inbound else "outbound"
    now = datetime.datetime.now(datetime.timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    call_rand = uuid.uuid4().hex[:8]
    rec_id = f"rec_{now.strftime('%Y%m%d')}_{direction}_{call_rand}"
    object_key = f"recordings/{date_path}/{direction}/{rec_id}.m4a"

    # Extract phone numbers from room name if formatted (e.g., sip-in-+918065355408_...)
    caller = ""
    callee = ""
    parts = room_name.split("-")
    if len(parts) >= 3:
        potential_num = parts[2].split("_")[0]
        if potential_num.startswith("+") or potential_num.isdigit():
            if is_inbound:
                caller = potential_num
                callee = "+918065355408"
            else:
                caller = "+918065355408"
                callee = potential_num

    # 1. Create recording record in PostgreSQL
    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=room_name,
        direction=direction,
        caller_number=caller,
        callee_number=callee,
        did_number="+918065355408",
        tenant_id="wasid-hq",
        agent_id="wasid-ai-automation-master",
        status="recording",
        storage_bucket=storage_r2.bucket,
        storage_object_key=object_key,
    )
    logger.info("Created call recording record %s for room %s", rec_id, room_name)

    # 2. Trigger LiveKit Egress recording
    try:
        egress_res = await lk.start_room_composite_egress(
            room_name=room_name,
            output_filename=object_key,
            layout="",
            audio_only=True,
            video_only=False,
            s3_bucket=storage_r2.bucket,
            s3_endpoint=storage_r2.endpoint,
            s3_access_key=storage_r2.access_key,
            s3_secret=storage_r2.secret_key,
            s3_region=storage_r2.region,
        )
        egress_id = getattr(egress_res, "egress_id", "") or ""
        logger.info("Successfully started Egress %s for room %s -> R2: %s", egress_id, room_name, object_key)

        # Update record with egress_id
        if egress_id:
            await telephony_db.update_recording(rec_id, egress_id=egress_id)
        return rec_id
    except Exception as e:
        logger.error("Failed to start egress recording for room %s: %s", room_name, e)
        await telephony_db.update_recording(
            rec_id,
            status="failed",
            error_message=str(e),
        )
        return rec_id


async def process_verified_event(event) -> Response:
    """Handle verified LiveKit webhook events."""
    event_name = getattr(event, "event", "") or ""
    logger.info("Processing LiveKit webhook event: %s", event_name)
    lk = get_livekit_client()

    if event_name in ("room_started", "participant_joined"):
        room = getattr(event, "room", None)
        rname = getattr(room, "name", "") if room else ""
        if rname:
            await auto_start_room_recording(rname, lk)

    elif event_name == "egress_ended":
        info = getattr(event, "egress_info", None)
        if info:
            egress_id = getattr(info, "egress_id", "")
            rname = getattr(info, "room_name", "")
            status_num = getattr(info, "status", 0)
            # EGRESS_COMPLETE = 3
            is_complete = status_num == api.EgressStatus.EGRESS_COMPLETE or "COMPLETE" in str(status_num)
            status_str = "completed" if is_complete else "failed"

            duration_secs = 0
            if getattr(info, "started_at", 0) and getattr(info, "ended_at", 0):
                duration_secs = int((info.ended_at - info.started_at) / 1e9)

            file_size = 0
            file_key = None
            file_results = getattr(info, "file_results", [])
            if file_results:
                f0 = file_results[0]
                file_size = getattr(f0, "size", 0)
                file_key = getattr(f0, "filename", None)

            rec = None
            if egress_id:
                rec = await telephony_db.get_recording_by_egress_id(egress_id)
            if not rec and rname:
                rec = await telephony_db.get_recording_by_room(rname)

            if rec:
                rec_id = rec["recording_id"]
                await telephony_db.update_recording(
                    recording_id=rec_id,
                    status=status_str,
                    duration_seconds=duration_secs,
                    file_size_bytes=file_size,
                    storage_object_key=file_key,
                    error_message=getattr(info, "error", None),
                )
                logger.info("Finalized recording %s (egress %s): status=%s, duration=%ds, size=%d bytes",
                            rec_id, egress_id, status_str, duration_secs, file_size)

    return Response(status_code=200, content="OK")


async def process_raw_webhook(data: dict) -> Response:
    """Handle raw JSON webhook payloads when signatures are bypassed or in dev mode."""
    event_name = data.get("event", "")
    lk = get_livekit_client()

    if event_name in ("room_started", "participant_joined"):
        room = data.get("room", {})
        rname = room.get("name", "")
        if rname:
            await auto_start_room_recording(rname, lk)

    elif event_name == "egress_ended":
        info = data.get("egress_info", {})
        egress_id = info.get("egress_id", "")
        rname = info.get("room_name", "")
        status_val = info.get("status", "")
        is_complete = "COMPLETE" in str(status_val) or status_val == 3
        status_str = "completed" if is_complete else "failed"

        duration_secs = 0
        started = info.get("started_at", 0)
        ended = info.get("ended_at", 0)
        if started and ended:
            duration_secs = int((int(ended) - int(started)) / 1e9)

        file_size = 0
        file_key = None
        file_results = info.get("file_results", [])
        if file_results:
            f0 = file_results[0]
            file_size = f0.get("size", 0)
            file_key = f0.get("filename", None)

        rec = None
        if egress_id:
            rec = await telephony_db.get_recording_by_egress_id(egress_id)
        if not rec and rname:
            rec = await telephony_db.get_recording_by_room(rname)

        if rec:
            rec_id = rec["recording_id"]
            await telephony_db.update_recording(
                recording_id=rec_id,
                status=status_str,
                duration_seconds=duration_secs,
                file_size_bytes=file_size,
                storage_object_key=file_key,
                error_message=info.get("error"),
            )

    return Response(status_code=200, content="OK (raw)")
