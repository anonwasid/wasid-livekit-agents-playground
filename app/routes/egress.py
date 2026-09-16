"""Egress and Call Recording Routes for WASID LiveKit Platform."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse

from app.security.basic_auth import get_current_user, requires_admin
from app.security.csrf import get_csrf_token, verify_csrf_token
from app.services.db import telephony_db
from app.services.livekit import LiveKitClient, get_livekit_client
from app.services.storage_r2 import storage_r2

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/egress", response_class=HTMLResponse, dependencies=[Depends(requires_admin)])
async def egress_index(
    request: Request,
    partial: Optional[str] = None,
    direction: Optional[str] = None,
    status: Optional[str] = None,
    tenant: Optional[str] = None,
    did: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    limit: int = 30,
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """List call recordings and active LiveKit egress jobs."""
    try:
        active_egress_jobs = await lk.list_egress(active=True)
    except Exception as e:
        logger.debug("Failed to list active egress jobs from LiveKit API: %s", e)
        active_egress_jobs = []

    distinct_tenants = []
    distinct_dids = []
    try:
        distinct_tenants = await telephony_db.get_distinct_tenants()
        distinct_dids = await telephony_db.get_distinct_dids()
    except Exception as e:
        logger.warning("Failed to fetch distinct tenants/dids: %s", e)

    # Query persistent recordings database
    try:
        stats = await telephony_db.get_recording_stats()
        offset = max(0, (page - 1) * limit)
        recordings = await telephony_db.list_recordings(
            direction=direction,
            status=status,
            tenant_id=tenant,
            did=did,
            search=search,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        logger.error("Failed to query recordings from database: %s", e)
        stats = {
            "total_recordings": 0,
            "inbound_recordings": 0,
            "outbound_recordings": 0,
            "active_recordings": 0,
            "total_duration_seconds": 0,
            "total_file_size_bytes": 0,
        }
        recordings = []

    current_user = get_current_user(request)

    template_data = {
        "request": request,
        "recordings": recordings,
        "stats": stats,
        "active_egress_jobs": active_egress_jobs,
        "current_user": current_user,
        "csrf_token": get_csrf_token(request),
        "selected_direction": direction or "all",
        "selected_status": status or "all",
        "selected_tenant": tenant or "all",
        "selected_did": did or "all",
        "distinct_tenants": distinct_tenants,
        "distinct_dids": distinct_dids,
        "search_query": search or "",
        "page": page,
        "limit": limit,
    }

    template_name = "egress/index.html.j2"
    return request.app.state.templates.TemplateResponse(request, template_name, template_data)


from app.services.transcode import ensure_mp3_in_r2


@router.get("/egress/{recording_id}/stream", dependencies=[Depends(requires_admin)])
async def stream_recording(
    request: Request,
    recording_id: str,
):
    """Stream audio recording with HTTP Range support for scrubbing in HTML5 player."""
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    object_key = rec.get("storage_object_key")
    if not object_key:
        raise HTTPException(status_code=404, detail="Recording media file path not found")

    # Determine stream key and appropriate media_type
    stream_key = object_key
    media_type = "audio/mpeg" if object_key.endswith(".mp3") else "audio/mp4"

    # If cached MP3 is already available, prefer MP3 for best compatibility
    if not object_key.endswith(".mp3"):
        mp3_key = object_key.rsplit(".", 1)[0] + ".mp3"
        try:
            if await storage_r2.object_exists(mp3_key):
                stream_key = mp3_key
                media_type = "audio/mpeg"
        except Exception:
            pass

    try:
        range_header = request.headers.get("Range")
        status_code, resp_headers, body_stream = await storage_r2.stream_object(
            stream_key,
            range_header=range_header,
        )
        resp_headers["Content-Type"] = media_type
        return StreamingResponse(
            body_stream,
            status_code=status_code,
            headers=resp_headers,
            media_type=media_type,
        )
    except Exception as e:
        logger.error("Error streaming recording %s: %s", recording_id, e)
        raise HTTPException(status_code=502, detail=f"Failed to stream recording: {e}")


@router.get("/egress/{recording_id}/download", dependencies=[Depends(requires_admin)])
async def download_recording(
    recording_id: str,
):
    """Directly download recording file in standard MP3 format with pre-signed Cloudflare R2 URL."""
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    object_key = rec.get("storage_object_key")
    if not object_key:
        raise HTTPException(status_code=404, detail="Recording file not available")

    # Ensure recording is in standard MP3 format
    target_key = object_key
    try:
        mp3_key = await ensure_mp3_in_r2(object_key)
        if mp3_key:
            target_key = mp3_key
            if mp3_key != object_key:
                # Update database record to point to cached MP3
                try:
                    await telephony_db.update_recording(recording_id, storage_object_key=mp3_key)
                except Exception as e:
                    logger.debug("Non-critical: could not update storage_object_key to MP3: %s", e)
    except Exception as e:
        logger.warning("MP3 transcoding fallback for %s: %s", recording_id, e)

    # Generate friendly MP3 filename
    direction = rec.get("direction", "call")
    caller = (rec.get("caller_number") or "caller").replace("+", "")
    callee = (rec.get("callee_number") or rec.get("did_number") or "").replace("+", "")
    ts = rec.get("started_at", "call")[:10]
    
    if callee:
        filename = f"{direction}_{caller}_to_{callee}_{ts}_{recording_id[:8]}.mp3"
    else:
        filename = f"{direction}_{caller}_{ts}_{recording_id[:8]}.mp3"

    url = storage_r2.generate_presigned_url(
        target_key,
        expires_in=600,
        download=True,
        filename=filename,
    )
    if not url:
        raise HTTPException(status_code=500, detail="Failed to generate secure download link")

    return RedirectResponse(url=url, status_code=307)


@router.post("/egress/{recording_id}/delete", dependencies=[Depends(requires_admin)])
async def delete_recording(
    request: Request,
    recording_id: str,
    csrf_token: str = Form(...),
):
    """Delete recording from Cloudflare R2 and update database status."""
    await verify_csrf_token(request)

    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    object_key = rec.get("storage_object_key")
    if object_key:
        try:
            await storage_r2.delete_object(object_key)
            logger.info("Deleted R2 object %s for recording %s", object_key, recording_id)
        except Exception as e:
            logger.warning("Error deleting object %s from R2: %s", object_key, e)

    await telephony_db.delete_recording(recording_id)
    logger.info("Marked recording %s as deleted", recording_id)

    return RedirectResponse(url="/egress", status_code=303)


@router.post("/egress/start", dependencies=[Depends(requires_admin)])
async def start_egress(
    request: Request,
    csrf_token: str = Form(...),
    room_name: str = Form(...),
    output_filename: str = Form(...),
    layout: str = Form("grid"),
    audio_only: Optional[str] = Form(None),
    video_only: Optional[str] = Form(None),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Start a room composite egress targeting Cloudflare R2."""
    await verify_csrf_token(request)

    try:
        # Standardize output path for Cloudflare R2
        now = datetime.now()
        filename = output_filename.replace("{room}", room_name)
        filename = filename.replace("{time}", now.strftime("%Y%m%d_%H%M%S"))
        if not filename.startswith("recordings/"):
            filename = f"recordings/{now.strftime('%Y/%m/%d')}/{filename}"

        await lk.start_room_composite_egress(
            room_name=room_name,
            output_filename=filename,
            layout=layout,
            audio_only=(audio_only == "on"),
            video_only=(video_only == "on"),
        )
    except Exception as e:
        logger.warning("Error starting egress: %s", e)

    return RedirectResponse(url="/egress", status_code=303)


@router.post("/egress/start/track", dependencies=[Depends(requires_admin)])
async def start_track_egress(
    request: Request,
    csrf_token: str = Form(...),
    room_name: str = Form(...),
    track_sid: str = Form(...),
    output_filename: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Start a single-track egress."""
    await verify_csrf_token(request)
    try:
        filename = output_filename.replace("{room}", room_name)
        filename = filename.replace("{time}", datetime.now().strftime("%Y%m%d_%H%M%S"))
        await lk.start_track_egress(room_name=room_name, track_sid=track_sid, output_filepath=filename)
    except Exception as e:
        logger.warning("Error starting track egress: %s", e)
    return RedirectResponse(url="/egress", status_code=303)


@router.post("/egress/start/web", dependencies=[Depends(requires_admin)])
async def start_web_egress(
    request: Request,
    csrf_token: str = Form(...),
    url: str = Form(...),
    output_filename: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Start a web-capture egress."""
    await verify_csrf_token(request)
    try:
        filename = output_filename.replace("{time}", datetime.now().strftime("%Y%m%d_%H%M%S"))
        await lk.start_web_egress(url=url, output_filepath=filename)
    except Exception as e:
        logger.warning("Error starting web egress: %s", e)
    return RedirectResponse(url="/egress", status_code=303)


@router.post("/egress/{egress_id}/stop", dependencies=[Depends(requires_admin)])
async def stop_egress(
    request: Request,
    egress_id: str,
    csrf_token: str = Form(...),
    lk: LiveKitClient = Depends(get_livekit_client),
):
    """Stop an active LiveKit egress job."""
    await verify_csrf_token(request)
    try:
        await lk.stop_egress(egress_id)
    except Exception as e:
        logger.warning("Error stopping egress: %s", e)

    return RedirectResponse(url="/egress", status_code=303)


@router.get("/api/v1/transcriptions")
async def get_transcriptions_api(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID (e.g. ADMIN)"),
    did_number: Optional[str] = Query(None, description="Filter by DID number (+918065355408)"),
    phone_number: Optional[str] = Query(None, description="Filter by caller or recipient phone number"),
    date: Optional[str] = Query(None, description="Filter by date string (YYYY-MM-DD)"),
    recording_id: Optional[str] = Query(None, description="Filter by specific recording ID"),
    call_id: Optional[str] = Query(None, description="Filter by specific call ID"),
    room_name: Optional[str] = Query(None, description="Filter by specific LiveKit room name"),
    format: Optional[str] = Query("json", description="Output format: json or text"),
    limit: int = Query(100, ge=1, le=500),
):
    """
    Dedicated Querying API to retrieve call transcriptions.
    Filtered by call ID, room name, tenant ID, DID number, caller/callee phone number, date, or recording ID.
    Used for downstream AI summarization, CRM synchronization, and data extraction.
    """
    try:
        results = await telephony_db.query_transcriptions(
            tenant_id=tenant_id,
            did_number=did_number,
            phone_number=phone_number,
            date_str=date,
            recording_id=recording_id,
            call_id=call_id,
            room_name=room_name,
            limit=limit,
        )
        if format and format.lower() == "text":
            lines = []
            for r in results:
                lines.append(f"[{r.get('started_at')}] [{r.get('direction', '').upper()}] Tenant: {r.get('tenant_id')} | Caller: {r.get('caller_number')} | Receiver: {r.get('callee_number') or r.get('did_number')}")
                lines.append(f"Transcript: {r.get('transcription')}\n")
            return PlainTextResponse(content="\n".join(lines))

        return {
            "status": "success",
            "count": len(results),
            "filters": {
                "tenant_id": tenant_id,
                "did_number": did_number,
                "phone_number": phone_number,
                "date": date,
                "recording_id": recording_id,
                "call_id": call_id,
                "room_name": room_name,
            },
            "transcriptions": results,
        }
    except Exception as e:
        logger.error("Error in query transcriptions API: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to query transcriptions: {e}")


@router.get("/egress/{recording_id}/transcript", dependencies=[Depends(requires_admin)])
async def get_recording_transcript(recording_id: str):
    """Get the AI transcription text and status for a recording (for modal inspection)."""
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    return {
        "recording_id": recording_id,
        "tenant_id": rec.get("tenant_id") or "ADMIN",
        "caller_number": rec.get("caller_number"),
        "callee_number": rec.get("callee_number") or rec.get("did_number"),
        "direction": rec.get("direction"),
        "duration_seconds": rec.get("duration_seconds"),
        "started_at": rec.get("started_at"),
        "transcription": rec.get("transcription"),
        "transcription_status": rec.get("transcription_status") or ("completed" if rec.get("transcription") else "pending"),
    }


@router.get("/egress/{recording_id}/transcript/download", dependencies=[Depends(requires_admin)])
async def download_recording_transcript(recording_id: str):
    """Download plain text file of the AI call transcription."""
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    transcript_text = rec.get("transcription") or "No transcription available."
    direction = (rec.get("direction") or "call").upper()
    tenant = rec.get("tenant_id") or "ADMIN"
    caller = rec.get("caller_number") or "Unknown"
    callee = rec.get("callee_number") or rec.get("did_number") or "+918065355408"
    duration = rec.get("duration_seconds", 0)
    mins = duration // 60
    secs = duration % 60
    started = rec.get("started_at", "N/A")

    content = f"""================================================================================
WASID AI TELEPHONY CALL TRANSCRIPTION REPORT
================================================================================
Recording ID   : {recording_id}
Date & Time    : {started}
Call Direction : {direction}
Tenant ID      : {tenant}
Caller (From)  : {caller}
Receiver (To)  : {callee}
DID Number     : {rec.get('did_number') or '+918065355408'}
Duration       : {mins}m {secs:02d}s
AI Engine      : Google Gemini 3.5 Transcribe Live (SMART Mode)
================================================================================

TRANSCRIPT:
--------------------------------------------------------------------------------
{transcript_text}
--------------------------------------------------------------------------------
Generated by WASID AI Telephony Platform
================================================================================
"""
    filename = f"transcript_{tenant}_{direction.lower()}_{recording_id[:12]}.txt"
    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.post("/egress/{recording_id}/transcribe", dependencies=[Depends(requires_admin)])
async def trigger_recording_transcription(recording_id: str):
    """On-demand trigger for Gemini 3.5 Live transcription."""
    import asyncio
    from app.services.transcribe_gemini import gemini_transcribe
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    await telephony_db.update_recording_transcription(recording_id, "", status="transcribing")

    def _task_done(task: asyncio.Task):
        if task.exception():
            logger.error(
                "Background transcription task for %s crashed: %s",
                recording_id, task.exception(),
            )

    task = asyncio.create_task(
        gemini_transcribe.transcribe_recording(recording_id, force=True),
        name=f"transcribe-{recording_id}",
    )
    task.add_done_callback(_task_done)

    return {
        "status": "queued",
        "recording_id": recording_id,
        "message": "Transcription job started in background with Gemini 3.5 Live STT",
    }
