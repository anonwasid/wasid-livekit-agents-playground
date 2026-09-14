"""Egress and Call Recording Routes for WASID LiveKit Platform."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

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

    # Query persistent recordings database
    try:
        stats = await telephony_db.get_recording_stats()
        offset = max(0, (page - 1) * limit)
        recordings = await telephony_db.list_recordings(
            direction=direction,
            status=status,
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
        "search_query": search or "",
        "page": page,
        "limit": limit,
    }

    template_name = "egress/index.html.j2"
    return request.app.state.templates.TemplateResponse(request, template_name, template_data)


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

    try:
        range_header = request.headers.get("Range")
        status_code, resp_headers, body_stream = await storage_r2.stream_object(
            object_key,
            range_header=range_header,
        )
        return StreamingResponse(
            body_stream,
            status_code=status_code,
            headers=resp_headers,
            media_type="audio/mp4",
        )
    except Exception as e:
        logger.error("Error streaming recording %s: %s", recording_id, e)
        raise HTTPException(status_code=502, detail=f"Failed to stream recording: {e}")


@router.get("/egress/{recording_id}/download", dependencies=[Depends(requires_admin)])
async def download_recording(
    recording_id: str,
):
    """Directly download recording file with pre-signed Cloudflare R2 URL."""
    rec = await telephony_db.get_recording(recording_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recording not found")

    object_key = rec.get("storage_object_key")
    if not object_key:
        raise HTTPException(status_code=404, detail="Recording file not available")

    # Generate friendly filename
    direction = rec.get("direction", "call")
    caller = rec.get("caller_number", "unknown").replace("+", "")
    ts = rec.get("started_at", "call")[:10]
    filename = f"{direction}_{caller}_{ts}_{recording_id[:8]}.m4a"

    url = storage_r2.generate_presigned_url(
        object_key,
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
