"""Background Recording Supervisor for LiveKit Telephony Calls.

Runs continuously inside the FastAPI lifespan to guarantee:
  1. Any active telephone room (inbound `sip-in-` or outbound `call-out-`) has an active egress job.
  2. Any completed calls are reconciled even if a webhook event was delayed or lost.
"""

import asyncio
import logging
from typing import Optional

from app.services.db import telephony_db
from app.services.livekit import LiveKitClient, get_livekit_client
from app.routes.webhooks import auto_start_room_recording

logger = logging.getLogger(__name__)

_supervisor_task: Optional[asyncio.Task] = None
_running: bool = False


async def _supervise_recordings_loop():
    """Continuous polling loop to reconcile call recordings with LiveKit rooms."""
    global _running
    _running = True
    logger.info("Starting LiveKit Recording Supervisor background worker...")

    while _running:
        try:
            lk = get_livekit_client()
            rooms, _ = await lk.list_all_rooms()
            active_room_names = {getattr(r, "name", "") for r in rooms}

            # 1. Check for any unrecorded SIP calls
            for rname in active_room_names:
                if rname.startswith("sip-in-") or rname.startswith("call-out-") or rname.startswith("sip-out-"):
                    existing = await telephony_db.get_recording_by_room(rname)
                    if not existing or existing.get("status") not in ("recording", "completed"):
                        logger.info("Supervisor detected unrecorded active call room %s. Starting auto-recording...", rname)
                        await auto_start_room_recording(rname, lk)

            # 2. Check for finalized recordings whose rooms have closed
            active_recs = await telephony_db.list_recordings(status="recording", limit=100)
            if active_recs:
                all_egress = await lk.list_egress(active=False)
                egress_by_id = {getattr(e, "egress_id", ""): e for e in all_egress}

                for rec in active_recs:
                    rname = rec.get("room_name", "")
                    egress_id = rec.get("egress_id", "")

                    # If room has closed
                    if rname not in active_room_names:
                        # Reconcile via LiveKit egress status if available
                        ejob = egress_by_id.get(egress_id)
                        if ejob:
                            status_val = str(getattr(ejob, "status", ""))
                            is_done = "COMPLETE" in status_val or getattr(ejob, "status", 0) == 3
                            duration = 0
                            if getattr(ejob, "started_at", 0) and getattr(ejob, "ended_at", 0):
                                duration = int((ejob.ended_at - ejob.started_at) / 1e9)

                            file_size = 0
                            file_key = None
                            file_results = getattr(ejob, "file_results", [])
                            if file_results:
                                file_size = getattr(file_results[0], "size", 0)
                                file_key = getattr(file_results[0], "filename", None)

                            await telephony_db.update_recording(
                                recording_id=rec["recording_id"],
                                status="completed" if is_done else "failed",
                                duration_seconds=duration,
                                file_size_bytes=file_size,
                                storage_object_key=file_key,
                            )
                            logger.info("Supervisor finalized closed call recording %s (%s)", rec["recording_id"], rname)

        except Exception as e:
            logger.debug("Supervisor loop iteration encountered error: %s", e)

        # Poll every 10 seconds
        await asyncio.sleep(10)


def start_recording_supervisor():
    """Start the recording supervisor background task."""
    global _supervisor_task
    if _supervisor_task is None or _supervisor_task.done():
        _supervisor_task = asyncio.create_task(_supervise_recordings_loop())


def stop_recording_supervisor():
    """Stop the recording supervisor background task."""
    global _running, _supervisor_task
    _running = False
    if _supervisor_task and not _supervisor_task.done():
        _supervisor_task.cancel()
