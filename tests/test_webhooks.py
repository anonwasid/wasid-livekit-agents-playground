"""Unit tests for LiveKit Webhook Handler."""

import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock
from app.routes.webhooks import auto_start_room_recording
from app.services.db import telephony_db


@pytest.mark.asyncio
async def test_auto_start_inbound_call_recording():
    lk = MagicMock()
    lk.start_room_composite_egress = AsyncMock(
        return_value=MagicMock(egress_id="EG_inbound_test_123")
    )
    room_name = f"sip-in-+918065355408_{uuid.uuid4().hex[:6]}"

    rec_id = await auto_start_room_recording(room_name, lk)
    assert rec_id is not None
    assert rec_id.startswith("rec_")
    assert "inbound" in rec_id

    # Verify egress was triggered with R2 parameters
    lk.start_room_composite_egress.assert_awaited_once()
    call_args = lk.start_room_composite_egress.call_args.kwargs
    assert call_args["room_name"] == room_name
    assert call_args["audio_only"] is True

    # Verify database record was saved
    rec = await telephony_db.get_recording(rec_id)
    assert rec is not None
    assert rec["room_name"] == room_name
    assert rec["direction"] == "inbound"
    assert rec["egress_id"] == "EG_inbound_test_123"


@pytest.mark.asyncio
async def test_auto_start_outbound_call_recording():
    lk = MagicMock()
    lk.start_room_composite_egress = AsyncMock(
        return_value=MagicMock(egress_id="EG_outbound_test_456")
    )
    room_name = f"sip-out-1726000000-{uuid.uuid4().hex[:6]}"

    rec_id = await auto_start_room_recording(room_name, lk)
    assert rec_id is not None
    assert "outbound" in rec_id

    rec = await telephony_db.get_recording(rec_id)
    assert rec is not None
    assert rec["direction"] == "outbound"


@pytest.mark.asyncio
async def test_auto_start_skips_non_phone_rooms():
    lk = MagicMock()
    rec_id = await auto_start_room_recording("regular-web-conference-123", lk)
    assert rec_id is None


def test_extract_call_numbers_comprehensive():
    from app.routes.webhooks import extract_call_numbers
    
    # Inbound call with underscore prefix
    caller, callee, did = extract_call_numbers("sip-in-_918009128306_McDm7ArZ6mqj", is_inbound=True)
    assert caller == "+918009128306"
    assert callee == "+918065355408"
    assert did == "+918065355408"

    # Inbound call with plus prefix
    caller, callee, did = extract_call_numbers("sip-in-_+918065355408_RnDLfPfnVHqv", is_inbound=True)
    assert caller == "+918065355408"
    assert callee == "+918065355408"

    # Outbound call
    caller, callee, did = extract_call_numbers("call-out-1726000000-ef8169", is_inbound=False)
    assert caller == "+918065355408"
    assert callee == "+1726000000"

    # Outbound SIP call
    caller, callee, did = extract_call_numbers("sip-out-+919876543210-abc", is_inbound=False)
    assert caller == "+918065355408"
    assert callee == "+919876543210"


@pytest.mark.asyncio
async def test_mp3_download_redirects_with_mp3_extension(client, auth_headers):
    from unittest.mock import patch
    from app.services.storage_r2 import storage_r2

    # Create test recording in database
    rec_id = f"rec_test_mp3_{uuid.uuid4().hex[:8]}"
    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name="sip-in-_918009128306_test",
        direction="inbound",
        caller_number="+918009128306",
        callee_number="+918065355408",
        did_number="+918065355408",
        status="completed",
        storage_bucket="n8n-production-backups",
        storage_object_key=f"recordings/2026/09/15/inbound/{rec_id}.mp3",
    )

    with patch.object(storage_r2, "generate_presigned_url", return_value="https://r2.test/download.mp3"):
        r = client.get(f"/egress/{rec_id}/download", headers=auth_headers, follow_redirects=False)
        assert r.status_code == 307
        loc = r.headers.get("location", "")
        assert ".mp3" in loc

