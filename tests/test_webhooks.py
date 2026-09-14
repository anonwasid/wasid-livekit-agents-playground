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
