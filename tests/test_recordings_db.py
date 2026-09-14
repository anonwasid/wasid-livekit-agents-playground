"""Unit tests for Call Recordings database repository."""

import uuid
import pytest
from app.services.db import telephony_db


@pytest.mark.asyncio
async def test_create_and_get_recording():
    rec_id = f"rec_test_{uuid.uuid4().hex[:8]}"
    room = f"sip-in-+918065355408_{uuid.uuid4().hex[:6]}"
    
    created = await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=room,
        direction="inbound",
        caller_number="+919876543210",
        callee_number="+918065355408",
        did_number="+918065355408",
        tenant_id="wasid-hq",
        agent_id="wasid-ai-automation-master",
        status="recording",
        storage_bucket="wasid-voice-recordings",
        storage_object_key=f"recordings/2026/09/15/inbound/{rec_id}.m4a",
    )
    assert created is not None
    assert created["recording_id"] == rec_id
    assert created["direction"] == "inbound"
    assert created["status"] == "recording"

    # Fetch by ID
    fetched = await telephony_db.get_recording(rec_id)
    assert fetched is not None
    assert fetched["recording_id"] == rec_id
    assert fetched["room_name"] == room

    # Fetch by Room
    by_room = await telephony_db.get_recording_by_room(room)
    assert by_room is not None
    assert by_room["recording_id"] == rec_id


@pytest.mark.asyncio
async def test_update_recording():
    rec_id = f"rec_test_{uuid.uuid4().hex[:8]}"
    egress_id = f"EG_mock_{uuid.uuid4().hex[:8]}"
    room = f"sip-out-12345-{uuid.uuid4().hex[:6]}"

    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=room,
        direction="outbound",
        caller_number="+918065355408",
        callee_number="+919876543211",
        status="recording",
        storage_bucket="wasid-voice-recordings",
        storage_object_key=f"recordings/2026/09/15/outbound/{rec_id}.m4a",
    )

    updated = await telephony_db.update_recording(
        recording_id=rec_id,
        egress_id=egress_id,
        status="completed",
        duration_seconds=42,
        file_size_bytes=262144,
    )
    assert updated is not None
    assert updated["status"] == "completed"
    assert updated["duration_seconds"] == 42
    assert updated["file_size_bytes"] == 262144
    assert updated["egress_id"] == egress_id

    # Fetch by Egress ID
    by_egress = await telephony_db.get_recording_by_egress_id(egress_id)
    assert by_egress is not None
    assert by_egress["recording_id"] == rec_id


@pytest.mark.asyncio
async def test_list_and_filter_recordings():
    # List all
    recs = await telephony_db.list_recordings(limit=10)
    assert len(recs) >= 2

    # Filter inbound
    inbound_recs = await telephony_db.list_recordings(direction="inbound")
    for r in inbound_recs:
        assert r["direction"] == "inbound"

    # Search filter
    search_recs = await telephony_db.list_recordings(search="9876543210")
    assert len(search_recs) >= 1
    assert any("9876543210" in (r.get("caller_number") or "") for r in search_recs)


@pytest.mark.asyncio
async def test_delete_and_stats():
    rec_id = f"rec_test_{uuid.uuid4().hex[:8]}"
    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=f"sip-in-del-{uuid.uuid4().hex[:6]}",
        direction="inbound",
        status="completed",
        duration_seconds=10,
        file_size_bytes=50000,
    )

    stats_before = await telephony_db.get_recording_stats()
    assert stats_before["total_recordings"] >= 1

    deleted = await telephony_db.delete_recording(rec_id)
    assert deleted is True

    # Check soft delete hides it from list
    recs = await telephony_db.list_recordings()
    assert not any(r["recording_id"] == rec_id for r in recs)
