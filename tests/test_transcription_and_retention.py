"""Unit and integration tests for Gemini 3.5 live transcription, tenant filtering, querying API, and retention pruning."""

import base64
import os
import uuid
from datetime import datetime, timezone, timedelta
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.db import telephony_db


def _auth_headers():
    creds = f"{os.environ.get('ADMIN_USERNAME', 'admin')}:{os.environ.get('ADMIN_PASSWORD', 'changeme')}"
    return {"Authorization": f"Basic {base64.b64encode(creds.encode()).decode()}"}


@pytest.mark.asyncio
async def test_tenant_and_did_recordings_and_distinct():
    rec_id = f"rec_tx_test_{uuid.uuid4().hex[:8]}"
    room = f"sip-in-+918065355408_{uuid.uuid4().hex[:6]}"
    await telephony_db.init_db()

    created = await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=room,
        direction="inbound",
        caller_number="+918009128306",
        callee_number="+918065355408",
        did_number="+918065355408",
        tenant_id="ADMIN",
        agent_id="wasid-ai-automation-master",
        status="completed",
        storage_bucket="n8n-production-backups",
        storage_object_key=f"recordings/2026/09/15/inbound/{rec_id}.m4a",
    )
    assert created is not None

    # Test distinct tenants and DIDs
    tenants = await telephony_db.get_distinct_tenants()
    assert "ADMIN" in tenants

    dids = await telephony_db.get_distinct_dids()
    assert "+918065355408" in dids

    # Test list_recordings with tenant_id filter
    admin_recs = await telephony_db.list_recordings(tenant_id="ADMIN")
    assert any(r["recording_id"] == rec_id for r in admin_recs)

    # Test list_recordings with did filter
    did_recs = await telephony_db.list_recordings(did="+918065355408")
    assert any(r["recording_id"] == rec_id for r in did_recs)


@pytest.mark.asyncio
async def test_transcription_persistence_and_query():
    rec_id = f"rec_tx_{uuid.uuid4().hex[:8]}"
    transcript_text = "Hello there! This is Wasaid AI, your automation master. How can I help you today?"
    await telephony_db.init_db()

    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name=f"sip-in-{uuid.uuid4().hex[:6]}",
        direction="inbound",
        caller_number="+918009128306",
        callee_number="+918065355408",
        did_number="+918065355408",
        tenant_id="ADMIN",
        status="completed",
    )

    # Persist transcript
    ok = await telephony_db.update_recording_transcription(rec_id, transcript_text, status="completed")
    assert ok is True

    # Query transcript
    rec = await telephony_db.get_recording(rec_id)
    assert rec["transcription"] == transcript_text
    assert rec["transcription_status"] == "completed"

    # Test query_transcriptions helper
    results = await telephony_db.query_transcriptions(tenant_id="ADMIN", recording_id=rec_id)
    assert len(results) >= 1
    assert any(r["transcription"] == transcript_text for r in results)


def test_api_v1_transcriptions_endpoint():
    with TestClient(app, raise_server_exceptions=False) as client:
        # JSON querying
        resp = client.get("/api/v1/transcriptions?tenant_id=ADMIN&limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert isinstance(data["transcriptions"], list)

        # Plain text formatting
        resp_txt = client.get("/api/v1/transcriptions?format=text&limit=5")
        assert resp_txt.status_code == 200
        assert resp_txt.headers["content-type"].startswith("text/plain")


@pytest.mark.asyncio
async def test_transcript_modal_and_download_endpoints():
    rec_id = f"rec_api_test_{uuid.uuid4().hex[:8]}"
    transcript_sample = "Testing plain text download and modal inspection."
    
    await telephony_db.init_db()
    await telephony_db.create_recording(
        recording_id=rec_id,
        room_name="test-room-tx",
        direction="inbound",
        caller_number="+918009128306",
        callee_number="+918065355408",
        did_number="+918065355408",
        tenant_id="ADMIN",
        status="completed",
    )
    await telephony_db.update_recording_transcription(rec_id, transcript_sample, status="completed")

    with TestClient(app, raise_server_exceptions=False) as client:
        # Test GET /egress/{rec_id}/transcript
        res_json = client.get(f"/egress/{rec_id}/transcript", headers=_auth_headers())
        assert res_json.status_code == 200
        jdata = res_json.json()
        assert jdata["recording_id"] == rec_id
        assert jdata["transcription"] == transcript_sample
        assert jdata["tenant_id"] == "ADMIN"

        # Test GET /egress/{rec_id}/transcript/download
        res_dl = client.get(f"/egress/{rec_id}/transcript/download", headers=_auth_headers())
        assert res_dl.status_code == 200
        assert "text/plain" in res_dl.headers["content-type"]
        assert "attachment" in res_dl.headers["content-disposition"]
        assert transcript_sample in res_dl.text
        assert "WASID AI TELEPHONY CALL TRANSCRIPTION REPORT" in res_dl.text


@pytest.mark.asyncio
async def test_30_day_retention_prune():
    old_rec_id = f"rec_old_{uuid.uuid4().hex[:8]}"
    old_started = datetime.now(timezone.utc) - timedelta(days=40)

    await telephony_db.init_db()
    sessionmaker = telephony_db.get_sessionmaker()
    from app.services.db import CallRecordingRecord
    async with sessionmaker() as session:
        async with session.begin():
            old_rec = CallRecordingRecord(
                recording_id=old_rec_id,
                room_name="sip-in-old-call",
                direction="inbound",
                caller_number="+919999999999",
                callee_number="+918065355408",
                did_number="+918065355408",
                tenant_id="ADMIN",
                status="completed",
                started_at=old_started,
                file_size_bytes=1024,
            )
            session.add(old_rec)

    res = await telephony_db.prune_expired_recordings(days=30)
    assert res["deleted_count"] >= 1
    assert res["freed_bytes"] >= 1024

    pruned = await telephony_db.get_recording(old_rec_id)
    assert pruned is None or pruned.get("status") == "deleted"
