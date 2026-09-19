"""Tests for Outbound SIP Security, Target Ownership, Deduplication & DID Resolution."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import status
from fastapi.testclient import TestClient

from app.main import app
from app.services.livekit import get_livekit_client
from app.services.db import telephony_db


@pytest.fixture
def test_client():
    mock_lk = MagicMock()
    mock_lk.sip_enabled = True
    mock_lk.create_room = AsyncMock(return_value=MagicMock())
    mock_lk.create_dispatch = AsyncMock(return_value=MagicMock())
    mock_lk.create_sip_participant = AsyncMock(return_value="participant_p123")

    app.dependency_overrides[get_livekit_client] = lambda: mock_lk
    with TestClient(app) as client:
        yield client, mock_lk
    app.dependency_overrides.pop(get_livekit_client, None)


@pytest.mark.asyncio
async def test_outbound_target_ownership_cross_tenant_rejected(test_client):
    """Verify Tenant A cannot call Tenant B's contact (403 Forbidden)."""
    client, mock_lk = test_client

    with patch.object(
        telephony_db,
        "verify_target_ownership",
        new=AsyncMock(return_value={
            "allowed": False,
            "status": "tenant_mismatch",
            "error": "TENANT_TARGET_MISMATCH: Contact belongs to tenant 'ALS12345678', not 'FIT12345678'.",
            "target_tenant": "ALS12345678"
        })
    ):
        resp = client.post(
            "/api/v1/sip/outbound-call",
            json={
                "tenant_id": "FIT12345678",
                "sip_call_to": "+97143435333",
                "contact_id": "cnt_alsafadi_01"
            }
        )
        assert resp.status_code == status.HTTP_403_FORBIDDEN
        assert "TENANT_TARGET_MISMATCH" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_outbound_target_ownership_unregistered_rejected(test_client):
    """Verify customer tenant cannot dial an unregistered raw number (400 Bad Request)."""
    client, mock_lk = test_client

    with patch.object(
        telephony_db,
        "verify_target_ownership",
        new=AsyncMock(return_value={
            "allowed": False,
            "status": "unregistered_target",
            "error": "UNREGISTERED_TARGET: Phone '+971509999999' is not registered under tenant 'FIT12345678'."
        })
    ):
        resp = client.post(
            "/api/v1/sip/outbound-call",
            json={
                "tenant_id": "FIT12345678",
                "sip_call_to": "+971509999999"
            }
        )
        assert resp.status_code == status.HTTP_400_BAD_REQUEST
        assert "UNREGISTERED_TARGET" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_outbound_duplicate_call_protection_rejected(test_client):
    """Verify duplicate active call for the same destination is blocked (409 Conflict)."""
    client, mock_lk = test_client

    with patch.object(
        telephony_db,
        "check_active_call_exists",
        new=AsyncMock(return_value={
            "call_id": "call_active_123",
            "tenant_id": "FIT12345678",
            "callee_did": "+97141234567",
            "status": "active"
        })
    ):
        resp = client.post(
            "/api/v1/sip/outbound-call",
            json={
                "tenant_id": "FIT12345678",
                "sip_call_to": "+97141234567",
                "lead_id": "lead_fit_01"
            }
        )
        assert resp.status_code == status.HTTP_409_CONFLICT
        assert "DUPLICATE_ACTIVE_CALL" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_outbound_verified_target_success_and_did_selection(test_client):
    """Verify verified tenant-owned target succeeds with OUTBOUND_VERIFIED and dynamic DID selection."""
    client, mock_lk = test_client

    with patch.object(
        telephony_db,
        "check_active_call_exists",
        new=AsyncMock(return_value=None)
    ), patch.object(
        telephony_db,
        "verify_target_ownership",
        new=AsyncMock(return_value={
            "allowed": True,
            "target_type": "lead",
            "target_id": "lead_fit_01",
            "customer_name": "FitZone Member",
            "phone": "+971501112233",
            "tenant_id": "FIT12345678"
        })
    ), patch.object(
        telephony_db,
        "get_tenant_outbound_did",
        new=AsyncMock(return_value="+97141234567")
    ), patch.object(
        telephony_db,
        "record_call_start",
        new=AsyncMock(return_value={"call_id": "call_fit_123"})
    ):
        resp = client.post(
            "/api/v1/sip/outbound-call",
            json={
                "tenant_id": "FIT12345678",
                "lead_id": "lead_fit_01",
                "sip_call_to": "+971501112233"
            }
        )
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert data["success"] is True
        assert data["tenant_id"] == "FIT12345678"
        assert data["agent_name"] == "wasid-customer-master"
        assert data["verification_state"] == "OUTBOUND_VERIFIED"
        assert data["caller_did"] == "+97141234567"
        assert data["callee"] == "+971501112233"


@pytest.mark.asyncio
async def test_outbound_wasid_admin_ad_hoc_calling(test_client):
    """Verify WASID Admin (WAS12345678) can call authorized prospects with Admin DID +918065355408."""
    client, mock_lk = test_client

    with patch.object(
        telephony_db,
        "check_active_call_exists",
        new=AsyncMock(return_value=None)
    ), patch.object(
        telephony_db,
        "verify_target_ownership",
        new=AsyncMock(return_value={
            "allowed": True,
            "target_type": "admin_ad_hoc",
            "customer_name": "Enterprise Prospect",
            "phone": "+918009128306",
            "tenant_id": "WAS12345678"
        })
    ), patch.object(
        telephony_db,
        "get_tenant_outbound_did",
        new=AsyncMock(return_value="+918065355408")
    ), patch.object(
        telephony_db,
        "record_call_start",
        new=AsyncMock(return_value={"call_id": "call_was_123"})
    ):
        resp = client.post(
            "/api/v1/sip/outbound-call",
            json={
                "tenant_id": "WAS12345678",
                "sip_call_to": "+918009128306",
                "is_admin_override": True
            }
        )
        assert resp.status_code == status.HTTP_200_OK
        data = resp.json()
        assert data["success"] is True
        assert data["tenant_id"] == "WAS12345678"
        assert data["agent_name"] == "wasid-ai-automation-master"
        assert data["caller_did"] == "+918065355408"
