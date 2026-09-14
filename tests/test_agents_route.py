# Unit and route integration tests for /agents endpoint
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from app.main import app
from app.services.livekit import get_livekit_client


def _auth_headers():
    import base64
    creds = f"{os.environ.get('ADMIN_USERNAME', 'admin')}:{os.environ.get('ADMIN_PASSWORD', 'admin')}"
    return {"Authorization": f"Basic {base64.b64encode(creds.encode()).decode()}"}


def _csrf_token():
    from app.security.csrf import generate_csrf_token
    return generate_csrf_token()


def _make_mock_lk():
    lk = MagicMock()
    lk.url = "wss://test.livekit.cloud"
    lk.list_all_dispatches = AsyncMock(return_value=([], 0.01))
    lk.list_sip_trunks = AsyncMock(return_value=[])
    lk.list_sip_inbound_trunks = AsyncMock(return_value=[])
    lk.list_sip_dispatch_rules = AsyncMock(return_value=[])
    lk.list_all_rooms = AsyncMock(return_value=([], 0.01))
    return lk


def test_agents_page_requires_auth():
    client = TestClient(app)
    response = client.get("/agents")
    assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_agents_page_renders_with_redesigned_ui():
    mock_lk = _make_mock_lk()
    app.dependency_overrides[get_livekit_client] = lambda: mock_lk
    client = TestClient(app)

    try:
        response = client.get("/agents", headers=_auth_headers())
        assert response.status_code == status.HTTP_200_OK
        text = response.text
        # Verify Key Information Architecture elements
        assert "WASID LIVE" in text
        assert "AGENT CONTROL CENTER" in text
        assert "PHASE 7 SIP TELEPHONY" in text
        assert "wasid-ai-automation-master" in text
        assert "wasid-customer-master" in text
        assert "Voice Routing Matrix" in text
        assert "PSTN Ingress" in text
        assert "ST_kcrc2jpfVgJ8" in text
        assert "Live Telephony Sessions" in text
        assert "Agent Inspector" in text
        assert "13 Dimensions" in text
    finally:
        app.dependency_overrides.clear()


def test_get_agent_canonical_spec_api():
    client = TestClient(app)
    response = client.get("/agents/api/spec/wasid-ai-automation-master", headers=_auth_headers())
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data.get("agent_id") == "wasid-ai-automation-master" or data.get("id") == "wasid-ai-automation-master"
    assert "config" in data or "tools" in data
