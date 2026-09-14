"""Unit tests for WASID Voice Routing Service."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from app.services.sip_routing import SipRoutingService, CANONICAL_MASTER_AGENT, DEFAULT_INBOUND_ROOM_PREFIX


@pytest.mark.asyncio
async def test_routing_matrix_empty():
    service = SipRoutingService()
    lk = MagicMock()
    lk.list_sip_inbound_trunks = AsyncMock(return_value=[])
    lk.list_sip_dispatch_rules = AsyncMock(return_value=[])
    lk.list_sip_trunks = AsyncMock(return_value=[])
    lk.list_all_rooms = AsyncMock(return_value=([], 0.0))

    matrix = await service.get_voice_routing_matrix(lk)
    assert matrix["canonical_target_agent"] == CANONICAL_MASTER_AGENT
    assert matrix["routes"] == []
    assert matrix["vobiz_ready"] is False
    assert matrix["active_sip_calls"] == 0


@pytest.mark.asyncio
async def test_routing_matrix_with_individual_rule():
    service = SipRoutingService()
    lk = MagicMock()
    
    # Mock trunk
    trunk = MagicMock()
    trunk.sip_trunk_id = "ST_vobiz_01"
    trunk.name = "Vobiz Main Inbound"
    trunk.numbers = ["+14155550100"]
    lk.list_sip_inbound_trunks = AsyncMock(return_value=[trunk])

    # Mock dispatch rule with room_config agent
    rule = MagicMock()
    rule.sip_dispatch_rule_id = "SDR_vobiz_01"
    rule.name = "WASID Vobiz Inbound Master"
    rule.rule_type = "individual"
    rule.trunk_ids = ["ST_vobiz_01"]
    
    # Mock rule protobuf structure
    rule_obj = MagicMock()
    rule_obj.dispatch_rule_individual = MagicMock(room_prefix="sip-in-")
    rule_obj.dispatch_rule_direct = None
    rule_obj.dispatch_rule_callee = None
    rule.rule = rule_obj

    # Mock room_config with canonical agent
    agent_dispatch = MagicMock(agent_name="wasid-ai-automation-master", metadata='{"tenant": "WAS12345678"}')
    room_config = MagicMock(agents=[agent_dispatch])
    rule.room_config = room_config

    lk.list_sip_dispatch_rules = AsyncMock(return_value=[rule])
    lk.list_sip_trunks = AsyncMock(return_value=[])
    lk.list_all_rooms = AsyncMock(return_value=([], 0.0))

    matrix = await service.get_voice_routing_matrix(lk)
    assert len(matrix["routes"]) == 1
    route = matrix["routes"][0]
    assert route["direction"] == "INBOUND"
    assert route["target_agent"] == "wasid-ai-automation-master"
    assert "sip-in-" in route["room_strategy"]
    assert route["is_unique_room"] is True
    assert matrix["vobiz_ready"] is True


@pytest.mark.asyncio
async def test_initiate_outbound_call_unique_room():
    service = SipRoutingService()
    lk = MagicMock()
    lk.sip_enabled = True
    lk.create_dispatch = AsyncMock(return_value=MagicMock())
    lk.create_sip_participant = AsyncMock(return_value=MagicMock(participant_id="SIP_part_1"))

    res = await service.initiate_outbound_call(
        lk=lk,
        sip_trunk_id="ST_out_01",
        sip_call_to="+14155550199",
        agent_name="wasid-ai-automation-master",
    )

    assert res["status"] == "initiated"
    assert res["room_name"].startswith("sip-out-")
    assert res["agent_name"] == "wasid-ai-automation-master"
    assert lk.create_dispatch.call_count == 1
    assert lk.create_sip_participant.call_count == 1


@pytest.mark.asyncio
async def test_did_routing_postgresql_sync_and_reassign():
    import uuid
    from app.services.db import telephony_db
    await telephony_db.init_db()

    # Ensure +971501234567 is initially mapped to wasid-ai-automation-master
    await telephony_db.upsert_did_routing(
        did="+971501234567",
        agent_id="wasid-ai-automation-master",
        tenant_id="wasid-hq",
        tenant_name="WASID HQ / Operations",
    )

    # Verify upserted DID is loaded (zero fake seed DIDs loaded)
    dids = await telephony_db.get_all_did_routings()
    assert len(dids) >= 1
    hq_did = await telephony_db.get_did_routing("+971501234567")
    assert hq_did is not None
    assert hq_did["agent_id"] == "wasid-ai-automation-master"

    # Reassign DID to wasid-customer-master
    service = SipRoutingService()
    lk = MagicMock()
    rule_mock = MagicMock(sip_dispatch_rule_id="SDR_test_999")
    lk.create_sip_dispatch_rule = AsyncMock(return_value=rule_mock)

    updated = await service.reassign_did_routing(
        lk=lk,
        did="+971501234567",
        agent_name="wasid-customer-master",
    )
    assert updated["agent_id"] == "wasid-customer-master"
    assert updated["dispatch_rule_id"] == "SDR_test_999"

    # Reset back to wasid-ai-automation-master
    await service.reassign_did_routing(
        lk=lk,
        did="+971501234567",
        agent_name="wasid-ai-automation-master",
    )

    # Verify invalid agent rejection
    with pytest.raises(ValueError):
        await service.reassign_did_routing(
            lk=lk,
            did="+971501234567",
            agent_name="invalid-fake-agent",
        )


@pytest.mark.asyncio
async def test_call_lifecycle_recording():
    import uuid
    from app.services.db import telephony_db
    call_id = f"call-test-{uuid.uuid4().hex[:8]}"
    call = await telephony_db.record_call_start(
        call_id=call_id,
        room_name=f"sip-out-{call_id}",
        direction="outbound",
        caller_did="Carrier Assigned",
        callee_did="+971500000000",
        agent_id="wasid-ai-automation-master",
    )
    assert call["call_id"] == call_id
    assert call["status"] == "active"

    ended = await telephony_db.record_call_end(
        call_id=call_id,
        duration_seconds=95,
        status="completed",
        outcome="customer_connected",
    )
    assert ended["duration_seconds"] == 95
    assert ended["status"] == "completed"
    assert ended["outcome"] == "customer_connected"
