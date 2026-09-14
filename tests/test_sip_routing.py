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
    lk.list_sip_dispatch_rules = AsyncMock(return_value=[])
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
async def test_did_routing_idempotent_and_reuses_existing_rule():
    """Verify that DID assignment is idempotent, reuses existing LiveKit dispatch rules, and never creates duplicates."""
    from app.services.db import telephony_db
    await telephony_db.init_db()

    # Pre-seed a DID for FitZone
    test_did = "+971509998877"
    await telephony_db.upsert_did_routing(
        did=test_did,
        agent_id="wasid-customer-master",
        tenant_id="TYON268696498",
        tenant_name="FitZone Gym",
        dispatch_rule_id="SDR_8N7DJE97PAze",
    )

    # Mock LiveKit with existing persistent rules
    existing_rule_customer = MagicMock()
    existing_rule_customer.sip_dispatch_rule_id = "SDR_8N7DJE97PAze"
    existing_rule_customer.name = "WASID Customer Inbound Master"
    existing_rule_customer.trunk_ids = ["ST_kcrc2jpfVgJ8"]
    agent_dispatch = MagicMock(agent_name="wasid-customer-master", metadata='{"tenant": "customer"}')
    existing_rule_customer.room_config = MagicMock(agents=[agent_dispatch])

    existing_rule_master = MagicMock()
    existing_rule_master.sip_dispatch_rule_id = "SDR_qgCxptTPBnyh"
    existing_rule_master.name = "WASID Vobiz Inbound Master"
    existing_rule_master.trunk_ids = []
    agent_dispatch_master = MagicMock(agent_name="wasid-ai-automation-master", metadata='{}')
    existing_rule_master.room_config = MagicMock(agents=[agent_dispatch_master])

    lk = MagicMock()
    lk.list_sip_dispatch_rules = AsyncMock(return_value=[existing_rule_customer, existing_rule_master])
    lk.create_sip_dispatch_rule = AsyncMock()
    lk.update_sip_dispatch_rule = AsyncMock()

    service = SipRoutingService()

    # Attempt 1: Assign to wasid-customer-master
    res1 = await service.reassign_did_routing(
        lk=lk,
        did=test_did,
        agent_name="wasid-customer-master",
        tenant_id="TYON268696498",
        tenant_name="FitZone Gym",
    )
    assert res1["agent_id"] == "wasid-customer-master"
    assert res1["dispatch_rule_id"] == "SDR_8N7DJE97PAze"
    assert res1["tenant_id"] == "TYON268696498"
    assert res1["tenant_name"] == "FitZone Gym"
    # CreateSIPDispatchRule MUST NOT have been called
    assert lk.create_sip_dispatch_rule.call_count == 0

    # Attempt 2: Repeat the EXACT same assignment (idempotency check)
    res2 = await service.reassign_did_routing(
        lk=lk,
        did=test_did,
        agent_name="wasid-customer-master",
        tenant_id="TYON268696498",
        tenant_name="FitZone Gym",
    )
    assert res2["agent_id"] == "wasid-customer-master"
    assert res2["dispatch_rule_id"] == "SDR_8N7DJE97PAze"
    assert res2["tenant_id"] == "TYON268696498"
    assert res2["tenant_name"] == "FitZone Gym"
    # Still zero calls to create_sip_dispatch_rule
    assert lk.create_sip_dispatch_rule.call_count == 0

    # Verify PostgreSQL has exactly 1 authoritative record for this DID
    rec = await telephony_db.get_did_routing(test_did)
    assert rec is not None
    assert rec["did"] == test_did
    assert rec["agent_id"] == "wasid-customer-master"
    assert rec["tenant_id"] == "TYON268696498"
    assert rec["tenant_name"] == "FitZone Gym"
    assert rec["dispatch_rule_id"] == "SDR_8N7DJE97PAze"


@pytest.mark.asyncio
async def test_did_routing_collision_fallback_recovery():
    """Verify that if create_sip_dispatch_rule raises collision, it safely adopts the existing rule."""
    from app.services.db import telephony_db
    await telephony_db.init_db()

    test_did = "+971509991122"
    service = SipRoutingService()

    # Create mock rule to be adopted
    conflicting_rule = MagicMock()
    conflicting_rule.sip_dispatch_rule_id = "SDR_8N7DJE97PAze"
    conflicting_rule.name = "WASID Vobiz Inbound (wasid-customer-master)"
    conflicting_rule.trunk_ids = []
    conflicting_rule.room_config = MagicMock(agents=[MagicMock(agent_name="wasid-customer-master")])

    lk = MagicMock()
    # Initially returns empty, so it attempts creation
    lk.list_sip_dispatch_rules = AsyncMock(side_effect=[
        [],  # initial list is empty
        [conflicting_rule]  # refetched list has the existing rule
    ])
    # create raises Twirp collision error
    lk.create_sip_dispatch_rule = AsyncMock(
        side_effect=Exception('TwirpError(code=invalid_argument, message=Dispatch rule for the same trunk, inbound number, number, and PIN combination already exists in dispatch rule "<new>" "WASID Vobiz Inbound (wasid-customer-master)", status=400)')
    )

    # Reassign should NOT raise error; it should gracefully adopt the conflicting rule
    res = await service.reassign_did_routing(
        lk=lk,
        did=test_did,
        agent_name="wasid-customer-master",
        tenant_id="TYON268696498",
        tenant_name="FitZone Gym",
    )
    assert res["agent_id"] == "wasid-customer-master"
    assert res["dispatch_rule_id"] == "SDR_8N7DJE97PAze"
    assert res["tenant_id"] == "TYON268696498"



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
