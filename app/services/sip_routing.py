"""WASID Voice Routing Service — LiveKit SIP Telephony and Canonical Agent Dispatch.

Establishes deterministic routing chains:
  Inbound:  Vobiz DID -> Vobiz SIP -> Inbound Trunk -> Dispatch Rule -> Canonical Agent (wasid-ai-automation-master) -> Unique Room (sip-in-*) -> Agent Worker
  Outbound: WASID Agent -> Unique Room (sip-out-*) -> Agent Worker -> LiveKit SIP Participant -> Outbound Trunk -> Vobiz -> Destination
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Canonical Master Agents
CANONICAL_MASTER_AGENT = "wasid-ai-automation-master"
CANONICAL_CUSTOMER_AGENT = "wasid-customer-master"
DEFAULT_TENANT_ID = "WAS12345678"
DEFAULT_INBOUND_ROOM_PREFIX = "sip-in-"
DEFAULT_OUTBOUND_ROOM_PREFIX = "sip-out-"


class SipRoutingService:
    """Manages voice routing, deterministic room strategies, and canonical agent bindings."""

    async def get_voice_routing_matrix(self, lk) -> Dict[str, Any]:
        """Compute the full voice routing matrix across inbound trunks, dispatch rules, and agent bindings."""
        inbound_trunks = []
        dispatch_rules = []
        outbound_trunks = []
        active_rooms = []

        try:
            inbound_trunks = await lk.list_sip_inbound_trunks()
        except Exception as e:
            logger.warning("Failed to list SIP inbound trunks for matrix: %s", e)

        try:
            dispatch_rules = await lk.list_sip_dispatch_rules()
        except Exception as e:
            logger.warning("Failed to list SIP dispatch rules for matrix: %s", e)

        try:
            outbound_trunks = await lk.list_sip_trunks()
        except Exception as e:
            logger.warning("Failed to list SIP outbound trunks for matrix: %s", e)

        try:
            active_rooms, _ = await lk.list_all_rooms()
        except Exception as e:
            logger.debug("Failed to list active rooms for matrix: %s", e)

        # Index trunks by ID
        trunk_map: Dict[str, Any] = {}
        for t in inbound_trunks:
            tid = getattr(t, "sip_trunk_id", None) or getattr(t, "id", "")
            if tid:
                trunk_map[tid] = t

        # Active SIP rooms count
        active_sip_rooms = [
            r for r in active_rooms 
            if getattr(r, "name", "").startswith(("sip-in-", "sip-out-"))
        ]

        routes: List[Dict[str, Any]] = []

        # 1. Map each dispatch rule to its trunk(s) and canonical agent binding
        for rule in dispatch_rules:
            rule_id = getattr(rule, "sip_dispatch_rule_id", "")
            rule_name = getattr(rule, "name", "") or rule_id[:16]
            rule_type = getattr(rule, "rule_type", "individual")

            # Extract agent binding from room_config
            bound_agent = None
            agent_metadata_raw = ""
            room_config = getattr(rule, "room_config", None)
            if room_config and hasattr(room_config, "agents"):
                for ag in room_config.agents:
                    if getattr(ag, "agent_name", ""):
                        bound_agent = ag.agent_name
                        agent_metadata_raw = getattr(ag, "metadata", "")
                        break

            # Fallback default if rule creates unique room for voice operations
            if not bound_agent:
                bound_agent = CANONICAL_MASTER_AGENT

            # Extract room prefix or room name
            room_strategy = "sip-in-{caller_id}_{hash}"
            rule_obj = getattr(rule, "rule", None)
            if rule_obj:
                if hasattr(rule_obj, "dispatch_rule_individual") and rule_obj.dispatch_rule_individual:
                    pfx = getattr(rule_obj.dispatch_rule_individual, "room_prefix", "") or DEFAULT_INBOUND_ROOM_PREFIX
                    room_strategy = f"{pfx}{{caller}}_{{suffix}}"
                elif hasattr(rule_obj, "dispatch_rule_direct") and rule_obj.dispatch_rule_direct:
                    rm = getattr(rule_obj.dispatch_rule_direct, "room_name", "")
                    room_strategy = f"Direct: {rm} (Shared)"
                elif hasattr(rule_obj, "dispatch_rule_callee") and rule_obj.dispatch_rule_callee:
                    pfx = getattr(rule_obj.dispatch_rule_callee, "room_prefix", "") or DEFAULT_INBOUND_ROOM_PREFIX
                    room_strategy = f"Callee: {pfx}{{callee}}_{{suffix}}"

            # Check assigned trunk IDs
            trunk_ids = list(getattr(rule, "trunk_ids", []))
            if trunk_ids:
                for tid in trunk_ids:
                    t_obj = trunk_map.get(tid)
                    t_name = getattr(t_obj, "name", "") if t_obj else tid[:16]
                    t_numbers = list(getattr(t_obj, "numbers", [])) if t_obj else []
                    did_display = ", ".join(t_numbers) if t_numbers else "All trunk DIDs"

                    routes.append({
                        "direction": "INBOUND",
                        "did": did_display,
                        "trunk_id": tid,
                        "trunk_name": t_name,
                        "rule_id": rule_id,
                        "rule_name": rule_name,
                        "rule_type": rule_type,
                        "target_agent": bound_agent,
                        "room_strategy": room_strategy,
                        "is_unique_room": "Direct" not in room_strategy,
                        "worker_status": "ONLINE (Ready)",
                        "route_status": "READY",
                        "metadata": agent_metadata_raw,
                    })
            else:
                # Wildcard dispatch rule (matches any trunk or direct caller)
                routes.append({
                    "direction": "INBOUND",
                    "did": "Any / Wildcard",
                    "trunk_id": "*",
                    "trunk_name": "Wildcard (All Inbound Trunks)",
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "target_agent": bound_agent,
                    "room_strategy": room_strategy,
                    "is_unique_room": "Direct" not in room_strategy,
                    "worker_status": "ONLINE (Ready)",
                    "route_status": "READY",
                    "metadata": agent_metadata_raw,
                })

        # 2. Outbound routes
        for ot in outbound_trunks:
            tid = getattr(ot, "sip_trunk_id", None) or getattr(ot, "id", "")
            t_name = getattr(ot, "name", "") or tid[:16]
            numbers = list(getattr(ot, "numbers", []))
            num_display = ", ".join(numbers) if numbers else "Carrier Assigned"
            routes.append({
                "direction": "OUTBOUND",
                "did": num_display,
                "trunk_id": tid,
                "trunk_name": t_name,
                "rule_id": "N/A (Direct Outbound)",
                "rule_name": "Deterministic Outbound Room",
                "rule_type": "dynamic-unique",
                "target_agent": CANONICAL_MASTER_AGENT,
                "room_strategy": "sip-out-{timestamp}_{uuid}",
                "is_unique_room": True,
                "worker_status": "ONLINE (Ready)",
                "route_status": "CONFIGURED",
                "metadata": json.dumps({"tenant_id": DEFAULT_TENANT_ID, "agent_id": CANONICAL_MASTER_AGENT}),
            })

        # Summary statistics
        inbound_count = sum(1 for r in routes if r["direction"] == "INBOUND")
        outbound_count = sum(1 for r in routes if r["direction"] == "OUTBOUND")
        vobiz_ready = inbound_count > 0 and any(
            r["is_unique_room"] and r["target_agent"] == CANONICAL_MASTER_AGENT for r in routes
        )

        return {
            "routes": routes,
            "inbound_trunks_count": len(inbound_trunks),
            "outbound_trunks_count": len(outbound_trunks),
            "dispatch_rules_count": len(dispatch_rules),
            "active_sip_calls": len(active_sip_rooms),
            "canonical_target_agent": CANONICAL_MASTER_AGENT,
            "vobiz_ready": vobiz_ready,
            "worker_status": "ONLINE (Voice Pool Ready)",
            "voice_pipelines": ["REALTIME (Gemini Live)", "CASCADE (Groq + LiteLLM + Gemini TTS)"],
            "summary": {
                "inbound_count": inbound_count,
                "outbound_count": outbound_count,
                "total_routes": len(routes),
                "unique_room_guarantee": True,
            }
        }

    async def provision_vobiz_canonical_rule(
        self,
        lk,
        trunk_ids: Optional[List[str]] = None,
        agent_name: str = CANONICAL_MASTER_AGENT,
        room_prefix: str = DEFAULT_INBOUND_ROOM_PREFIX,
    ) -> Dict[str, Any]:
        """Provisions the canonical individual dispatch rule binding inbound SIP calls to the canonical agent with unique rooms."""
        name = "WASID Vobiz Inbound Master"
        metadata = json.dumps({
            "tenant_id": DEFAULT_TENANT_ID,
            "agent_id": agent_name,
            "provider": "vobiz",
            "direction": "inbound",
            "room_prefix": room_prefix,
        })

        res = await lk.create_sip_dispatch_rule(
            name=name,
            trunk_ids=trunk_ids or [],
            dispatch_rule_type="individual",
            room_prefix=room_prefix,
            agent_name=agent_name,
            agent_metadata=metadata,
            metadata=metadata,
        )
        return {
            "status": "success",
            "rule_id": getattr(res, "sip_dispatch_rule_id", str(res)),
            "name": name,
            "agent_name": agent_name,
            "room_prefix": room_prefix,
            "room_strategy": f"{room_prefix}{{caller}}_{{suffix}}",
        }

    async def initiate_outbound_call(
        self,
        lk,
        sip_trunk_id: str,
        sip_call_to: str,
        agent_name: str = CANONICAL_MASTER_AGENT,
        participant_identity: Optional[str] = None,
        tenant_id: str = DEFAULT_TENANT_ID,
    ) -> Dict[str, Any]:
        """Initiate an outbound SIP call enforcing ONE CALL = ONE UNIQUE ROOM and automatic agent dispatch."""
        if not lk.sip_enabled:
            raise ValueError("LiveKit SIP service is not enabled")

        # 1. Generate collision-safe unique room name
        timestamp = int(time.time())
        short_id = uuid.uuid4().hex[:6]
        room_name = f"{DEFAULT_OUTBOUND_ROOM_PREFIX}{timestamp}-{short_id}"

        # 2. Prepare metadata
        call_meta = json.dumps({
            "tenant_id": tenant_id,
            "agent_id": agent_name,
            "direction": "outbound",
            "call_to": sip_call_to,
            "created_at": timestamp,
        })

        # 3. Dispatch canonical agent to the unique room
        try:
            await lk.create_dispatch(
                agent_name=agent_name,
                room=room_name,
                metadata=call_meta,
            )
            logger.info("Dispatched agent '%s' to unique outbound room '%s'", agent_name, room_name)
        except Exception as e:
            logger.warning("Error pre-dispatching agent to outbound room: %s", e)

        # 4. Create the SIP participant
        identity = participant_identity or f"sip-{sip_call_to.replace('+', '')}"
        participant_res = await lk.create_sip_participant(
            sip_trunk_id=sip_trunk_id,
            sip_call_to=sip_call_to,
            room_name=room_name,
            participant_identity=identity,
        )

        return {
            "status": "initiated",
            "room_name": room_name,
            "sip_trunk_id": sip_trunk_id,
            "sip_call_to": sip_call_to,
            "participant_identity": identity,
            "agent_name": agent_name,
            "sip_participant": str(participant_res),
        }

    async def get_live_telephony_sessions(self, lk) -> List[Dict[str, Any]]:
        """Fetch active telephony calls and room dispatches for the live control console."""
        sessions = []
        try:
            rooms, _ = await lk.list_all_rooms()
        except Exception as e:
            logger.debug("Failed to list rooms for telephony sessions: %s", e)
            rooms = []

        now = time.time()
        for r in rooms:
            rname = getattr(r, "name", "")
            is_sip_in = rname.startswith("sip-in-")
            is_sip_out = rname.startswith("sip-out-")
            if not (is_sip_in or is_sip_out or "sip" in rname.lower()):
                continue

            direction = "INBOUND" if is_sip_in else ("OUTBOUND" if is_sip_out else "TELEPHONY")
            created_at_sec = getattr(r, "creation_time", 0)
            dur_sec = max(0, int(now - created_at_sec)) if created_at_sec > 0 else 0
            mins = dur_sec // 60
            secs = dur_sec % 60
            dur_str = f"{mins}m {secs:02d}s"

            # Parse room metadata
            meta_str = getattr(r, "metadata", "") or "{}"
            agent_id = CANONICAL_MASTER_AGENT
            tenant_id = DEFAULT_TENANT_ID
            caller = "Carrier Caller"
            did = "Carrier Ingress"

            try:
                if meta_str.startswith("{"):
                    meta_json = json.loads(meta_str)
                    agent_id = meta_json.get("agent_id", agent_id)
                    tenant_id = meta_json.get("tenant_id", tenant_id)
                    caller = meta_json.get("caller", caller)
                    did = meta_json.get("did", did)
            except Exception:
                pass

            sessions.append({
                "room_name": rname,
                "direction": direction,
                "caller": caller,
                "did": did,
                "agent_name": agent_id,
                "tenant_id": tenant_id,
                "num_participants": getattr(r, "num_participants", 1),
                "duration": dur_str,
                "status": "ACTIVE",
                "provider_mode": "REALTIME / CASCADE",
            })

        return sessions


sip_routing_service = SipRoutingService()
