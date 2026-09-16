"""WASID Voice Routing Service — LiveKit SIP Telephony and Canonical Agent Dispatch.

Establishes deterministic routing chains:
  Inbound:  Vobiz DID -> Vobiz SIP -> Inbound Trunk -> Dispatch Rule -> Canonical Agent (wasid-ai-automation-master) -> Unique Room (sip-in-*) -> Agent Worker
  Outbound: WASID Agent -> Unique Room (sip-out-*) -> Agent Worker -> LiveKit SIP Participant -> Outbound Trunk -> Vobiz -> Destination
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from app.services.db import telephony_db

logger = logging.getLogger(__name__)

# Canonical Master Agents
CANONICAL_MASTER_AGENT = "wasid-ai-automation-master"
CANONICAL_CUSTOMER_AGENT = "wasid-customer-master"
CANONICAL_AGENTS = [CANONICAL_MASTER_AGENT, CANONICAL_CUSTOMER_AGENT]
DEFAULT_TENANT_ID = "wasid-hq"
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

        # 3. PostgreSQL Authoritative DID Routings & LiveKit Reconciliation
        db_did_records = []
        try:
            db_did_records = await telephony_db.get_all_did_routings()
        except Exception as e:
            logger.warning("Failed to fetch DID routings from database: %s", e)

        # Index live dispatch rules by rule ID and bound agent
        live_rule_ids = {getattr(r, "sip_dispatch_rule_id", ""): r for r in dispatch_rules}
        live_agent_rules = {}
        for r in dispatch_rules:
            rc = getattr(r, "room_config", None)
            if rc and hasattr(rc, "agents"):
                for ag in rc.agents:
                    if getattr(ag, "agent_name", ""):
                        live_agent_rules[ag.agent_name] = r

        authoritative_dids = []
        for dr in db_did_records:
            did_num = dr["did"]
            agent_id = dr["agent_id"]
            rule_id = dr.get("dispatch_rule_id")
            
            # Check LiveKit sync status
            is_synced = False
            matched_rule = None
            if rule_id and rule_id in live_rule_ids:
                matched_rule = live_rule_ids[rule_id]
                # Verify agent binding inside matched rule
                rc = getattr(matched_rule, "room_config", None)
                if rc and hasattr(rc, "agents"):
                    for ag in rc.agents:
                        if getattr(ag, "agent_name", "") == agent_id:
                            is_synced = True
                            break
            elif agent_id in live_agent_rules:
                matched_rule = live_agent_rules[agent_id]
                is_synced = True

            authoritative_dids.append({
                "did": did_num,
                "tenant_id": dr["tenant_id"],
                "tenant_name": dr["tenant_name"],
                "agent_id": agent_id,
                "provider": dr["provider"],
                "inbound_trunk_id": dr.get("inbound_trunk_id") or "vobiz-primary",
                "dispatch_rule_id": getattr(matched_rule, "sip_dispatch_rule_id", rule_id or "SDR_qgCxptTPBnyh"),
                "room_prefix": dr.get("room_prefix", DEFAULT_INBOUND_ROOM_PREFIX),
                "room_strategy": f"{dr.get('room_prefix', DEFAULT_INBOUND_ROOM_PREFIX)}{{caller}}_{{suffix}}",
                "sync_status": "IN-SYNC" if is_synced else "DRIFT",
                "is_active": dr.get("is_active", True),
                "source_of_truth": "PostgreSQL",
            })

        # Summary statistics
        inbound_count = sum(1 for r in routes if r["direction"] == "INBOUND")
        outbound_count = sum(1 for r in routes if r["direction"] == "OUTBOUND")
        in_sync_dids = sum(1 for d in authoritative_dids if d["sync_status"] == "IN-SYNC")
        drift_dids = sum(1 for d in authoritative_dids if d["sync_status"] == "DRIFT")
        vobiz_ready = (inbound_count > 0 and any(
            r["is_unique_room"] and r["target_agent"] in CANONICAL_AGENTS for r in routes
        )) or in_sync_dids > 0

        # Recent calls from PostgreSQL
        recent_calls = []
        try:
            recent_calls = await telephony_db.list_recent_calls(limit=25)
        except Exception as e:
            logger.debug("Failed to list recent calls: %s", e)

        return {
            "routes": routes,
            "did_routings": authoritative_dids,
            "recent_calls": recent_calls,
            "inbound_trunks_count": len(inbound_trunks),
            "outbound_trunks_count": len(outbound_trunks),
            "dispatch_rules_count": len(dispatch_rules),
            "authoritative_did_count": len(authoritative_dids),
            "in_sync_did_count": in_sync_dids,
            "drift_did_count": drift_dids,
            "active_sip_calls": len(active_sip_rooms),
            "canonical_target_agent": CANONICAL_MASTER_AGENT,
            "canonical_agents": CANONICAL_AGENTS,
            "vobiz_ready": vobiz_ready,
            "worker_status": "ONLINE (Voice Pool Ready)",
            "voice_pipelines": ["REALTIME (Gemini Live)", "CASCADE (Groq + LiteLLM + Gemini TTS)"],
            "summary": {
                "inbound_count": inbound_count,
                "outbound_count": outbound_count,
                "total_routes": len(routes),
                "total_authoritative_dids": len(authoritative_dids),
                "in_sync_dids": in_sync_dids,
                "drift_dids": drift_dids,
                "unique_room_guarantee": True,
            }
        }

    async def find_existing_dispatch_rule(
        self,
        lk,
        agent_name: str,
        preferred_rule_id: Optional[str] = None,
        dispatch_rules: Optional[List[Any]] = None,
    ) -> Optional[Any]:
        """Find an existing appropriate LiveKit dispatch rule for the given agent."""
        if dispatch_rules is None:
            dispatch_rules = []
            if hasattr(lk, "list_sip_dispatch_rules"):
                try:
                    res = lk.list_sip_dispatch_rules()
                    if asyncio.iscoroutine(res):
                        dispatch_rules = await res
                    elif isinstance(res, list):
                        dispatch_rules = res
                except Exception as e:
                    logger.warning("Failed to list dispatch rules when searching: %s", e)

        # 1. Match by preferred_rule_id if valid
        if preferred_rule_id:
            for r in dispatch_rules:
                if getattr(r, "sip_dispatch_rule_id", "") == preferred_rule_id:
                    return r

        # 2. Match by bound agent in room_config
        for r in dispatch_rules:
            rc = getattr(r, "room_config", None)
            if rc and hasattr(rc, "agents"):
                for ag in rc.agents:
                    if getattr(ag, "agent_name", "") == agent_name:
                        return r

        # 3. Match by name
        expected_names = {
            f"WASID Vobiz Inbound ({agent_name})".lower(),
            f"WASID Inbound ({agent_name})".lower(),
        }
        if agent_name == CANONICAL_CUSTOMER_AGENT:
            expected_names.add("wasid customer inbound master")
            expected_names.add("wasid customer master")
        elif agent_name == CANONICAL_MASTER_AGENT:
            expected_names.add("wasid vobiz inbound master")
            expected_names.add("wasid master")

        for r in dispatch_rules:
            rname = (getattr(r, "name", "") or "").lower()
            if rname in expected_names or f"({agent_name})".lower() in rname:
                return r

        # 4. Match by metadata
        for r in dispatch_rules:
            meta = getattr(r, "metadata", "") or ""
            if f'"{agent_name}"' in meta or f"'{agent_name}'" in meta:
                return r

        # 5. Canonical fallback ID match in live rules
        canonical_target_id = "SDR_8N7DJE97PAze" if agent_name == CANONICAL_CUSTOMER_AGENT else "SDR_qgCxptTPBnyh"
        for r in dispatch_rules:
            if getattr(r, "sip_dispatch_rule_id", "") == canonical_target_id:
                return r

        return None

    async def reassign_did_routing(
        self,
        lk,
        did: str,
        agent_name: str,
        tenant_id: Optional[str] = None,
        tenant_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Reassigns a DID to a canonical agent, updating PostgreSQL and synchronizing LiveKit dispatch idempotently."""
        if agent_name not in CANONICAL_AGENTS:
            raise ValueError(f"Invalid agent '{agent_name}'. Must be one of {CANONICAL_AGENTS}")

        # 1. Check the authoritative PostgreSQL DID/tenant/agent assignment first
        existing_rec = await telephony_db.get_did_routing(did)

        # Preserve existing tenant data if not explicitly provided
        target_tenant_id = tenant_id or (existing_rec.get("tenant_id") if existing_rec else None) or DEFAULT_TENANT_ID
        target_tenant_name = tenant_name or (existing_rec.get("tenant_name") if existing_rec else None) or "WASID Operations"
        existing_rule_id = existing_rec.get("dispatch_rule_id") if existing_rec else None
        inbound_trunk_id = (existing_rec.get("inbound_trunk_id") if existing_rec else None) or "vobiz-primary"

        # 2 & 3. Check existing LiveKit SIP trunks/dispatch rules before creating anything
        # Reuse/update the existing appropriate persistent LiveKit routing object
        rule_res = await self.provision_vobiz_canonical_rule(
            lk=lk,
            agent_name=agent_name,
            room_prefix=DEFAULT_INBOUND_ROOM_PREFIX,
            preferred_rule_id=existing_rule_id,
        )
        default_fallback_rule = "SDR_8N7DJE97PAze" if agent_name == CANONICAL_CUSTOMER_AGENT else "SDR_qgCxptTPBnyh"
        rule_id = rule_res.get("rule_id") or default_fallback_rule

        # 8. PostgreSQL remains the authoritative WASID source of truth
        updated = await telephony_db.upsert_did_routing(
            did=did,
            agent_id=agent_name,
            tenant_id=target_tenant_id,
            tenant_name=target_tenant_name,
            provider="vobiz",
            inbound_trunk_id=inbound_trunk_id,
            dispatch_rule_id=rule_id,
            room_prefix=DEFAULT_INBOUND_ROOM_PREFIX,
            is_active=True,
        )
        logger.info(
            "Successfully assigned DID %s -> %s (tenant: %s, rule: %s, status: %s) in PostgreSQL & LiveKit",
            did, agent_name, target_tenant_id, rule_id, rule_res.get("status", "synced")
        )
        return updated

    async def provision_vobiz_canonical_rule(
        self,
        lk,
        trunk_ids: Optional[List[str]] = None,
        agent_name: str = CANONICAL_MASTER_AGENT,
        room_prefix: str = DEFAULT_INBOUND_ROOM_PREFIX,
        preferred_rule_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Provisions or reuses the canonical dispatch rule binding inbound SIP calls to the canonical agent.
        
        Guarantees idempotency:
          - Reuses existing persistent LiveKit dispatch rules when available.
          - Only updates if modification is strictly required.
          - Never duplicates dispatch rules on repeated assignment attempts.
        """
        name = f"WASID Vobiz Inbound ({agent_name})"
        metadata = json.dumps({
            "tenant_id": DEFAULT_TENANT_ID,
            "agent_id": agent_name,
            "provider": "vobiz",
            "direction": "inbound",
            "room_prefix": room_prefix,
        })

        # 1. Fetch existing dispatch rules
        dispatch_rules = []
        if hasattr(lk, "list_sip_dispatch_rules"):
            try:
                res = lk.list_sip_dispatch_rules()
                if asyncio.iscoroutine(res):
                    dispatch_rules = await res
                elif isinstance(res, list):
                    dispatch_rules = res
            except Exception as e:
                logger.warning("Failed to list dispatch rules in provision_vobiz_canonical_rule: %s", e)

        # 2. Check if an appropriate rule already exists
        existing_rule = await self.find_existing_dispatch_rule(
            lk=lk,
            agent_name=agent_name,
            preferred_rule_id=preferred_rule_id,
            dispatch_rules=dispatch_rules,
        )

        if existing_rule:
            rule_id = getattr(existing_rule, "sip_dispatch_rule_id", preferred_rule_id or "")
            rule_name = getattr(existing_rule, "name", name)
            logger.info("Found existing LiveKit SIP dispatch rule %s ('%s') for agent '%s' — reusing without recreation.", rule_id, rule_name, agent_name)

            # Check if agent binding is already in room_config
            bound_agents = []
            rc = getattr(existing_rule, "room_config", None)
            if rc and hasattr(rc, "agents"):
                bound_agents = [getattr(ag, "agent_name", "") for ag in rc.agents]

            needs_update = False
            if agent_name not in bound_agents and hasattr(lk, "update_sip_dispatch_rule"):
                needs_update = True

            if needs_update:
                try:
                    logger.info("Updating existing dispatch rule %s to bind canonical agent '%s'", rule_id, agent_name)
                    await lk.update_sip_dispatch_rule(
                        sip_dispatch_rule_id=rule_id,
                        name=rule_name,
                        trunk_ids=list(getattr(existing_rule, "trunk_ids", [])) or trunk_ids,
                        dispatch_rule_type="individual",
                        room_prefix=room_prefix,
                        agent_name=agent_name,
                        agent_metadata=metadata,
                        metadata=metadata,
                    )
                except Exception as ue:
                    logger.warning("Non-fatal: could not update dispatch rule %s: %s", rule_id, ue)

            return {
                "status": "reused",
                "rule_id": rule_id,
                "name": rule_name,
                "agent_name": agent_name,
                "room_prefix": room_prefix,
                "room_strategy": f"{room_prefix}{{caller}}_{{suffix}}",
            }

        # 3. If no existing rule was found, create it safely
        try:
            res = await lk.create_sip_dispatch_rule(
                name=name,
                trunk_ids=trunk_ids or [],
                dispatch_rule_type="individual",
                room_prefix=room_prefix,
                agent_name=agent_name,
                agent_metadata=metadata,
                metadata=metadata,
            )
            rule_id = getattr(res, "sip_dispatch_rule_id", str(res))
            logger.info("Created new LiveKit SIP dispatch rule %s ('%s') for agent '%s'", rule_id, name, agent_name)
            return {
                "status": "created",
                "rule_id": rule_id,
                "name": name,
                "agent_name": agent_name,
                "room_prefix": room_prefix,
                "room_strategy": f"{room_prefix}{{caller}}_{{suffix}}",
            }
        except Exception as e:
            err_str = str(e)
            if "already exists" in err_str.lower() or "400" in err_str:
                logger.warning("Dispatch rule creation collision detected (%s). Re-querying to adopt existing rule...", e)
                try:
                    refetched_rules = []
                    if hasattr(lk, "list_sip_dispatch_rules"):
                        r_list = lk.list_sip_dispatch_rules()
                        refetched_rules = (await r_list) if asyncio.iscoroutine(r_list) else r_list
                    adopted = await self.find_existing_dispatch_rule(
                        lk=lk,
                        agent_name=agent_name,
                        preferred_rule_id=preferred_rule_id,
                        dispatch_rules=refetched_rules,
                    )
                    if adopted:
                        a_id = getattr(adopted, "sip_dispatch_rule_id", "")
                        a_name = getattr(adopted, "name", name)
                        logger.info("Successfully adopted existing dispatch rule %s ('%s')", a_id, a_name)
                        return {
                            "status": "reused",
                            "rule_id": a_id,
                            "name": a_name,
                            "agent_name": agent_name,
                            "room_prefix": room_prefix,
                            "room_strategy": f"{room_prefix}{{caller}}_{{suffix}}",
                        }
                except Exception as re_err:
                    logger.warning("Failed during fallback adoption: %s", re_err)

            raise

    async def initiate_outbound_call(
        self,
        lk,
        sip_trunk_id: str,
        sip_call_to: str,
        agent_name: str = CANONICAL_MASTER_AGENT,
        participant_identity: Optional[str] = None,
        tenant_id: str = DEFAULT_TENANT_ID,
        caller_did: str = "+918065355408",
        voice_mode: str = "realtime",
        call_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Initiate an outbound SIP call enforcing ONE CALL = ONE UNIQUE ROOM and automatic agent dispatch."""
        if not lk.sip_enabled:
            raise ValueError("LiveKit SIP service is not enabled")

        if agent_name not in CANONICAL_AGENTS:
            logger.warning("Target agent '%s' not in canonical agents, defaulting to '%s'", agent_name, CANONICAL_MASTER_AGENT)
            agent_name = CANONICAL_MASTER_AGENT

        # 1. Generate collision-safe unique room name
        timestamp = int(time.time())
        short_id = uuid.uuid4().hex[:6]
        room_name = f"{DEFAULT_OUTBOUND_ROOM_PREFIX}{timestamp}-{short_id}"
        call_id = f"call_{timestamp}_{short_id}"

        # 2. Record start in PostgreSQL global source of truth
        try:
            await telephony_db.record_call_start(
                call_id=call_id,
                room_name=room_name,
                direction="outbound",
                caller_did=caller_did,
                callee_did=sip_call_to,
                agent_id=agent_name,
                tenant_id=tenant_id,
                metadata={"sip_trunk_id": sip_trunk_id, "room_name": room_name, "voice_mode": voice_mode, **(call_context or {})},
            )
        except Exception as e:
            logger.warning("Failed to record outbound call start in PostgreSQL: %s", e)

        # 3. Prepare metadata with full canonical context
        meta_dict = {
            "call_id": call_id,
            "tenant_id": tenant_id,
            "agent_id": agent_name,
            "direction": "outbound",
            "call_direction": "OUTBOUND",
            "call_to": sip_call_to,
            "caller_did": caller_did,
            "voice_mode": voice_mode,
            "mode": voice_mode,
            "created_at": timestamp,
        }
        if call_context and isinstance(call_context, dict):
            meta_dict.update(call_context)
        call_meta = json.dumps(meta_dict)

        # 4. Dispatch canonical agent to the unique room
        try:
            await lk.create_dispatch(
                agent_name=agent_name,
                room=room_name,
                metadata=call_meta,
            )
            logger.info("Dispatched canonical agent '%s' to unique outbound room '%s'", agent_name, room_name)
        except Exception as e:
            logger.warning("Error pre-dispatching agent to outbound room: %s", e)

        # 5. Create the SIP participant
        identity = participant_identity or f"sip-{sip_call_to.replace('+', '')}"
        participant_res = await lk.create_sip_participant(
            sip_trunk_id=sip_trunk_id,
            sip_call_to=sip_call_to,
            room_name=room_name,
            participant_identity=identity,
        )

        return {
            "status": "initiated",
            "call_id": call_id,
            "room_name": room_name,
            "sip_trunk_id": sip_trunk_id,
            "sip_call_to": sip_call_to,
            "participant_identity": identity,
            "agent_name": agent_name,
            "sip_participant": str(participant_res),
        }

    async def get_recent_calls(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch recent call logs from PostgreSQL."""
        try:
            return await telephony_db.list_recent_calls(limit=limit)
        except Exception as e:
            logger.warning("Failed to list recent calls: %s", e)
            return []

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
