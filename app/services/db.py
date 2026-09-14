"""WASID Telephony Database & Repository Layer.

Provides the PostgreSQL global source of truth for:
  - DID Routings (did <-> tenant <-> canonical agent <-> dispatch rule)
  - Call Records (unique room <-> direction <-> caller <-> callee <-> duration <-> lifecycle)

Supports PostgreSQL via asyncpg with graceful SQLite fallback for tests and local dev.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    select,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import declarative_base

logger = logging.getLogger(__name__)

Base = declarative_base()


class DidRoutingRecord(Base):
    """Authoritative DID routing table in PostgreSQL."""

    __tablename__ = "did_routings"

    did = Column(String(32), primary_key=True, index=True)
    tenant_id = Column(String(64), nullable=False, index=True, default="wasid-hq")
    tenant_name = Column(String(128), nullable=False, default="WASID HQ")
    agent_id = Column(String(64), nullable=False, default="wasid-ai-automation-master")
    provider = Column(String(32), nullable=False, default="vobiz")
    inbound_trunk_id = Column(String(64), nullable=True)
    dispatch_rule_id = Column(String(64), nullable=True)
    room_prefix = Column(String(32), nullable=False, default="sip-in-")
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "did": self.did,
            "tenant_id": self.tenant_id,
            "tenant_name": self.tenant_name,
            "agent_id": self.agent_id,
            "provider": self.provider,
            "inbound_trunk_id": self.inbound_trunk_id or "",
            "dispatch_rule_id": self.dispatch_rule_id or "",
            "room_prefix": self.room_prefix,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class CallRecord(Base):
    """Authoritative Call lifecycle table in PostgreSQL."""

    __tablename__ = "calls"

    call_id = Column(String(64), primary_key=True, index=True)
    tenant_id = Column(String(64), nullable=False, index=True, default="wasid-hq")
    room_name = Column(String(128), nullable=False, index=True)
    direction = Column(String(16), nullable=False, default="inbound")  # inbound / outbound
    caller_did = Column(String(32), nullable=False, default="")
    callee_did = Column(String(32), nullable=False, default="")
    agent_id = Column(String(64), nullable=False, default="wasid-ai-automation-master")
    duration_seconds = Column(Integer, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="initiated")  # initiated, active, completed, failed
    outcome = Column(String(64), nullable=True)
    transcript = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=False, default="{}")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    ended_at = Column(DateTime(timezone=True), nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "call_id": self.call_id,
            "tenant_id": self.tenant_id,
            "room_name": self.room_name,
            "direction": self.direction,
            "caller_did": self.caller_did,
            "callee_did": self.callee_did,
            "agent_id": self.agent_id,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "outcome": self.outcome,
            "transcript": self.transcript,
            "metadata_json": self.metadata_json,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }


# Canonical DID Seed Data derived from authoritative PostgreSQL tenant bindings
CANONICAL_DID_SEEDS = [
    {
        "did": "+971501234567",
        "tenant_id": "TGLX965152579",
        "tenant_name": "WASID HQ / Operations",
        "agent_id": "wasid-ai-automation-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_qgCxptTPBnyh",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+97141234567",
        "tenant_id": "TZEE794100737",
        "tenant_name": "FitZone Gym Dubai",
        "agent_id": "wasid-customer-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_8N7DJE97PAze",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+971501112233",
        "tenant_id": "TZEE794100737",
        "tenant_name": "FitZone Gym Dubai (VIP)",
        "agent_id": "wasid-customer-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_8N7DJE97PAze",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+97143435333",
        "tenant_id": "TSQZ905389656",
        "tenant_name": "Al Safadi Gourmet",
        "agent_id": "wasid-customer-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_8N7DJE97PAze",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+97143435334",
        "tenant_id": "TSQZ905389656",
        "tenant_name": "Al Safadi Gourmet (Reservations)",
        "agent_id": "wasid-customer-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_8N7DJE97PAze",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+97143624788",
        "tenant_id": "TBNY613619934",
        "tenant_name": "ABC Dental Clinic",
        "agent_id": "wasid-customer-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_8N7DJE97PAze",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+1800WASIDAI",
        "tenant_id": "TGLX965152579",
        "tenant_name": "WASID HQ International",
        "agent_id": "wasid-ai-automation-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_qgCxptTPBnyh",
        "room_prefix": "sip-in-",
    },
    {
        "did": "+91800WASIDAI",
        "tenant_id": "TGLX965152579",
        "tenant_name": "WASID India Operations",
        "agent_id": "wasid-ai-automation-master",
        "provider": "vobiz",
        "inbound_trunk_id": "ST_kcrc2jpfVgJ8",
        "dispatch_rule_id": "SDR_qgCxptTPBnyh",
        "room_prefix": "sip-in-",
    },
]


class TelephonyDatabase:
    """Async database repository for LiveKit Telephony routing and calls."""

    def __init__(self):
        self._engine: Optional[AsyncEngine] = None
        self._sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None
        self._initialized: bool = False

    def get_database_url(self) -> str:
        """Resolve database URL, adapting PostgreSQL URLs to asyncpg."""
        url = os.getenv("DATABASE_URL")
        if url:
            if url.startswith("postgres://"):
                url = url.replace("postgres://", "postgresql+asyncpg://", 1)
            elif url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
                url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
            return url

        # Fallback to local SQLite
        db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "wasid_telephony.db"))
        return f"sqlite+aiosqlite:///{db_path}"

    def get_engine(self) -> AsyncEngine:
        if self._engine is None:
            url = self.get_database_url()
            connect_args = {}
            if "sqlite" in url:
                connect_args["check_same_thread"] = False
            self._engine = create_async_engine(
                url,
                echo=False,
                future=True,
                connect_args=connect_args,
            )
        return self._engine

    def get_sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        if self._sessionmaker is None:
            engine = self.get_engine()
            self._sessionmaker = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False,
            )
        return self._sessionmaker

    async def init_db(self) -> None:
        """Initialize database schema and seed canonical DIDs."""
        if self._initialized:
            return

        engine = self.get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Seed initial canonical DIDs and migrate legacy tenant IDs
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                result = await session.execute(select(DidRoutingRecord))
                existing = {row.did: row for row in result.scalars()}

                # Purge legacy mock tenant records
                for did, row in list(existing.items()):
                    if row.tenant_id == "CIT49119004":
                        await session.delete(row)
                        del existing[did]

                for seed in CANONICAL_DID_SEEDS:
                    if seed["did"] not in existing:
                        record = DidRoutingRecord(
                            did=seed["did"],
                            tenant_id=seed["tenant_id"],
                            tenant_name=seed["tenant_name"],
                            agent_id=seed["agent_id"],
                            provider=seed["provider"],
                            inbound_trunk_id=seed.get("inbound_trunk_id"),
                            dispatch_rule_id=seed.get("dispatch_rule_id"),
                            room_prefix=seed.get("room_prefix", "sip-in-"),
                            is_active=True,
                        )
                        session.add(record)
                    else:
                        # Migrate legacy tenant_id if it differs from canonical
                        row = existing[seed["did"]]
                        if row.tenant_id != seed["tenant_id"]:
                            row.tenant_id = seed["tenant_id"]
                            row.tenant_name = seed["tenant_name"]
                            row.agent_id = seed["agent_id"]

        self._initialized = True
        logger.info(
            "Telephony database initialized successfully (%s)",
            "PostgreSQL" if "postgresql" in self.get_database_url() else "SQLite",
        )

    async def get_all_did_routings(self) -> List[Dict[str, Any]]:
        """Retrieve all DID routings ordered by DID."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(DidRoutingRecord).order_by(DidRoutingRecord.did)
            result = await session.execute(stmt)
            return [row.to_dict() for row in result.scalars()]

    async def get_did_routing(self, did: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific DID routing."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(DidRoutingRecord).where(DidRoutingRecord.did == did)
            result = await session.execute(stmt)
            record = result.scalars().first()
            return record.to_dict() if record else None

    async def upsert_did_routing(
        self,
        did: str,
        agent_id: str,
        tenant_id: Optional[str] = None,
        tenant_name: Optional[str] = None,
        provider: str = "vobiz",
        inbound_trunk_id: Optional[str] = None,
        dispatch_rule_id: Optional[str] = None,
        room_prefix: str = "sip-in-",
        is_active: bool = True,
    ) -> Dict[str, Any]:
        """Insert or update a DID routing."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(DidRoutingRecord).where(DidRoutingRecord.did == did)
                result = await session.execute(stmt)
                record = result.scalars().first()

                now = datetime.now(timezone.utc)
                if record:
                    record.agent_id = agent_id
                    if tenant_id:
                        record.tenant_id = tenant_id
                    if tenant_name:
                        record.tenant_name = tenant_name
                    if inbound_trunk_id:
                        record.inbound_trunk_id = inbound_trunk_id
                    if dispatch_rule_id:
                        record.dispatch_rule_id = dispatch_rule_id
                    record.provider = provider
                    record.room_prefix = room_prefix
                    record.is_active = is_active
                    record.updated_at = now
                else:
                    record = DidRoutingRecord(
                        did=did,
                        tenant_id=tenant_id or "wasid-hq",
                        tenant_name=tenant_name or "WASID HQ",
                        agent_id=agent_id,
                        provider=provider,
                        inbound_trunk_id=inbound_trunk_id,
                        dispatch_rule_id=dispatch_rule_id,
                        room_prefix=room_prefix,
                        is_active=is_active,
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(record)

                await session.flush()
                return record.to_dict()

    async def record_call_start(
        self,
        call_id: str,
        room_name: str,
        direction: str,
        caller_did: str,
        callee_did: str,
        agent_id: str,
        tenant_id: str = "wasid-hq",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record the start of a call in the PostgreSQL database."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                record = CallRecord(
                    call_id=call_id,
                    tenant_id=tenant_id,
                    room_name=room_name,
                    direction=direction,
                    caller_did=caller_did,
                    callee_did=callee_did,
                    agent_id=agent_id,
                    duration_seconds=0,
                    status="active",
                    outcome="in_progress",
                    metadata_json=json.dumps(metadata or {}),
                    created_at=datetime.now(timezone.utc),
                )
                session.add(record)
                await session.flush()
                return record.to_dict()

    async def record_call_end(
        self,
        call_id: str,
        duration_seconds: int = 0,
        status: str = "completed",
        outcome: Optional[str] = None,
        transcript: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Record the completion of a call."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(CallRecord).where(CallRecord.call_id == call_id)
                result = await session.execute(stmt)
                record = result.scalars().first()
                if not record:
                    return None

                record.duration_seconds = duration_seconds
                record.status = status
                record.outcome = outcome or ("completed" if status == "completed" else "failed")
                if transcript:
                    record.transcript = transcript
                record.ended_at = datetime.now(timezone.utc)
                await session.flush()
                return record.to_dict()

    async def list_recent_calls(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List recent calls ordered by start time descending."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecord).order_by(CallRecord.created_at.desc()).limit(limit)
            result = await session.execute(stmt)
            return [row.to_dict() for row in result.scalars()]


# Singleton instance
telephony_db = TelephonyDatabase()
