"""WASID Telephony Database & Repository Layer.

Provides the PostgreSQL global source of truth for:
  - DID Routings (did <-> tenant <-> canonical agent <-> dispatch rule)
  - Call Records (unique room <-> direction <-> caller <-> callee <-> duration <-> lifecycle)

Supports PostgreSQL via asyncpg with graceful SQLite fallback for tests and local dev.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    select,
    func,
    desc,
    or_,
    text,
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


class CallRecordingRecord(Base):
    """Authoritative Call Recording table in PostgreSQL."""

    __tablename__ = "call_recordings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    recording_id = Column(String(64), unique=True, index=True, nullable=False)
    call_id = Column(String(64), index=True, nullable=True)
    room_name = Column(String(128), index=True, nullable=False)
    egress_id = Column(String(64), index=True, nullable=True)
    direction = Column(String(16), nullable=False, default="inbound")  # inbound / outbound
    caller_number = Column(String(32), nullable=False, default="")
    callee_number = Column(String(32), nullable=False, default="")
    did_number = Column(String(32), nullable=False, default="")
    tenant_id = Column(String(64), nullable=False, index=True, default="wasid-hq")
    agent_id = Column(String(64), nullable=False, default="wasid-ai-automation-master")
    status = Column(String(32), nullable=False, default="recording")  # recording, completed, failed, deleted
    duration_seconds = Column(Integer, nullable=False, default=0)
    file_size_bytes = Column(Integer, nullable=False, default=0)
    storage_provider = Column(String(32), nullable=False, default="cloudflare_r2")
    storage_bucket = Column(String(128), nullable=False, default="n8n-production-backups")
    storage_object_key = Column(String(256), nullable=True)
    media_url = Column(String(512), nullable=True)
    started_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    ended_at = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=False, default="{}")
    transcription = Column(Text, nullable=True)
    transcription_status = Column(String(32), nullable=False, default="pending")  # pending, transcribing, completed, failed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "recording_id": self.recording_id,
            "call_id": self.call_id,
            "room_name": self.room_name,
            "egress_id": self.egress_id,
            "direction": self.direction,
            "caller_number": self.caller_number,
            "callee_number": self.callee_number,
            "did_number": self.did_number,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "status": self.status,
            "duration_seconds": self.duration_seconds,
            "file_size_bytes": self.file_size_bytes,
            "storage_provider": self.storage_provider,
            "storage_bucket": self.storage_bucket,
            "storage_object_key": self.storage_object_key,
            "media_url": self.media_url,
            "download_url": f"https://lkdashboard.wasidai.com/api/v1/egress/{self.recording_id}/download" if self.recording_id else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "error_message": self.error_message,
            "metadata_json": self.metadata_json,
            "transcription": self.transcription,
            "transcription_status": self.transcription_status or "pending",
        }


# No synthetic or fabricated seed DIDs in production. Authoritative data flows solely from PostgreSQL.
CANONICAL_DID_SEEDS: List[Dict[str, Any]] = []

SYNTHETIC_SEED_DIDS = {
    "+971501234567",
    "+97141234567",
    "+971501112233",
    "+97143435333",
    "+97143435334",
    "+97143624788",
    "+1800WASIDAI",
    "+91800WASIDAI",
    "+18005559999",
}


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

        # Fallback to local SQLite for local testing only
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
        """Initialize database schema and purge legacy synthetic seed DIDs."""
        if self._initialized:
            return

        engine = self.get_engine()
        db_url = self.get_database_url()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            if "postgresql" in db_url:
                try:
                    await conn.execute(text("ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS transcription TEXT;"))
                    await conn.execute(text("ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS transcription_status VARCHAR(32) DEFAULT 'pending';"))
                except Exception as ex:
                    logger.debug("PostgreSQL column migration notice: %s", ex)
            else:
                try:
                    await conn.execute(text("ALTER TABLE call_recordings ADD COLUMN transcription TEXT;"))
                except Exception:
                    pass
                try:
                    await conn.execute(text("ALTER TABLE call_recordings ADD COLUMN transcription_status VARCHAR(32) DEFAULT 'pending';"))
                except Exception:
                    pass

        # Purge legacy mock/synthetic seed records from database
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                result = await session.execute(select(DidRoutingRecord))
                existing = {row.did: row for row in result.scalars()}

                for did, row in list(existing.items()):
                    if did in SYNTHETIC_SEED_DIDS or row.tenant_id in ("CIT49119004", "TZEE794100737"):
                        await session.delete(row)
                        del existing[did]

                # Backfill historical recordings: Tenant ID and dual-sided contact numbers
                recs_res = await session.execute(select(CallRecordingRecord))
                for rec in recs_res.scalars():
                    rname = rec.room_name or ""
                    is_inbound = rec.direction == "inbound"
                    match = re.search(r'(?:sip-in|call-out|sip-out)[-_]+(?:\+)?(\d{10,15})', rname)
                    phone = f"+{match.group(1)}" if match else ""

                    # 1. Tenant ID attribution: for admin DID or missing tenant, attribute to ADMIN
                    if not rec.tenant_id or rec.tenant_id in ("wasid-hq", "default", "None", "") or rec.did_number == "+918065355408":
                        rec.tenant_id = "ADMIN"

                    # 2. Contact numbers: Inbound vs Outbound
                    if is_inbound:
                        # Inbound Call:
                        # Caller (From) = Customer Phone Number
                        # Receiver (To) = DID Number (+918065355408)
                        if phone and phone != "+918065355408":
                            rec.caller_number = phone
                        elif not rec.caller_number or rec.caller_number in ("Inbound Caller", "+918065355408"):
                            rec.caller_number = phone or "+918009128306"
                        rec.callee_number = "+918065355408"
                        rec.did_number = "+918065355408"
                    else:
                        # Outbound Call:
                        # Caller (From) = DID Number (+918065355408)
                        # Receiver (To) = Customer Destination Phone Number
                        rec.caller_number = "+918065355408"
                        rec.did_number = "+918065355408"
                        if phone and phone != "+918065355408":
                            rec.callee_number = phone
                        elif not rec.callee_number or rec.callee_number in ("+918065355408", "Inbound Caller"):
                            rec.callee_number = phone or "+919876543210"

                    if not rec.transcription_status:
                        rec.transcription_status = "pending"

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
        """Record the completion of a call by call_id or room_name."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(CallRecord).where(
                    or_(CallRecord.call_id == call_id, CallRecord.room_name == call_id)
                )
                result = await session.execute(stmt)
                record = result.scalars().first()
                if not record:
                    return None

                if duration_seconds > 0 or record.duration_seconds == 0:
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

    # Recording Management
    async def create_recording(
        self,
        recording_id: str,
        room_name: str,
        direction: str = "inbound",
        call_id: Optional[str] = None,
        egress_id: Optional[str] = None,
        caller_number: str = "",
        callee_number: str = "",
        did_number: str = "",
        tenant_id: str = "wasid-hq",
        agent_id: str = "wasid-ai-automation-master",
        status: str = "recording",
        storage_bucket: str = "n8n-production-backups",
        storage_object_key: Optional[str] = None,
        duration_seconds: int = 0,
        file_size_bytes: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a new call recording record in PostgreSQL."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                record = CallRecordingRecord(
                    recording_id=recording_id,
                    call_id=call_id,
                    room_name=room_name,
                    egress_id=egress_id,
                    direction=direction,
                    caller_number=caller_number,
                    callee_number=callee_number,
                    did_number=did_number,
                    tenant_id=tenant_id,
                    agent_id=agent_id,
                    status=status,
                    duration_seconds=duration_seconds,
                    file_size_bytes=file_size_bytes,
                    storage_provider="cloudflare_r2",
                    storage_bucket=storage_bucket,
                    storage_object_key=storage_object_key,
                    started_at=datetime.now(timezone.utc),
                    metadata_json=json.dumps(metadata or {}),
                )
                session.add(record)
                await session.flush()
                return record.to_dict()

    async def update_recording(
        self,
        recording_id: str,
        egress_id: Optional[str] = None,
        status: Optional[str] = None,
        duration_seconds: Optional[int] = None,
        file_size_bytes: Optional[int] = None,
        storage_object_key: Optional[str] = None,
        media_url: Optional[str] = None,
        ended_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update an existing call recording record."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(CallRecordingRecord).where(CallRecordingRecord.recording_id == recording_id)
                result = await session.execute(stmt)
                record = result.scalars().first()
                if not record:
                    return None

                if egress_id is not None:
                    record.egress_id = egress_id
                if status is not None:
                    record.status = status
                if duration_seconds is not None:
                    record.duration_seconds = duration_seconds
                if file_size_bytes is not None:
                    record.file_size_bytes = file_size_bytes
                if storage_object_key is not None:
                    record.storage_object_key = storage_object_key
                if media_url is not None:
                    record.media_url = media_url
                if ended_at is not None:
                    record.ended_at = ended_at
                elif status in ("completed", "failed", "deleted") and not record.ended_at:
                    record.ended_at = datetime.now(timezone.utc)
                if error_message is not None:
                    record.error_message = error_message
                if metadata is not None:
                    record.metadata_json = json.dumps(metadata)

                await session.flush()
                return record.to_dict()

    async def get_recording(self, recording_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a specific recording by its canonical recording_id."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord).where(CallRecordingRecord.recording_id == recording_id)
            result = await session.execute(stmt)
            record = result.scalars().first()
            return record.to_dict() if record else None

    async def get_recording_by_egress_id(self, egress_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a recording by its LiveKit egress ID."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord).where(CallRecordingRecord.egress_id == egress_id)
            result = await session.execute(stmt)
            record = result.scalars().first()
            return record.to_dict() if record else None

    async def get_recording_by_room(self, room_name: str) -> Optional[Dict[str, Any]]:
        """Fetch the latest recording for a given room name."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = (
                select(CallRecordingRecord)
                .where(CallRecordingRecord.room_name == room_name)
                .order_by(CallRecordingRecord.started_at.desc())
            )
            result = await session.execute(stmt)
            record = result.scalars().first()
            return record.to_dict() if record else None

    async def list_recordings(
        self,
        direction: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        tenant_id: Optional[str] = None,
        did: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List recordings with optional filtering and pagination."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord).where(CallRecordingRecord.status != "deleted")
            if direction and direction.lower() != "all":
                stmt = stmt.where(CallRecordingRecord.direction == direction.lower())
            if status and status.lower() != "all":
                stmt = stmt.where(CallRecordingRecord.status == status.lower())
            if tenant_id and tenant_id.lower() != "all":
                if tenant_id.upper() == "ADMIN":
                    stmt = stmt.where(
                        or_(
                            CallRecordingRecord.tenant_id == "ADMIN",
                            CallRecordingRecord.tenant_id == "wasid-hq",
                            CallRecordingRecord.did_number == "+918065355408",
                        )
                    )
                else:
                    stmt = stmt.where(CallRecordingRecord.tenant_id.ilike(f"%{tenant_id.strip()}%"))
            if did and did.lower() != "all":
                did_clean = did.strip().replace(" ", "")
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.did_number.ilike(f"%{did_clean}%"),
                        CallRecordingRecord.caller_number.ilike(f"%{did_clean}%"),
                        CallRecordingRecord.callee_number.ilike(f"%{did_clean}%"),
                    )
                )
            if search:
                pattern = f"%{search.strip()}%"
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.caller_number.ilike(pattern),
                        CallRecordingRecord.callee_number.ilike(pattern),
                        CallRecordingRecord.did_number.ilike(pattern),
                        CallRecordingRecord.tenant_id.ilike(pattern),
                        CallRecordingRecord.room_name.ilike(pattern),
                        CallRecordingRecord.recording_id.ilike(pattern),
                        CallRecordingRecord.egress_id.ilike(pattern),
                        CallRecordingRecord.transcription.ilike(pattern),
                    )
                )
            stmt = stmt.order_by(CallRecordingRecord.started_at.desc()).limit(limit).offset(offset)
            result = await session.execute(stmt)
            return [row.to_dict() for row in result.scalars()]

    async def get_distinct_tenants(self) -> List[str]:
        """Get unique tenant IDs from recordings and routings."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord.tenant_id).where(CallRecordingRecord.status != "deleted").distinct()
            res = await session.execute(stmt)
            tenants = set(r for r in res.scalars() if r)
            tenants.add("ADMIN")
            return sorted(list(tenants))

    async def get_distinct_dids(self) -> List[str]:
        """Get unique DID numbers from recordings and routings."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord.did_number).where(CallRecordingRecord.status != "deleted").distinct()
            res = await session.execute(stmt)
            dids = set(r for r in res.scalars() if r)
            dids.add("+918065355408")
            return sorted(list(dids))

    async def update_recording_transcription(
        self,
        recording_id: str,
        transcription: str,
        status: str = "completed",
    ) -> bool:
        """Update transcription text and status for a recording."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(CallRecordingRecord).where(CallRecordingRecord.recording_id == recording_id)
                res = await session.execute(stmt)
                rec = res.scalars().first()
                if not rec:
                    return False
                rec.transcription = transcription
                rec.transcription_status = status
                await session.flush()
                return True

    async def reset_stale_transcriptions(self) -> int:
        """Reset recordings stuck in 'transcribing' without transcript back to 'pending'."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                from sqlalchemy import or_, update
                stmt = (
                    update(CallRecordingRecord)
                    .where(
                        CallRecordingRecord.transcription_status == "transcribing",
                        or_(
                            CallRecordingRecord.transcription == None,
                            CallRecordingRecord.transcription == "",
                        ),
                    )
                    .values(transcription_status="pending")
                )
                res = await session.execute(stmt)
                return res.rowcount or 0


    async def query_transcriptions(
        self,
        tenant_id: Optional[str] = None,
        did_number: Optional[str] = None,
        phone_number: Optional[str] = None,
        started_after: Optional[datetime] = None,
        started_before: Optional[datetime] = None,
        date_str: Optional[str] = None,
        recording_id: Optional[str] = None,
        call_id: Optional[str] = None,
        room_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query transcripts by call_id, room_name, tenant ID, DID, phone, timestamps, or recording ID."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            # Auto-correlate missing call_id/tenant_id from calls table where room_name matches
            if room_name or call_id:
                try:
                    c_lookup = room_name or call_id
                    sub_stmt = select(CallRecord).where(
                        or_(CallRecord.room_name == c_lookup, CallRecord.call_id == c_lookup)
                    )
                    c_res = await session.execute(sub_stmt)
                    c_match = c_res.scalars().first()
                    if c_match:
                        await session.execute(
                            update(CallRecordingRecord)
                            .where(
                                CallRecordingRecord.room_name == c_match.room_name,
                                or_(CallRecordingRecord.call_id.is_(None), CallRecordingRecord.tenant_id.in_(["ADMIN", "wasid-hq"]))
                            )
                            .values(call_id=c_match.call_id, tenant_id=c_match.tenant_id)
                        )
                        await session.commit()
                except Exception as bfe:
                    logger.debug("Correlation sync non-fatal: %s", bfe)

            stmt = select(CallRecordingRecord).where(
                CallRecordingRecord.status != "deleted",
                CallRecordingRecord.transcription.isnot(None),
            )
            if recording_id:
                stmt = stmt.where(CallRecordingRecord.recording_id == recording_id)
            if room_name and call_id:
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.room_name == room_name,
                        CallRecordingRecord.call_id == call_id,
                        CallRecordingRecord.recording_id == call_id,
                    )
                )
            elif room_name:
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.room_name == room_name,
                        CallRecordingRecord.call_id == room_name,
                    )
                )
            elif call_id:
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.call_id == call_id,
                        CallRecordingRecord.room_name == call_id,
                        CallRecordingRecord.recording_id == call_id,
                    )
                )
            if tenant_id and tenant_id.lower() != "all":
                if room_name or call_id:
                    # When querying a specific unique room or call, match tenant or allow platform carrier ADMIN records
                    stmt = stmt.where(
                        or_(
                            CallRecordingRecord.tenant_id.ilike(f"%{tenant_id}%"),
                            CallRecordingRecord.tenant_id.in_(["ADMIN", "wasid-hq"]),
                        )
                    )
                elif tenant_id.upper() == "ADMIN" and not phone_number:
                    stmt = stmt.where(
                        or_(
                            CallRecordingRecord.tenant_id == "ADMIN",
                            CallRecordingRecord.tenant_id == "wasid-hq",
                            CallRecordingRecord.did_number == "+918065355408",
                        )
                    )
                elif tenant_id.upper() == "ADMIN":
                    stmt = stmt.where(
                        or_(
                            CallRecordingRecord.tenant_id == "ADMIN",
                            CallRecordingRecord.tenant_id == "wasid-hq",
                        )
                    )
                else:
                    stmt = stmt.where(CallRecordingRecord.tenant_id.ilike(f"%{tenant_id}%"))
            if did_number and did_number.lower() != "all":
                clean_did = did_number.strip().replace(" ", "")
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.did_number.ilike(f"%{clean_did}%"),
                        CallRecordingRecord.caller_number.ilike(f"%{clean_did}%"),
                        CallRecordingRecord.callee_number.ilike(f"%{clean_did}%"),
                    )
                )
            if phone_number:
                clean_phone = phone_number.strip().replace(" ", "")
                stmt = stmt.where(
                    or_(
                        CallRecordingRecord.caller_number.ilike(f"%{clean_phone}%"),
                        CallRecordingRecord.callee_number.ilike(f"%{clean_phone}%"),
                    )
                )
            if started_after:
                stmt = stmt.where(CallRecordingRecord.started_at >= started_after)
            if started_before:
                stmt = stmt.where(CallRecordingRecord.started_at <= started_before)
            if date_str:
                pattern = f"%{date_str.strip()}%"
                stmt = stmt.where(
                    or_(
                        func.to_char(CallRecordingRecord.started_at, 'YYYY-MM-DD').ilike(pattern),
                        CallRecordingRecord.room_name.ilike(pattern),
                    )
                )

            stmt = stmt.order_by(CallRecordingRecord.started_at.desc()).limit(limit)
            res = await session.execute(stmt)
            return [row.to_dict() for row in res.scalars()]

    async def prune_expired_recordings(self, days: int = 30) -> Dict[str, Any]:
        """Automatically delete recordings older than 30 days from database and Cloudflare R2."""
        from app.services.storage_r2 import storage_r2
        await self.init_db()
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        deleted_count = 0
        freed_bytes = 0

        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecordingRecord).where(
                CallRecordingRecord.started_at < cutoff,
                CallRecordingRecord.status != "deleted",
            )
            res = await session.execute(stmt)
            expired_recs = res.scalars().all()

            for rec in expired_recs:
                obj_key = rec.storage_object_key
                if obj_key and storage_r2.is_configured():
                    try:
                        await storage_r2.delete_object(obj_key)
                        await storage_r2.delete_object(f"{obj_key}.mp3")
                    except Exception as e:
                        logger.warning("Error deleting R2 object %s during prune: %s", obj_key, e)

                freed_bytes += rec.file_size_bytes or 0
                rec.status = "deleted"
                deleted_count += 1

            if deleted_count > 0:
                await session.commit()
                logger.info("Pruned %d expired recordings (>%d days old), freed %d bytes", deleted_count, days, freed_bytes)

        return {"deleted_count": deleted_count, "freed_bytes": freed_bytes}

    async def delete_recording(self, recording_id: str, hard: bool = False) -> bool:
        """Mark a recording as deleted or permanently remove it."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            async with session.begin():
                stmt = select(CallRecordingRecord).where(CallRecordingRecord.recording_id == recording_id)
                result = await session.execute(stmt)
                record = result.scalars().first()
                if not record:
                    return False
                if hard:
                    await session.delete(record)
                else:
                    record.status = "deleted"
                await session.flush()
                return True

    async def get_recording_stats(self) -> Dict[str, Any]:
        """Compute aggregated statistics for the recordings dashboard."""
        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt_total = select(func.count(CallRecordingRecord.id)).where(CallRecordingRecord.status != "deleted")
            stmt_inbound = select(func.count(CallRecordingRecord.id)).where(
                CallRecordingRecord.status != "deleted", CallRecordingRecord.direction == "inbound"
            )
            stmt_outbound = select(func.count(CallRecordingRecord.id)).where(
                CallRecordingRecord.status != "deleted", CallRecordingRecord.direction == "outbound"
            )
            stmt_active = select(func.count(CallRecordingRecord.id)).where(CallRecordingRecord.status == "recording")
            stmt_duration = select(func.sum(CallRecordingRecord.duration_seconds)).where(CallRecordingRecord.status != "deleted")
            stmt_bytes = select(func.sum(CallRecordingRecord.file_size_bytes)).where(CallRecordingRecord.status != "deleted")

            total = (await session.execute(stmt_total)).scalar() or 0
            inbound = (await session.execute(stmt_inbound)).scalar() or 0
            outbound = (await session.execute(stmt_outbound)).scalar() or 0
            active = (await session.execute(stmt_active)).scalar() or 0
            total_duration = (await session.execute(stmt_duration)).scalar() or 0
            total_bytes = (await session.execute(stmt_bytes)).scalar() or 0

            return {
                "total_recordings": total,
                "inbound_recordings": inbound,
                "outbound_recordings": outbound,
                "active_recordings": active,
                "total_duration_seconds": total_duration,
                "total_file_size_bytes": total_bytes,
            }

    async def get_call_context(self, identifier: str) -> Dict[str, Any]:
        """Fetch full structured call and lead context by call_id or room_name."""
        if not identifier:
            return {"found": False, "error": "No identifier provided"}

        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            stmt = select(CallRecord).where(
                or_(CallRecord.call_id == identifier, CallRecord.room_name == identifier)
            )
            result = await session.execute(stmt)
            record = result.scalars().first()
            if not record:
                return {"found": False, "error": f"Call record '{identifier}' not found"}

            meta: Dict[str, Any] = {}
            if record.metadata_json:
                try:
                    meta = json.loads(record.metadata_json)
                except Exception:
                    pass

            lead_id = meta.get("lead_id") or ""
            phone = record.callee_did or record.caller_did or ""

            # If prospect details are not in call metadata, fetch from customer_leads
            customer_name = meta.get("customer_name") or meta.get("name") or ""
            company_name = meta.get("company_name") or meta.get("company") or ""
            industry = meta.get("industry") or ""
            source = meta.get("source") or ""
            reqs = meta.get("original_requirement") or meta.get("requirements") or meta.get("notes") or ""
            ai_summary = meta.get("ai_consultant_summary") or meta.get("ai_summary") or ""

            if not (customer_name and company_name):
                try:
                    lead_query = text(
                        "SELECT id, name, company_name, industry, source, ai_summary, notes, tenant_id "
                        "FROM customer_leads WHERE id = :lead_id OR phone = :phone LIMIT 1"
                    )
                    lead_res = await session.execute(lead_query, {"lead_id": lead_id, "phone": phone})
                    lead_row = lead_res.fetchone()
                    if lead_row:
                        customer_name = customer_name or lead_row[1] or ""
                        company_name = company_name or lead_row[2] or ""
                        industry = industry or lead_row[3] or ""
                        source = source or lead_row[4] or ""
                        ai_summary = ai_summary or lead_row[5] or ""
                        reqs = reqs or lead_row[6] or ""
                except Exception as le:
                    logger.debug("Could not query customer_leads for call context: %s", le)

            call_dir = (record.direction or "OUTBOUND").upper()
            return {
                "found": True,
                "call_id": record.call_id,
                "room_name": record.room_name,
                "direction": record.direction,
                "call_direction": call_dir,
                "caller_did": record.caller_did,
                "callee_did": record.callee_did,
                "agent_id": record.agent_id,
                "tenant_id": record.tenant_id,
                "voice_mode": meta.get("voice_mode") or meta.get("mode") or "realtime",
                "customer_name": customer_name,
                "company_name": company_name,
                "phone_number": phone,
                "industry": industry,
                "source": source,
                "original_requirement": reqs,
                "ai_consultant_summary": ai_summary,
                "call_objective": meta.get("call_objective") or "Confirm demo booking and qualify workflow automation requirements",
                "script": meta.get("script") or meta.get("opening_script") or "",
                "whatsapp_context": meta.get("whatsapp_context") or "",
                "metadata": meta,
            }

    async def lookup_caller(self, phone_number: str) -> Dict[str, Any]:
        """Lookup caller phone against customer_leads to distinguish known leads from unknown callers."""
        if not phone_number:
            return {"found": False, "reason": "empty_phone"}

        clean = re.sub(r"[^\d]", "", phone_number)
        phone_tail = clean[-10:] if len(clean) >= 10 else clean
        if not phone_tail:
            return {"found": False, "reason": "invalid_phone"}

        await self.init_db()
        sessionmaker = self.get_sessionmaker()
        async with sessionmaker() as session:
            try:
                # Query matching customer_leads
                pattern = f"%{phone_tail}%"
                query = text(
                    "SELECT id, tenant_id, name, phone, company_name, industry, source, ai_summary, notes, status "
                    "FROM customer_leads WHERE phone LIKE :pattern ORDER BY created_at DESC LIMIT 1"
                )
                res = await session.execute(query, {"pattern": pattern})
                row = res.fetchone()
                if row:
                    return {
                        "found": True,
                        "lead_id": row[0],
                        "tenant_id": row[1],
                        "customer_name": row[2] or "",
                        "phone": row[3] or "",
                        "company_name": row[4] or "",
                        "industry": row[5] or "",
                        "source": row[6] or "",
                        "ai_summary": row[7] or "",
                        "original_requirement": row[8] or "",
                        "status": row[9] or "",
                    }
            except Exception as e:
                logger.debug("Error querying customer_leads in lookup_caller: %s", e)

            return {"found": False, "phone": phone_number}


# Singleton instance
telephony_db = TelephonyDatabase()

