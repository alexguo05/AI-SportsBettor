"""Durable PostgreSQL job claims, leases, retries, and notifications."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from src.db.engine import DatabaseResources
from src.db.models import job_outbox

JOB_CHANNEL = "ai_sports_jobs"
JOB_NAMESPACE = uuid.UUID("72a5f16f-d286-42ea-bc0a-62e7cf292193")

ENRICH_NEWS = "enrich_news"
RESOLVE_NEWS = "resolve_news"
RESOLVE_MARKET = "resolve_market"
RESOLVE_KALSHI_MARKET = "resolve_kalshi_market"
SUPPORTED_JOB_TYPES = frozenset(
    {ENRICH_NEWS, RESOLVE_NEWS, RESOLVE_MARKET, RESOLVE_KALSHI_MARKET}
)


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    job_type: str
    idempotency_key: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int

    @classmethod
    def from_mapping(cls, row: Any) -> JobRecord:
        return cls(
            job_id=str(row["job_id"]),
            job_type=str(row["job_type"]),
            idempotency_key=str(row["idempotency_key"]),
            payload=dict(row["payload"]),
            attempts=int(row["attempts"]),
            max_attempts=int(row["max_attempts"]),
        )


def enqueue_job(
    connection: Connection,
    *,
    job_type: str,
    idempotency_key: str,
    payload: dict[str, Any],
    priority: int = 0,
    max_attempts: int = 5,
    notify: bool = True,
    now: datetime | None = None,
) -> str:
    """Insert one idempotent job in the caller's transaction and wake listeners."""

    if job_type not in SUPPORTED_JOB_TYPES:
        raise ValueError(f"unsupported job type: {job_type}")
    timestamp = (now or datetime.now(UTC)).astimezone(UTC)
    job_id = str(
        uuid.uuid5(
            JOB_NAMESPACE,
            f"{job_type}:{idempotency_key}",
        )
    )
    statement = insert(job_outbox).values(
        job_id=job_id,
        job_type=job_type,
        idempotency_key=idempotency_key,
        payload=payload,
        status="pending",
        priority=priority,
        attempts=0,
        max_attempts=max_attempts,
        available_at=timestamp,
        lease_owner=None,
        lease_expires_at=None,
        last_error=None,
        updated_at=timestamp,
        completed_at=None,
    )
    connection.execute(
        statement.on_conflict_do_nothing(
            index_elements=[
                job_outbox.c.job_type,
                job_outbox.c.idempotency_key,
            ]
        )
    )
    if notify:
        connection.execute(select(func.pg_notify(JOB_CHANNEL, job_id)))
    return job_id


class JobRepository:
    def __init__(self, resources: DatabaseResources) -> None:
        self.resources = resources

    def claim(
        self,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
        job_types: set[str] | None = None,
        now: datetime | None = None,
    ) -> list[JobRecord]:
        timestamp = (now or datetime.now(UTC)).astimezone(UTC)
        claimable = or_(
            job_outbox.c.status == "pending",
            and_(
                job_outbox.c.status == "leased",
                job_outbox.c.lease_expires_at < timestamp,
            ),
        )
        candidate = (
            select(job_outbox.c.job_id)
            .where(
                claimable,
                job_outbox.c.available_at <= timestamp,
            )
            .order_by(
                job_outbox.c.priority.desc(),
                job_outbox.c.available_at,
                job_outbox.c.created_at,
            )
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
        if job_types:
            candidate = candidate.where(job_outbox.c.job_type.in_(sorted(job_types)))
        candidate_cte = candidate.cte("claimable_jobs")
        statement = (
            update(job_outbox)
            .where(
                job_outbox.c.job_id.in_(
                    select(candidate_cte.c.job_id)
                )
            )
            .values(
                status="leased",
                lease_owner=lease_owner,
                lease_expires_at=timestamp + timedelta(seconds=lease_seconds),
                attempts=job_outbox.c.attempts + 1,
                updated_at=timestamp,
            )
            .returning(job_outbox)
        )
        with self.resources.engine.begin() as connection:
            rows = connection.execute(statement).mappings().all()
        return [JobRecord.from_mapping(row) for row in rows]

    def complete(
        self,
        job: JobRecord,
        *,
        lease_owner: str,
        now: datetime | None = None,
    ) -> None:
        timestamp = (now or datetime.now(UTC)).astimezone(UTC)
        with self.resources.engine.begin() as connection:
            connection.execute(
                update(job_outbox)
                .where(
                    job_outbox.c.job_id == job.job_id,
                    job_outbox.c.status == "leased",
                    job_outbox.c.lease_owner == lease_owner,
                )
                .values(
                    status="completed",
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error=None,
                    completed_at=timestamp,
                    updated_at=timestamp,
                )
            )

    def fail(
        self,
        job: JobRecord,
        *,
        lease_owner: str,
        error: str,
        now: datetime | None = None,
    ) -> str:
        timestamp = (now or datetime.now(UTC)).astimezone(UTC)
        dead = job.attempts >= job.max_attempts
        next_status = "dead" if dead else "pending"
        delay_seconds = min(2 ** max(job.attempts - 1, 0), 300)
        with self.resources.engine.begin() as connection:
            connection.execute(
                update(job_outbox)
                .where(
                    job_outbox.c.job_id == job.job_id,
                    job_outbox.c.status == "leased",
                    job_outbox.c.lease_owner == lease_owner,
                )
                .values(
                    status=next_status,
                    available_at=(
                        timestamp
                        if dead
                        else timestamp + timedelta(seconds=delay_seconds)
                    ),
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error=error[:4000],
                    updated_at=timestamp,
                )
            )
        return next_status

    def unfinished_count(self, *, job_types: set[str] | None = None) -> int:
        statement = select(func.count()).select_from(job_outbox).where(
            job_outbox.c.status.in_(["pending", "leased"])
        )
        if job_types:
            statement = statement.where(job_outbox.c.job_type.in_(sorted(job_types)))
        with self.resources.engine.connect() as connection:
            return int(connection.scalar(statement) or 0)
