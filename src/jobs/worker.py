"""Run durable enrichment and entity jobs with bounded concurrency."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.db.engine import DatabaseResources, create_database_resources
from src.enrich_news.config import EnrichmentSettings, load_enrichment_settings
from src.enrich_news.pipeline import enrich_record
from src.enrich_news.provider import ClaudeProvider
from src.enrich_news.repository import EnrichmentRepository
from src.entity_bank.provider import ClaudeEntityProvider
from src.entity_bank.resolution_repository import ResolutionRepository
from src.entity_bank.resolver import CandidateIndex
from src.entity_bank.worker import Batch, process_market_events, process_news
from src.jobs.repository import (
    ENRICH_NEWS,
    RESOLVE_KALSHI_MARKET,
    RESOLVE_MARKET,
    RESOLVE_NEWS,
    SUPPORTED_JOB_TYPES,
    JobRecord,
    JobRepository,
)

WRITE_CONFIRMATION = "RUN_JOB_WORKER"
DEFAULT_CONCURRENCY = 10
MAX_CONCURRENCY = 30


@dataclass(frozen=True)
class JobResult:
    job_id: str
    job_type: str
    outcome: str
    details: dict[str, Any]


class WorkerRuntime:
    def __init__(
        self,
        *,
        resources: DatabaseResources,
        settings: EnrichmentSettings,
        allow_network: bool,
        video_concurrency: int,
        market_concurrency: int,
    ) -> None:
        self.resources = resources
        self.settings = settings
        self.allow_network = allow_network
        self.enrichment_repository = EnrichmentRepository(resources)
        self.resolution_repository = ResolutionRepository(resources)
        self._thread_local = threading.local()
        self._candidate_lock = threading.Lock()
        self._bank_version_id: str | None = None
        self._candidate_index: CandidateIndex | None = None
        self.video_slots = threading.BoundedSemaphore(video_concurrency)
        self.market_slots = threading.BoundedSemaphore(market_concurrency)

    def _providers(self) -> tuple[ClaudeProvider, ClaudeEntityProvider]:
        providers = getattr(self._thread_local, "providers", None)
        if providers is None:
            api_key = self.settings.api_key or ""
            providers = (
                ClaudeProvider(
                    api_key,
                    model_name=self.settings.model_name,
                    max_tokens=self.settings.max_output_tokens,
                ),
                ClaudeEntityProvider(
                    api_key,
                    model_name=self.settings.model_name,
                    max_tokens=self.settings.max_output_tokens,
                ),
            )
            self._thread_local.providers = providers
        return providers

    def resolution_context(self) -> tuple[str, CandidateIndex]:
        latest_version = self.resolution_repository.latest_bank_version_id()
        if latest_version is None:
            raise RuntimeError("apply nflverse entity bank before processing resolution jobs")
        with self._candidate_lock:
            if (
                self._candidate_index is None
                or self._bank_version_id != latest_version
            ):
                self._candidate_index = CandidateIndex(
                    self.resolution_repository.load_candidate_rows()
                )
                self._bank_version_id = latest_version
            return latest_version, self._candidate_index

    def invalidate_candidates(self) -> None:
        with self._candidate_lock:
            self._candidate_index = None

    def handle(self, job: JobRecord) -> JobResult:
        if job.job_type == ENRICH_NEWS:
            return self._handle_enrich_news(job)
        if job.job_type == RESOLVE_NEWS:
            return self._handle_resolve_news(job)
        if job.job_type == RESOLVE_MARKET:
            return self._handle_resolve_market(job)
        if job.job_type == RESOLVE_KALSHI_MARKET:
            return self._handle_resolve_kalshi_market(job)
        raise ValueError(f"unsupported job type: {job.job_type}")

    def _handle_enrich_news(self, job: JobRecord) -> JobResult:
        news_id = str(job.payload["news_id"])
        version = str(
            job.payload.get("enrichment_version")
            or self.settings.enrichment_version
        )
        if self.enrichment_repository.has_completed(
            news_id=news_id,
            enrichment_version=version,
        ):
            return JobResult(job.job_id, job.job_type, "already_completed", {})
        record = self.enrichment_repository.load_record(news_id)
        if record is None:
            raise ValueError(f"news row does not exist: {news_id}")
        provider, _ = self._providers()
        has_video = any(
            (media.media_type or "").casefold() in {"video", "animated_gif"}
            for media in record.media
        )
        semaphore = self.video_slots if has_video else _NullSemaphore()
        with semaphore:
            result = enrich_record(
                record,
                provider,
                enrichment_version=version,
                allow_network=self.allow_network,
            )
        if not result.status.startswith("completed"):
            raise RuntimeError(result.error or "news enrichment failed")
        self.enrichment_repository.persist_result(result)
        return JobResult(
            job.job_id,
            job.job_type,
            "completed",
            {
                "news_id": news_id,
                "input_tokens": result.usage.input_tokens,
                "output_tokens": result.usage.output_tokens,
            },
        )

    def _handle_resolve_news(self, job: JobRecord) -> JobResult:
        bank_version_id, index = self.resolution_context()
        records = self.resolution_repository.load_news(
            limit=1,
            extractor_version=str(job.payload["extractor_version"]),
            enrichment_version=str(job.payload["enrichment_version"]),
            news_id=str(job.payload["news_id"]),
            input_fingerprint=str(job.payload["input_fingerprint"]),
        )
        if not records:
            return JobResult(job.job_id, job.job_type, "already_completed", {})
        _, provider = self._providers()
        batch = Batch()
        process_news(
            records=records,
            provider=provider,
            index=index,
            bank_version_id=bank_version_id,
            batch=batch,
            observed_at=datetime.now(UTC),
        )
        if batch.failures:
            raise RuntimeError(json.dumps(batch.failures, default=str))
        self.resolution_repository.persist_batch(batch.as_repository_batch())
        if batch.provisional_entities:
            self.invalidate_candidates()
        return JobResult(
            job.job_id,
            job.job_type,
            "completed",
            {
                "news_id": job.payload["news_id"],
                "mentions": len(batch.mentions),
                "input_tokens": batch.input_tokens,
                "output_tokens": batch.output_tokens,
            },
        )

    def _handle_resolve_market(self, job: JobRecord) -> JobResult:
        bank_version_id, index = self.resolution_context()
        events = self.resolution_repository.load_market_events(
            event_limit=1,
            event_ids={str(job.payload["event_id"])},
        )
        if not events:
            return JobResult(job.job_id, job.job_type, "not_active", {})
        _, provider = self._providers()
        batch = Batch()
        with self.market_slots:
            process_market_events(
                events=events,
                provider=provider,
                index=index,
                bank_version_id=bank_version_id,
                batch=batch,
                observed_at=datetime.now(UTC),
            )
        if batch.failures:
            raise RuntimeError(json.dumps(batch.failures, default=str))
        self.resolution_repository.persist_batch(batch.as_repository_batch())
        if batch.provisional_entities:
            self.invalidate_candidates()
        return JobResult(
            job.job_id,
            job.job_type,
            "completed",
            {
                "event_id": job.payload["event_id"],
                "markets": len(batch.classifications),
                "mentions": len(batch.mentions),
                "input_tokens": batch.input_tokens,
                "output_tokens": batch.output_tokens,
            },
        )

    def _handle_resolve_kalshi_market(self, job: JobRecord) -> JobResult:
        bank_version_id, index = self.resolution_context()
        events = self.resolution_repository.load_kalshi_market_events(
            event_limit=1,
            event_tickers={str(job.payload["event_ticker"])},
        )
        if not events:
            return JobResult(job.job_id, job.job_type, "not_active", {})
        _, provider = self._providers()
        batch = Batch()
        with self.market_slots:
            process_market_events(
                events=events,
                provider=provider,
                index=index,
                bank_version_id=bank_version_id,
                batch=batch,
                observed_at=datetime.now(UTC),
                source_kind="kalshi_market",
            )
        if batch.failures:
            raise RuntimeError(json.dumps(batch.failures, default=str))
        self.resolution_repository.persist_batch(batch.as_repository_batch())
        if batch.provisional_entities:
            self.invalidate_candidates()
        return JobResult(
            job.job_id,
            job.job_type,
            "completed",
            {
                "event_ticker": job.payload["event_ticker"],
                "markets": len(batch.classifications),
                "mentions": len(batch.mentions),
                "input_tokens": batch.input_tokens,
                "output_tokens": batch.output_tokens,
            },
        )


class _NullSemaphore:
    def __enter__(self) -> _NullSemaphore:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("JOB_WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)),
    )
    parser.add_argument(
        "--video-concurrency",
        type=int,
        default=int(os.environ.get("JOB_VIDEO_CONCURRENCY", "2")),
    )
    parser.add_argument(
        "--market-concurrency",
        type=int,
        default=int(os.environ.get("JOB_MARKET_CONCURRENCY", "5")),
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=float(os.environ.get("JOB_POLL_INTERVAL_SECONDS", "1")),
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.environ.get("JOB_LEASE_SECONDS", "900")),
    )
    parser.add_argument(
        "--job-types",
        default=",".join(sorted(SUPPORTED_JOB_TYPES)),
    )
    parser.add_argument("--no-network", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--confirm-live-writes")
    return parser


def _parse_job_types(value: str) -> set[str]:
    selected = {item.strip() for item in value.split(",") if item.strip()}
    unknown = selected - SUPPORTED_JOB_TYPES
    if not selected or unknown:
        raise ValueError(f"invalid job types: {sorted(unknown)}")
    return selected


def _log(event: str, **details: Any) -> None:
    print(
        json.dumps(
            {
                "event": event,
                "timestamp": datetime.now(UTC).isoformat(),
                **details,
            },
            sort_keys=True,
            default=str,
        ),
        flush=True,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.confirm_live_writes != WRITE_CONFIRMATION:
        print(
            f"ERROR: --confirm-live-writes must equal {WRITE_CONFIRMATION}",
            file=sys.stderr,
        )
        return 2
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        print(
            f"ERROR: --concurrency must be between 1 and {MAX_CONCURRENCY}",
            file=sys.stderr,
        )
        return 2
    args.video_concurrency = min(args.video_concurrency, args.concurrency)
    args.market_concurrency = min(args.market_concurrency, args.concurrency)
    if not 1 <= args.video_concurrency <= args.concurrency:
        print("ERROR: invalid --video-concurrency", file=sys.stderr)
        return 2
    if not 1 <= args.market_concurrency <= args.concurrency:
        print("ERROR: invalid --market-concurrency", file=sys.stderr)
        return 2
    if not 0.1 <= args.poll_interval_seconds <= 60:
        print("ERROR: invalid --poll-interval-seconds", file=sys.stderr)
        return 2
    if args.lease_seconds < 120:
        print("ERROR: --lease-seconds must be at least 120", file=sys.stderr)
        return 2
    try:
        job_types = _parse_job_types(args.job_types)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    src_dir = Path(__file__).resolve().parents[1]
    settings = load_enrichment_settings(src_dir)
    if not settings.api_key:
        print("ERROR: ANTHROPIC_API_KEY is not configured", file=sys.stderr)
        return 2
    resources = create_database_resources(src_dir)
    jobs = JobRepository(resources)
    runtime = WorkerRuntime(
        resources=resources,
        settings=settings,
        allow_network=not args.no_network,
        video_concurrency=args.video_concurrency,
        market_concurrency=args.market_concurrency,
    )
    lease_owner = f"{socket.gethostname()}:{os.getpid()}"
    stopping = threading.Event()

    def stop(*_args: object) -> None:
        stopping.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    futures: dict[Future[JobResult], JobRecord] = {}
    exit_code = 0
    _log(
        "JOB_WORKER_STARTED",
        concurrency=args.concurrency,
        video_concurrency=args.video_concurrency,
        market_concurrency=args.market_concurrency,
        job_types=sorted(job_types),
        lease_owner=lease_owner,
    )
    try:
        with ThreadPoolExecutor(
            max_workers=args.concurrency,
            thread_name_prefix="sports-job",
        ) as executor:
            while not stopping.is_set() or futures:
                done = {future for future in futures if future.done()}
                for future in done:
                    job = futures.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        status = jobs.fail(
                            job,
                            lease_owner=lease_owner,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        _log(
                            "JOB_FAILED",
                            job_id=job.job_id,
                            job_type=job.job_type,
                            attempts=job.attempts,
                            status=status,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        if status == "dead":
                            exit_code = 1
                    else:
                        jobs.complete(job, lease_owner=lease_owner)
                        _log(
                            "JOB_COMPLETED",
                            job_id=result.job_id,
                            job_type=result.job_type,
                            outcome=result.outcome,
                            **result.details,
                        )

                if stopping.is_set():
                    if futures:
                        wait(
                            set(futures),
                            timeout=args.poll_interval_seconds,
                            return_when=FIRST_COMPLETED,
                        )
                    continue

                capacity = args.concurrency - len(futures)
                if capacity:
                    claimed = jobs.claim(
                        limit=capacity,
                        lease_owner=lease_owner,
                        lease_seconds=args.lease_seconds,
                        job_types=job_types,
                    )
                    for job in claimed:
                        futures[executor.submit(runtime.handle, job)] = job

                if args.once and not futures and jobs.unfinished_count(
                    job_types=job_types
                ) == 0:
                    break
                if futures:
                    wait(
                        set(futures),
                        timeout=args.poll_interval_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                else:
                    stopping.wait(args.poll_interval_seconds)
    finally:
        resources.close()
    _log("JOB_WORKER_STOPPED", exit_code=exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
