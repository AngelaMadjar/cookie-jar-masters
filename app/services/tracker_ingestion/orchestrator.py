from __future__ import annotations

from datetime import datetime, timezone
from threading import BoundedSemaphore, Lock
from time import perf_counter

from app.services.tracker_ingestion.ingestion_service import IngestionService
from app.services.tracker_ingestion.purpose_service import PurposeService
from app.services.tracker_ingestion.reference_data_service import ReferenceDataService
from app.services.tracker_ingestion.reporting_service import ReportingService
from app.services.tracker_ingestion.scan_lifecycle_service import ScanLifecycleService
from app.services.tracker_ingestion.tracker_upsert_service import TrackerUpsertService


class TrackerIngestionOrchestrator:
    CONCURRENCY_LIMIT = 8
    _semaphore = BoundedSemaphore(CONCURRENCY_LIMIT)
    _lock = Lock()
    _active_requests = 0
    _max_active_seen = 0

    @staticmethod
    def _now_utc():
        return datetime.now(timezone.utc)

    @staticmethod
    def ingest_file(file_path: str):
        ingestion = IngestionService.load_and_validate(file_path)

        refs = ReferenceDataService.build_reference_maps(
            ingestion.good_df,
            ingestion.source_name,
            ingestion.cmp_name,
        )

        upsert_result = TrackerUpsertService.upsert_trackers(
            ingestion.good_df,
            refs,
            ingestion.cmp_name,
            ingestion.source_name,
        )

        purpose_links = PurposeService.create_purpose_links(
            ingestion.good_df,
            refs,
            upsert_result.tracker_map,
        )

        processed_df, failed_df = ReportingService.build_output_frames(
            ingestion.good_df,
            ingestion.bad_df,
        )

        return {
            "file_path": file_path,
            "processed_df": processed_df,
            "failed_df": failed_df,
            "summary": ReportingService.summarize(
                ingestion.source_name,
                processed_df,
                failed_df,
                upsert_result.created_count,
                upsert_result.existing_count,
                purpose_links,
            ),
        }

    @staticmethod
    def ingest_single_file(
        file_path: str,
        test_case: str,
        run_id: str,
        month: str = "2026-02",
        request_received_timestamp: datetime | None = None,
    ) -> dict:
        request_received = request_received_timestamp or TrackerIngestionOrchestrator._now_utc()
        file_started = perf_counter()

        TrackerIngestionOrchestrator._semaphore.acquire()
        try:
            with TrackerIngestionOrchestrator._lock:
                TrackerIngestionOrchestrator._active_requests += 1
                if TrackerIngestionOrchestrator._active_requests > TrackerIngestionOrchestrator._max_active_seen:
                    TrackerIngestionOrchestrator._max_active_seen = (
                        TrackerIngestionOrchestrator._active_requests
                    )
                active_processing_requests = TrackerIngestionOrchestrator._active_requests

            processing_start = TrackerIngestionOrchestrator._now_utc()

            result = TrackerIngestionOrchestrator.ingest_file(file_path)
            ScanLifecycleService.persist_results(
                month=month,
                source_file_gs_uri=file_path,
                processed_df=result["processed_df"],
                failed_df=result["failed_df"],
            )

            processing_finished = TrackerIngestionOrchestrator._now_utc()
            response_finished = TrackerIngestionOrchestrator._now_utc()
            file_makespan_sec = perf_counter() - file_started

            summary = result["summary"]
            queue_wait_time_sec = (processing_start - request_received).total_seconds()
            processing_time_sec = (processing_finished - processing_start).total_seconds()
            end_to_end_latency_sec = (response_finished - request_received).total_seconds()

            summary.update(
                {
                    "test_case": test_case,
                    "run_id": run_id,
                    "month": month,
                    "request_received_timestamp": request_received.isoformat(),
                    "processing_start_timestamp": processing_start.isoformat(),
                    "processing_finished_timestamp": processing_finished.isoformat(),
                    "response_finished_timestamp": response_finished.isoformat(),
                    "queue_wait_time_sec": round(queue_wait_time_sec, 6),
                    "processing_time_sec": round(processing_time_sec, 6),
                    "end_to_end_latency_sec": round(end_to_end_latency_sec, 6),
                    "file_makespan_sec": round(file_makespan_sec, 6),
                    "active_processing_requests": active_processing_requests,
                    "max_active_requests_seen": TrackerIngestionOrchestrator._max_active_seen,
                }
            )
            return summary
        finally:
            with TrackerIngestionOrchestrator._lock:
                TrackerIngestionOrchestrator._active_requests -= 1
            TrackerIngestionOrchestrator._semaphore.release()
