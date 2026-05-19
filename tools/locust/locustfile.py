from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from typing import Any

from locust import HttpUser, events, task
from locust.exception import StopUser


RUN_SPEC_PATH = os.getenv("LOCUST_RUN_SPEC_PATH", "")
RUN_SPEC_JSON = os.getenv("LOCUST_RUN_SPEC_JSON", "")
RAW_JSON_PATH = os.getenv("LOCUST_RAW_JSON_PATH", "")
MONTH = os.getenv("LOCUST_MONTH", "2026-02")
RUNTIME_FILE_LIST_PATH = os.getenv(
    "LOCUST_RUNTIME_FILE_LIST_PATH",
    "tools/locust/runtime_input_files_2026-02.json",
)
LOCUST_BEARER_TOKEN = os.getenv("LOCUST_BEARER_TOKEN", "")

_lock = threading.Lock()
_file_queue: deque[str] = deque()
_requests_started = 0
_requests_completed = 0
_total_requests = 0
_test_case = ""
_run_id = ""
_records: list[dict[str, Any]] = []
_stop_triggered = False


def _load_run_spec() -> None:
    global _file_queue, _total_requests, _test_case, _run_id
    payload = None
    if RUN_SPEC_JSON:
        payload = json.loads(RUN_SPEC_JSON)
    elif RUN_SPEC_PATH:
        with open(RUN_SPEC_PATH, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    else:
        raise RuntimeError("Either LOCUST_RUN_SPEC_JSON or LOCUST_RUN_SPEC_PATH is required")

    _test_case = payload.get("test_case") or ""
    _run_id = payload.get("run_id") or ""
    with open(RUNTIME_FILE_LIST_PATH, "r", encoding="utf-8") as fh:
        runtime_payload = json.load(fh)
    file_paths = runtime_payload.get("file_paths") or []
    if not file_paths:
        raise RuntimeError(f"No file_paths found in runtime list: {RUNTIME_FILE_LIST_PATH}")
    _file_queue = deque(file_paths)
    _total_requests = len(file_paths)


def _record_result(file_path: str, status_code: int, ok: bool, latency_ms: float, payload: dict[str, Any] | None, error: str | None):
    entry = {
        "file_path": file_path,
        "status_code": status_code,
        "ok": bool(ok),
        "locust_latency_ms": round(latency_ms, 3),
        "error": error,
        "test_case": _test_case,
        "run_id": _run_id,
        "month": MONTH,
    }
    if payload:
        entry.update(
            {
                "file": payload.get("file"),
                "processed_rows": payload.get("processed_rows", 0),
                "failed_rows": payload.get("failed_rows", 0),
                "created_trackers": payload.get("created_trackers", 0),
                "existing_trackers": payload.get("existing_trackers", 0),
                "queue_wait_time_sec": payload.get("queue_wait_time_sec", 0),
                "processing_time_sec": payload.get("processing_time_sec", 0),
                "end_to_end_latency_sec": payload.get("end_to_end_latency_sec", 0),
                "active_processing_requests": payload.get("active_processing_requests", 0),
                "max_active_requests_seen": payload.get("max_active_requests_seen", 0),
                "request_received_timestamp": payload.get("request_received_timestamp"),
                "processing_start_timestamp": payload.get("processing_start_timestamp"),
                "processing_finished_timestamp": payload.get("processing_finished_timestamp"),
                "response_finished_timestamp": payload.get("response_finished_timestamp"),
            }
        )
    _records.append(entry)


def _extract_error(payload: dict[str, Any] | None, status_code: int) -> str:
    if not payload:
        return f"HTTP {status_code}"
    detail = payload.get("details")
    err = payload.get("error")
    if detail and err:
        return f"{err}: {detail}"
    if detail:
        return str(detail)
    if err:
        return str(err)
    return f"HTTP {status_code}"


def _maybe_stop(environment) -> None:
    global _stop_triggered
    with _lock:
        if _stop_triggered:
            return
        if _requests_completed >= _total_requests and _total_requests > 0:
            _stop_triggered = True
            if environment.runner:
                environment.runner.quit()


@events.test_start.add_listener
def _on_test_start(environment, **kwargs):
    global _requests_started, _requests_completed, _records, _stop_triggered
    _requests_started = 0
    _requests_completed = 0
    _records = []
    _stop_triggered = False
    _load_run_spec()


@events.test_stop.add_listener
def _on_test_stop(environment, **kwargs):
    if not RAW_JSON_PATH:
        return
    parent = os.path.dirname(RAW_JSON_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "test_case": _test_case,
                "run_id": _run_id,
                "month": MONTH,
                "requests_total": _total_requests,
                "requests_started": _requests_started,
                "requests_completed": _requests_completed,
                "records": _records,
            },
            fh,
            indent=2,
        )


class IngestUser(HttpUser):
    wait_time = lambda self: 0  # noqa: E731

    @task
    def ingest_one_file(self):
        global _requests_started, _requests_completed

        file_path = None
        with _lock:
            if _file_queue:
                file_path = _file_queue.popleft()
                _requests_started += 1

        if not file_path:
            # Keep user alive briefly until all in-flight requests are done.
            _maybe_stop(self.environment)
            raise StopUser()

        body = {
            "file_path": file_path,
            "test_case": _test_case,
            "run_id": _run_id,
            "month": MONTH,
        }

        start = time.perf_counter()
        payload: dict[str, Any] | None = None
        status_code = 0
        ok = False
        err: str | None = None
        latency_ms = 0.0
        try:
            headers = {"Authorization": f"Bearer {LOCUST_BEARER_TOKEN}"} if LOCUST_BEARER_TOKEN else None
            with self.client.post(
                "/scan/ingest",
                json=body,
                headers=headers,
                catch_response=True,
                name="/scan/ingest",
            ) as response:
                status_code = response.status_code
                latency_ms = (time.perf_counter() - start) * 1000.0
                try:
                    payload = response.json()
                except Exception:
                    payload = None

                if response.status_code == 200:
                    ok = True
                    response.success()
                else:
                    err = _extract_error(payload, response.status_code)
                    response.failure(err)
        except Exception as exc:
            status_code = 0
            latency_ms = (time.perf_counter() - start) * 1000.0
            err = f"request_exception: {exc}"
        finally:
            _record_result(
                file_path=file_path,
                status_code=status_code,
                ok=ok,
                latency_ms=latency_ms,
                payload=payload,
                error=err,
            )
            with _lock:
                _requests_completed += 1
            _maybe_stop(self.environment)
