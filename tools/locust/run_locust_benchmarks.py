#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import requests


CASE_TO_FOLDER = {
    "T1": "T1_original",
    "T2": "T2_medium_existing_heavy",
    "T3": "T3_medium_new_heavy",
    "T4": "T4_large_existing_heavy",
    "T5": "T5_large_new_heavy",
    "T6": "T6_skewed_existing_heavy",
    "T7": "T7_skewed_new_heavy",
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run Locust benchmarks for T1-T7 with automated DB reset and file staging.")
    p.add_argument("--host", default="http://127.0.0.1:8080", help="Ingestion app base URL")
    p.add_argument("--month", default="2026-02")
    p.add_argument("--test-cases", default="T1,T2,T3,T4,T5,T6,T7")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--users", type=int, default=80)
    p.add_argument("--spawn-rate", type=int, default=80)
    p.add_argument("--total-requests", type=int, default=80)
    p.add_argument("--ui", action="store_true", help="Run locust in web UI mode with autostart/autoquit.")
    p.add_argument("--results-dir", default="", help="Optional override for results root. If unset, writes under each case folder.")
    p.add_argument("--locust-bin", default="python3 -m locust")
    return p


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _split_csv(text: str) -> list[str]:
    return [x.strip() for x in text.split(",") if x.strip()]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * p))
    return float(ordered[idx])


def _post_ok(host: str, endpoint: str, payload: dict):
    print(f"  -> POST {endpoint}")
    resp = requests.post(f"{host}{endpoint}", json=payload, timeout=1800)
    if resp.status_code != 200:
        raise RuntimeError(f"{endpoint} failed: {resp.status_code} {resp.text}")


def reset_and_seed_db(host: str):
    print("Step 1/5: Reset and reseed database")
    _post_ok(host, "/db/empty", {})
    _post_ok(host, "/db/populate/cmp", {})
    _post_ok(host, "/db/populate/trackers", {})
    print("  DB reset + seed completed.")


def clear_month_dirs(month: str):
    print(f"Step 2/5: Clear monthly runtime folders for {month}")
    for branch in ("input", "processed", "failed"):
        folder = Path("data/monthly_tracker_audits") / branch / month
        folder.mkdir(parents=True, exist_ok=True)
        removed = 0
        for existing in folder.glob("*.csv"):
            existing.unlink(missing_ok=True)
            removed += 1
        print(f"  Cleared {removed} files from {folder}")


def load_manifest(case: str) -> list[str]:
    case_folder = CASE_TO_FOLDER[case]
    manifest_path = Path("data/benchmarks") / case_folder / "manifest" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths = payload.get("generated_file_paths") or []
    if len(paths) == 0:
        raise ValueError(f"No generated_file_paths in manifest: {manifest_path}")
    return [str(Path(p)) for p in paths]


def stage_files(file_paths: list[str], month: str, total_requests: int) -> list[str]:
    print(f"Step 3/5: Stage {total_requests} files into monthly input")
    if len(file_paths) < total_requests:
        raise ValueError(f"Manifest contains only {len(file_paths)} files, expected at least {total_requests}")
    chosen = file_paths[:total_requests]
    target_dir = Path("data/monthly_tracker_audits/input") / month
    target_dir.mkdir(parents=True, exist_ok=True)
    staged_paths: list[str] = []
    for src_str in chosen:
        src = Path(src_str)
        if not src.exists():
            raise FileNotFoundError(f"Source CSV missing: {src}")
        target = target_dir / src.name
        shutil.copy2(src, target)
        staged_paths.append(str(target))
    print(f"  Staged {len(staged_paths)} files into {target_dir}")
    return staged_paths


def case_results_dir(case: str, results_root_override: str) -> Path:
    if results_root_override.strip():
        return Path(results_root_override)
    return Path("data/benchmarks") / CASE_TO_FOLDER[case] / "locust_results"


def run_locust(args, run_spec_payload: dict, raw_json_path: Path) -> int:
    env = os.environ.copy()
    env["LOCUST_RUN_SPEC_JSON"] = json.dumps(run_spec_payload)
    env["LOCUST_RAW_JSON_PATH"] = str(raw_json_path)
    env["LOCUST_MONTH"] = args.month
    env["LOCUST_RUNTIME_FILE_LIST_PATH"] = "tools/locust/runtime_input_files_2026-02.json"

    locustfile = Path("tools/locust/locustfile.py")
    cmd = [
        *shlex.split(args.locust_bin),
        "-f",
        str(locustfile),
        "--host",
        args.host,
        "-u",
        str(args.users),
        "-r",
        str(args.spawn_rate),
    ]
    if args.ui:
        cmd.extend(["--autostart", "--autoquit", "3"])
    else:
        cmd.append("--headless")
    print("Step 4/5: Run Locust load test")
    print(f"  Command: {' '.join(cmd)}")
    completed = subprocess.run(cmd, env=env, check=False)
    return int(completed.returncode)


def load_raw_records(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("records") or []


def aggregate_run_metrics(records: list[dict], run_id: str, test_case: str, repeat_index: int, locust_exit_code: int) -> dict:
    ok_records = [r for r in records if r.get("ok")]

    queue_wait = [float(r.get("queue_wait_time_sec", 0) or 0) for r in ok_records]
    processing = [float(r.get("processing_time_sec", 0) or 0) for r in ok_records]
    e2e = [float(r.get("end_to_end_latency_sec", 0) or 0) for r in ok_records]
    received_ts = [r.get("request_received_timestamp") for r in ok_records if r.get("request_received_timestamp")]
    finished_ts = [r.get("response_finished_timestamp") for r in ok_records if r.get("response_finished_timestamp")]

    makespan_sec = 0.0
    if received_ts and finished_ts:
        start = min(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in received_ts)
        end = max(datetime.fromisoformat(x.replace("Z", "+00:00")) for x in finished_ts)
        makespan_sec = (end - start).total_seconds()
    elif e2e:
        makespan_sec = sum(e2e)

    files_count = len(ok_records)
    records_count = int(sum(int(r.get("processed_rows", 0) or 0) + int(r.get("failed_rows", 0) or 0) for r in ok_records))
    created_total = int(sum(int(r.get("created_trackers", 0) or 0) for r in ok_records))
    existing_total = int(sum(int(r.get("existing_trackers", 0) or 0) for r in ok_records))
    failed_total = int(sum(int(r.get("failed_rows", 0) or 0) for r in ok_records))
    active_parallel = [int(r.get("active_processing_requests", 0) or 0) for r in ok_records]
    max_active_seen = [int(r.get("max_active_requests_seen", 0) or 0) for r in ok_records]

    files_per_sec = (files_count / makespan_sec) if makespan_sec > 0 else 0.0
    records_per_sec = (records_count / makespan_sec) if makespan_sec > 0 else 0.0

    return {
        "run_id": run_id,
        "test_case": test_case,
        "repeat_index": repeat_index,
        "locust_exit_code": locust_exit_code,
        "requests_total": len(records),
        "requests_ok": files_count,
        "requests_failed": len(records) - files_count,
        "total_processing_time_sec": round(makespan_sec, 6),
        "files_per_sec": round(files_per_sec, 6),
        "records_per_sec": round(records_per_sec, 6),
        "processing_time_p50_sec": round(float(median(processing)) if processing else 0.0, 6),
        "processing_time_p95_sec": round(_percentile(processing, 0.95), 6),
        "end_to_end_latency_p95_sec": round(_percentile(e2e, 0.95), 6),
        "queue_wait_time_p95_sec": round(_percentile(queue_wait, 0.95), 6),
        "created_trackers_total": created_total,
        "existing_trackers_total": existing_total,
        "failed_rows_total": failed_total,
        "max_active_processing_requests_observed": int(max(active_parallel) if active_parallel else 0),
        "max_active_requests_seen_observed": int(max(max_active_seen) if max_active_seen else 0),
    }


def write_case_outputs(case_dir: Path, run_row: dict, records_payload: dict):
    raw_dir = case_dir / "raw"
    agg_dir = case_dir / "aggregated"
    raw_dir.mkdir(parents=True, exist_ok=True)
    agg_dir.mkdir(parents=True, exist_ok=True)
    run_id = run_row["run_id"]
    raw_path = raw_dir / f"{run_id}.json"
    raw_path.write_text(json.dumps(records_payload, indent=2), encoding="utf-8")

    runs_csv = agg_dir / "runs.csv"
    fields = list(run_row.keys())
    file_exists = runs_csv.exists()
    with runs_csv.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        if not file_exists:
            writer.writeheader()
        writer.writerow(run_row)


def write_case_summary(case_dir: Path):
    agg_dir = case_dir / "aggregated"
    runs_csv = agg_dir / "runs.csv"
    if not runs_csv.exists():
        return
    with runs_csv.open("r", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return
    numeric_keys = [
        "total_processing_time_sec",
        "files_per_sec",
        "records_per_sec",
        "processing_time_p50_sec",
        "processing_time_p95_sec",
        "end_to_end_latency_p95_sec",
        "queue_wait_time_p95_sec",
        "created_trackers_total",
        "existing_trackers_total",
        "failed_rows_total",
        "max_active_processing_requests_observed",
        "max_active_requests_seen_observed",
    ]

    summary = {
        "test_case": rows[0]["test_case"],
        "runs": len(rows),
    }
    for k in numeric_keys:
        vals = [float(r.get(k, 0) or 0) for r in rows]
        summary[f"{k}_mean"] = round(sum(vals) / len(vals), 6)
        summary[f"{k}_min"] = round(min(vals), 6)
        summary[f"{k}_max"] = round(max(vals), 6)

    summary_path = agg_dir / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)


def main():
    args = build_parser().parse_args()
    test_cases = _split_csv(args.test_cases)
    for c in test_cases:
        if c not in CASE_TO_FOLDER:
            raise ValueError(f"Unknown test case: {c}")
    if args.total_requests <= 0:
        raise ValueError("--total-requests must be > 0")
    if args.repeats <= 0:
        raise ValueError("--repeats must be > 0")

    for case in test_cases:
        case_dir = case_results_dir(case, args.results_dir)
        case_dir.mkdir(parents=True, exist_ok=True)
        manifest_paths = load_manifest(case)

        for repeat_index in range(1, args.repeats + 1):
            run_id = f"{case}_r{repeat_index}_{_now_utc()}"
            print(f"\n=== {case} repeat {repeat_index} ({run_id}) ===")
            reset_and_seed_db(args.host)
            clear_month_dirs(args.month)
            staged_paths = stage_files(manifest_paths, args.month, args.total_requests)

            raw_json_path = case_dir / "raw" / f"{run_id}.json"
            run_spec_payload = {
                "test_case": case,
                "run_id": run_id,
            }
            exit_code = run_locust(args, run_spec_payload, raw_json_path)
            if exit_code != 0:
                print(f"  Locust exited with code {exit_code}. Aggregating partial/failed run from raw results.")

            if not raw_json_path.exists():
                raise RuntimeError(
                    f"Raw results not found for run {run_id} at {raw_json_path}. "
                    f"Locust exit code was {exit_code}."
                )
            records_payload = json.loads(raw_json_path.read_text(encoding="utf-8"))
            records = records_payload.get("records") or []
            run_row = aggregate_run_metrics(records, run_id, case, repeat_index, exit_code)
            print("Step 5/5: Write results")
            write_case_outputs(case_dir, run_row, records_payload)
            print(
                "  Run KPI snapshot: "
                f"makespan={run_row['total_processing_time_sec']}s, "
                f"files/s={run_row['files_per_sec']}, "
                f"records/s={run_row['records_per_sec']}, "
                f"max_parallel={run_row['max_active_processing_requests_observed']}"
            )
            print(f"  Raw JSON: {raw_json_path}")

        write_case_summary(case_dir)
        print(f"Completed case {case}. Results: {case_dir}")
        print(f"  Aggregated CSV: {case_dir / 'aggregated' / 'runs.csv'}")
        print(f"  Summary CSV: {case_dir / 'aggregated' / 'summary.csv'}")

    if args.results_dir.strip():
        print(f"\nAll done. Results root override: {args.results_dir}")
    else:
        print("\nAll done. Results written under each data/benchmarks/<case>/locust_results folder.")


if __name__ == "__main__":
    main()
