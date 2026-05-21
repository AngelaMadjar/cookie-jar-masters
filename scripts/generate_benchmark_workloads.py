#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

"""
Benchmark Workload Generation Notes
===================================

Cross-referencing in app
------------------------
For each scan row, the app maps text fields to DB IDs (type, category, vendor,
domain, cmp, etc.). It then builds a tracker key:
    (tracker_name, tracker_type_id, tracking_domain_id)

Behavior:
1. If key exists in DB: counts as existing, and ensures tracker<->cmp link
   exists.
2. If key does not exist: inserts tracker, then creates tracker<->cmp link.
3. Rows failing tracker-name validation are routed to failed output and skip
   upsert path.

How this generator creates row categories
-----------------------------------------
failed rows:
- tracker_name is rewritten to `_ga_failed_lookup_marker_*`.
- app validator treats these as invalid pattern rows and routes them to failed.

new rows:
- tracker_name is rewritten to unique `bench_new_*`.
- this avoids accidental validation failures and keeps inserts independent.

existing rows:
- rows are rewritten from an existing-key pool built from seed files only.
- pool entries are filtered to include only tracker names valid under app
  tracker-name validation rules.
- intent: these keys should already exist after reseeding DB.
- existing rows are split 50/50 into update-like vs noop-like rows.
- medium cases enforce unique existing keys within each file.
- for medium cases, if unique-existing capacity is exhausted, remaining
  existing-target rows are converted to `new` rows.
- large and skewed cases allow repeated existing keys within a file to preserve
  existing-heavy workload identity at scale.

Why this supports scalability testing
-------------------------------------
- Existing-heavy cases (T2/T4/T6): mostly existing rows ->
  lookup/match/upsert-heavy.
- New-heavy cases (T3/T5/T7): mostly new rows -> insert-heavy.
- Medium vs large (T2/T3 vs T4/T5): size-scaling behavior.
- Skewed (T6/T7): straggler effect from one very large file.
- Same request model (one file/request) + fixed concurrency supports cleaner
  queueing vs processing comparisons across workload shapes.
"""


DEFAULT_BASELINE_FOLDER = "T1_original"
DEFAULT_FILES_PER_CASE = 80
DEFAULT_SEED_FILES = [
    "data/seed/Master Cookie list & purposes (Global) - Jan 24(1P cookies) - Final.csv",
    "data/seed/Master Cookie list & purposes (Global) - Jan 24(3P cookies) - Final.csv",
]


@dataclass(frozen=True)
class CaseDef:
    test_case: str
    folder: str
    rows_per_file: int | None
    new_ratio: float
    failed_ratio: float
    existing_ratio: float
    skewed: bool
    enforce_unique_existing_within_file: bool
    huge_rows: int | None = None
    small_rows: int | None = None


@dataclass(frozen=True)
class ExistingRecord:
    tracker_name: str
    tracker_type: str
    tracking_domain: str
    vendor_name: str | None
    consent_category: str | None
    tracker_duration: str | None


CASE_DEFS = [
    CaseDef("T2", "T2_medium_existing_heavy", 2000, 0.10, 0.10, 0.80, False, True),
    CaseDef("T3", "T3_medium_new_heavy", 2000, 0.80, 0.10, 0.10, False, True),
    CaseDef("T4", "T4_large_existing_heavy", 20000, 0.10, 0.10, 0.80, False, False),
    CaseDef("T5", "T5_large_new_heavy", 20000, 0.80, 0.10, 0.10, False, False),
    CaseDef("T6", "T6_skewed_existing_heavy", None, 0.10, 0.10, 0.80, True, False, huge_rows=200000, small_rows=200),
    CaseDef("T7", "T7_skewed_new_heavy", None, 0.80, 0.10, 0.10, True, False, huge_rows=200000, small_rows=200),
]


COL_CANDIDATES = {
    "tracker_name": ("Tracker Name", "tracker_name"),
    "tracker_type": ("Tracker Type", "tracker_type"),
    "tracking_domain": ("Tracking Domain", "tracking_domain"),
    "vendor_name": ("Vendor Name", "vendor_name"),
    "consent_category": ("Consent Category", "consent_category", "New Consent Category"),
    "tracker_duration": ("Tracker Duration", "tracker_duration"),
    "tracker_purpose": ("Tracker Purpose", "tracker_purpose"),
    "tracker_purpose_source": ("Tracker Purpose (Source)", "tracker_purpose_source"),
    "vendor_description": ("Vendor Description", "vendor_description"),
}


def read_csv_flexible(path: Path) -> tuple[pd.DataFrame, str]:
    last_exc = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, sep=",", dtype=str, encoding=encoding, keep_default_na=False), encoding
        except Exception as exc:  # pragma: no cover
            last_exc = exc
    raise last_exc


def find_col(df: pd.DataFrame, logical_name: str) -> str | None:
    for c in COL_CANDIDATES[logical_name]:
        if c in df.columns:
            return c
    return None


def is_valid_tracker_name_for_existing_pool(tracker_name: str | None) -> bool:
    if not tracker_name:
        return False
    patterns = [
        "_ak_abt_*",
        "_ga_*",
        "_hjSession_*",
        "_hjSessionUser_*",
        "AMCV_*",
        "AMCVS_*",
        "signifyd_id_*",
        "ttcsid_*",
    ]
    for pattern in patterns:
        prefix = pattern.replace("*", "")
        if tracker_name.startswith(prefix):
            return tracker_name == pattern
    return True


def sample_rows(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    if len(df) == 0:
        return df.copy()
    idx = [rng.randrange(len(df)) for _ in range(n)]
    return df.iloc[idx].copy().reset_index(drop=True)


def build_existing_pool(seed_files: list[Path]) -> list[ExistingRecord]:
    # Keep one canonical record per tracker identity key so we can enforce
    # no per-file repetition of existing tracker keys when pool size allows.
    key_map: dict[tuple[str, str, str], ExistingRecord] = {}
    sources = seed_files
    for src in sources:
        df, _ = read_csv_flexible(src)
        c_name = find_col(df, "tracker_name")
        c_type = find_col(df, "tracker_type")
        c_domain = find_col(df, "tracking_domain")
        if not c_name or not c_type or not c_domain:
            continue

        c_vendor = find_col(df, "vendor_name")
        c_category = find_col(df, "consent_category")
        c_duration = find_col(df, "tracker_duration")
        for _, row in df.iterrows():
            tracker_name = str(row.get(c_name, "")).strip()
            tracker_type = str(row.get(c_type, "")).strip()
            tracking_domain = str(row.get(c_domain, "")).strip()
            if not tracker_name or not tracker_type or not tracking_domain:
                continue
            if not is_valid_tracker_name_for_existing_pool(tracker_name):
                continue
            vendor_name = str(row.get(c_vendor, "")).strip() if c_vendor else ""
            consent_category = str(row.get(c_category, "")).strip() if c_category else ""
            tracker_duration = str(row.get(c_duration, "")).strip() if c_duration else ""
            rec = ExistingRecord(
                tracker_name=tracker_name,
                tracker_type=tracker_type,
                tracking_domain=tracking_domain,
                vendor_name=vendor_name or None,
                consent_category=consent_category or None,
                tracker_duration=tracker_duration or None,
            )
            k = (rec.tracker_name, rec.tracker_type, rec.tracking_domain)
            key_map[k] = rec
    pool = list(key_map.values())
    if not pool:
        raise ValueError("Existing tracker pool is empty. Check seed/baseline inputs.")
    return pool


class ExistingSampler:
    def __init__(self, records: list[ExistingRecord], rng: random.Random):
        self.records = list(records)
        self.rng = rng
        self._reshuffle()
        self.cursor = 0

    def _reshuffle(self):
        self.rng.shuffle(self.records)

    def take(self) -> ExistingRecord:
        if self.cursor >= len(self.records):
            self.cursor = 0
            self._reshuffle()
        rec = self.records[self.cursor]
        self.cursor += 1
        return rec

    def take_unique_for_file(
        self,
        n: int,
        used_keys: set[tuple[str, str, str]],
    ) -> tuple[list[ExistingRecord], int]:
        selected: list[ExistingRecord] = []
        max_attempts = max(1000, n * 20)
        attempts = 0
        while len(selected) < n and attempts < max_attempts:
            rec = self.take()
            k = (rec.tracker_name, rec.tracker_type, rec.tracking_domain)
            attempts += 1
            if k in used_keys:
                continue
            used_keys.add(k)
            selected.append(rec)

        shortfall = n - len(selected)
        return selected, shortfall

    def take_with_repeats(self, n: int) -> list[ExistingRecord]:
        return [self.take() for _ in range(n)]


def write_manifest(folder: Path, case_def: CaseDef, totals: dict, paths: list[str], rows_per_file_value):
    manifest_dir = folder / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "test_case": case_def.test_case,
        "number_of_files": len(paths),
        "rows_per_file": rows_per_file_value,
        "total_rows": totals["total_rows"],
        "expected_new_records": totals["new_records"],
        "expected_failed_lookup_records": totals["failed_lookup_records"],
        "expected_existing_records": totals["existing_records"],
        "expected_update_records": totals["update_records"],
        "expected_noop_records": totals["noop_records"],
        "generated_file_paths": paths,
        "unique_tracker_keys_total": totals["unique_tracker_keys_total"],
        "duplicate_tracker_key_rows_total": totals["duplicate_tracker_key_rows_total"],
    }
    (manifest_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def recreate_case_folder(folder: Path):
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True, exist_ok=True)


def tracker_key_from_row(df: pd.DataFrame, idx: int) -> tuple[str, str, str] | None:
    c_name = find_col(df, "tracker_name")
    c_type = find_col(df, "tracker_type")
    c_domain = find_col(df, "tracking_domain")
    if not c_name or not c_type or not c_domain:
        return None
    name = str(df.at[idx, c_name]).strip()
    typ = str(df.at[idx, c_type]).strip()
    dom = str(df.at[idx, c_domain]).strip()
    if not name or not typ or not dom:
        return None
    return (name, typ, dom)


def apply_record_to_row(df: pd.DataFrame, idx: int, rec: ExistingRecord, cols: dict[str, str | None]):
    c_name = cols["tracker_name"]
    c_type = cols["tracker_type"]
    c_domain = cols["tracking_domain"]
    c_vendor = cols["vendor_name"]
    c_category = cols["consent_category"]
    c_duration = cols["tracker_duration"]
    c_purpose = cols["tracker_purpose"]
    c_purpose_source = cols["tracker_purpose_source"]
    c_vendor_desc = cols["vendor_description"]
    if c_name:
        df.at[idx, c_name] = rec.tracker_name
    if c_type:
        df.at[idx, c_type] = rec.tracker_type
    if c_domain:
        df.at[idx, c_domain] = rec.tracking_domain
    if c_vendor and rec.vendor_name is not None:
        df.at[idx, c_vendor] = rec.vendor_name
    if c_category and rec.consent_category is not None:
        df.at[idx, c_category] = rec.consent_category
    if c_duration and rec.tracker_duration is not None:
        df.at[idx, c_duration] = rec.tracker_duration

    # Align with intended business logic: scans should not carry these values.
    if c_purpose:
        df.at[idx, c_purpose] = ""
    if c_purpose_source:
        df.at[idx, c_purpose_source] = ""
    if c_vendor_desc:
        df.at[idx, c_vendor_desc] = ""


def generate_case(
    source_files: list[Path],
    case_def: CaseDef,
    case_folder: Path,
    rng: random.Random,
    existing_sampler: ExistingSampler,
):
    input_dir = case_folder / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    totals = {
        "total_rows": 0,
        "new_records": 0,
        "failed_lookup_records": 0,
        "existing_records": 0,
        "update_records": 0,
        "noop_records": 0,
        "unique_tracker_keys_total": 0,
        "duplicate_tracker_key_rows_total": 0,
    }
    paths = []
    new_key_counter = 0

    huge_file = source_files[0] if case_def.skewed else None
    for file_index, src in enumerate(source_files):
        base_df, encoding = read_csv_flexible(src)
        if case_def.skewed:
            target_rows = case_def.huge_rows if src == huge_file else case_def.small_rows
        else:
            target_rows = case_def.rows_per_file
        out = sample_rows(base_df, int(target_rows), rng)

        n = len(out)
        failed_n = int(round(n * case_def.failed_ratio))
        new_n = int(round(n * case_def.new_ratio))
        existing_n = max(0, n - failed_n - new_n)
        update_n = existing_n // 2
        noop_n = existing_n - update_n

        idxs = list(range(n))
        rng.shuffle(idxs)
        failed_idx = idxs[:failed_n]
        new_idx = idxs[failed_n : failed_n + new_n]
        existing_idx = idxs[failed_n + new_n :]
        update_idx = set(existing_idx[:update_n])

        cols = {
            "tracker_name": find_col(out, "tracker_name"),
            "tracker_type": find_col(out, "tracker_type"),
            "tracking_domain": find_col(out, "tracking_domain"),
            "vendor_name": find_col(out, "vendor_name"),
            "consent_category": find_col(out, "consent_category"),
            "tracker_duration": find_col(out, "tracker_duration"),
            "tracker_purpose": find_col(out, "tracker_purpose"),
            "tracker_purpose_source": find_col(out, "tracker_purpose_source"),
            "vendor_description": find_col(out, "vendor_description"),
        }
        c_name = cols["tracker_name"]
        c_duration = cols["tracker_duration"]
        if not c_name:
            raise ValueError(f"tracker_name column not found in output for {src}")

        for i in failed_idx:
            out.at[i, c_name] = f"_ga_failed_lookup_marker_{file_index}_{i}"

        for i in new_idx:
            # Global-in-case monotonic counter avoids any accidental new-key collisions
            # across files, even if file indices or row positions are reused/refactored.
            out.at[i, c_name] = f"bench_new_{case_def.test_case}_{new_key_counter}"
            new_key_counter += 1
            # Ensure scans do not carry purpose/description values.
            c_purpose = cols["tracker_purpose"]
            c_purpose_source = cols["tracker_purpose_source"]
            c_vendor_desc = cols["vendor_description"]
            if c_purpose:
                out.at[i, c_purpose] = ""
            if c_purpose_source:
                out.at[i, c_purpose_source] = ""
            if c_vendor_desc:
                out.at[i, c_vendor_desc] = ""

        existing_shortfall = 0
        shortfall_idx: list[int] = []
        if case_def.enforce_unique_existing_within_file:
            existing_recs, existing_shortfall = existing_sampler.take_unique_for_file(existing_n, set())
            kept_existing_idx = existing_idx[: len(existing_recs)]
            shortfall_idx = existing_idx[len(existing_recs) :]
        else:
            existing_recs = existing_sampler.take_with_repeats(existing_n)
            kept_existing_idx = existing_idx

        for i, rec in zip(kept_existing_idx, existing_recs):
            apply_record_to_row(out, i, rec, cols)
            if c_duration and i in update_idx:
                out.at[i, c_duration] = "bench_update_duration"

        # Medium cases enforce unique existing keys within file. If capacity is
        # exhausted, convert the remainder to new rows to preserve file size.
        if existing_shortfall > 0:
            for i in shortfall_idx:
                out.at[i, c_name] = f"bench_new_{case_def.test_case}_{new_key_counter}"
                new_key_counter += 1
                c_purpose = cols["tracker_purpose"]
                c_purpose_source = cols["tracker_purpose_source"]
                c_vendor_desc = cols["vendor_description"]
                if c_purpose:
                    out.at[i, c_purpose] = ""
                if c_purpose_source:
                    out.at[i, c_purpose_source] = ""
                if c_vendor_desc:
                    out.at[i, c_vendor_desc] = ""

            # Rebalance accounting to reflect what was actually generated.
            existing_n -= existing_shortfall
            new_n += existing_shortfall
            update_n = existing_n // 2
            noop_n = existing_n - update_n

        file_keys = []
        for i in range(n):
            k = tracker_key_from_row(out, i)
            if k:
                file_keys.append(k)
        unique_keys = len(set(file_keys))
        duplicate_rows = len(file_keys) - unique_keys

        out_path = input_dir / src.name
        out.to_csv(out_path, index=False, encoding=encoding)
        # E3 adaptation: manifests must use descriptive benchmark folder paths used by the E3 GCS bucket layout.
        paths.append(f"gs://e3-data-benchmarks/{case_folder.name}/input/{src.name}")

        totals["total_rows"] += n
        totals["new_records"] += new_n
        totals["failed_lookup_records"] += failed_n
        totals["existing_records"] += existing_n
        totals["update_records"] += update_n
        totals["noop_records"] += noop_n
        totals["unique_tracker_keys_total"] += unique_keys
        totals["duplicate_tracker_key_rows_total"] += duplicate_rows

    rows_value = (
        case_def.rows_per_file
        if not case_def.skewed
        else {"small_files": case_def.small_rows, "large_file": case_def.huge_rows}
    )
    write_manifest(case_folder, case_def, totals, paths, rows_value)


def build_parser():
    p = argparse.ArgumentParser(description="Generate T2-T7 benchmark datasets from read-only T1 baseline.")
    p.add_argument("--base-dir", default="data/benchmarks")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--files-per-case", type=int, default=DEFAULT_FILES_PER_CASE)
    p.add_argument("--baseline-folder", default=DEFAULT_BASELINE_FOLDER)
    p.add_argument(
        "--seed-file",
        action="append",
        default=[],
        help="Seed CSV path to build existing tracker pool. Can be passed multiple times.",
    )
    return p


def main():
    args = build_parser().parse_args()
    rng = random.Random(args.seed)

    base_dir = Path(args.base_dir)
    source_dir = base_dir / args.baseline_folder / "input"
    if not source_dir.exists():
        raise FileNotFoundError(f"Missing baseline folder: {source_dir}")

    baseline_files = sorted(source_dir.glob("*.csv"))
    if len(baseline_files) < args.files_per_case:
        raise ValueError(
            f"Expected at least {args.files_per_case} baseline files, found {len(baseline_files)}"
        )
    baseline_files = baseline_files[: args.files_per_case]

    seed_files = [Path(p) for p in (args.seed_file or DEFAULT_SEED_FILES)]
    for seed_path in seed_files:
        if not seed_path.exists():
            raise FileNotFoundError(f"Missing seed file: {seed_path}")

    existing_pool = build_existing_pool(seed_files)
    existing_sampler = ExistingSampler(existing_pool, rng)

    for case_def in CASE_DEFS:
        case_folder = base_dir / case_def.folder
        recreate_case_folder(case_folder)
        generate_case(
            source_files=baseline_files,
            case_def=case_def,
            case_folder=case_folder,
            rng=rng,
            existing_sampler=existing_sampler,
        )

    print("Generated T2-T7 benchmark datasets and manifests from read-only T1 baseline.")


if __name__ == "__main__":
    main()
