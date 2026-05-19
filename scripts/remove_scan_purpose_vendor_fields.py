#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

"""
Examples

Run specific files:
python3 scripts/remove_scan_purpose_vendor_fields.py \
  --input-file data/benchmarks/T1_original/input/micro.CLIENT_X.com-2026-02.csv \
  --output-dir data/sanitized_scans

Run all files in a folder:
python3 scripts/remove_scan_purpose_vendor_fields.py \
  --input-dir data/benchmarks/T1_original/input \
  --output-dir data/sanitized_scans
"""


FIELDS_TO_CLEAR = {
    "Tracker Purpose",
    "tracker_purpose",
    "Tracker Purpose (Source)",
    "tracker_purpose_source",
    "Vendor Description",
    "vendor_description",
}


def read_csv_flexible(path: Path) -> tuple[pd.DataFrame, str]:
    last_exc = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            df = pd.read_csv(path, sep=",", dtype=str, encoding=encoding, keep_default_na=False)
            return df, encoding
        except Exception as exc:  # pragma: no cover
            last_exc = exc
    raise last_exc


def clear_optional_fields(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    touched = 0
    for col in FIELDS_TO_CLEAR:
        if col in df.columns:
            df[col] = ""
            touched += 1
    return df, touched


def process_file(source_path: Path, source_root: Path, output_root: Path) -> tuple[Path, int]:
    relative_path = source_path.relative_to(source_root)
    output_path = output_root / relative_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df, encoding = read_csv_flexible(source_path)
    out_df, touched = clear_optional_fields(df)
    out_df.to_csv(output_path, index=False, encoding=encoding)
    return output_path, touched


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Remove tracker purpose/vendor description content from scan files by clearing known columns."
        )
    )
    parser.add_argument(
        "--input-file",
        action="append",
        default=[],
        help="Specific file to process. Can be provided multiple times.",
    )
    parser.add_argument(
        "--input-dir",
        default="data/monthly_tracker_audits/input",
        help="Directory containing scan files (default: data/monthly_tracker_audits/input).",
    )
    parser.add_argument(
        "--output-dir",
        default="data/sanitized_scans",
        help="Output directory for sanitized files (default: data/sanitized_scans).",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir).resolve()
    input_files = [Path(path).resolve() for path in args.input_file]

    if input_files:
        source_files = []
        for file_path in input_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Input file does not exist: {file_path}")
            if not file_path.is_file():
                raise IsADirectoryError(f"Input path is not a file: {file_path}")
            source_files.append(file_path)
    else:
        input_dir = Path(args.input_dir).resolve()
        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
        if not input_dir.is_dir():
            raise NotADirectoryError(f"Input path is not a directory: {input_dir}")
        source_files = sorted([path for path in input_dir.rglob("*") if path.is_file() and path.suffix.lower() == ".csv"])

    if not source_files:
        print("No CSV files found to process.")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    touched_total = 0
    if input_files:
        for source_file in source_files:
            _, touched = process_file(source_file, source_file.parent, output_dir)
            touched_total += touched
    else:
        input_dir = Path(args.input_dir).resolve()
        for source_file in source_files:
            _, touched = process_file(source_file, input_dir, output_dir)
            touched_total += touched

    print(f"Processed {len(source_files)} files.")
    print(f"Cleared purpose/description columns occurrences: {touched_total}")
    print(f"Sanitized output written to: {output_dir}")


if __name__ == "__main__":
    main()
