#!/usr/bin/env python3
from __future__ import annotations

"""
Anonymize client-identifying text in files by replacing case-insensitive
client name tokens with "CLIENT_X".

Behavior:
- Replaces matches in both file content and output path segments.
- Supports processing specific files (--input-file, repeatable) or all files
  under an input directory (--input-dir).
- Preserves source relative structure in directory mode.
- Writes anonymized results under --output-dir.

Examples:
1) Specific files
    python scripts/anonymize_client_files.py \
    --input-file "/Users/angela.madjar/Repositories/Philips/CookieJar/cookie-jar/data/Master Cookie list & purposes (Global) - Jan 24(1P cookies) - Final.csv" \
    --input-file "/Users/angela.madjar/Repositories/Philips/CookieJar/cookie-jar/data/Master Cookie list & purposes (Global) - Jan 24(3P cookies) - Final.csv" \
    --output-dir "/Users/angela.madjar/Angela/Finki-Masters/cookie-jar-masters/data"

2) Whole directory
    python scripts/anonymize_client_files.py \
    --input-dir "/Users/angela.madjar/Repositories/Philips/CookieJar/cookie-jar/data" \
    --output-dir "/Users/angela.madjar/Angela/Finki-Masters/cookie-jar-masters/data"
"""


import argparse
import re
from pathlib import Path

CLIENT_PATTERN = re.compile(r"philips", re.IGNORECASE)
REPLACEMENT = "CLIENT_X"


def anonymize_text(text: str) -> str:
    """Replace all case-insensitive client tokens in text."""
    return CLIENT_PATTERN.sub(REPLACEMENT, text)


def read_text_file(path: Path) -> tuple[str, str]:
    """Read text file with utf-8 fallback to latin-1; return (content, encoding)."""
    for encoding in ("utf-8", "latin-1"):
        try:
            return path.read_text(encoding=encoding), encoding
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Unable to decode {path}")


def process_file(source_path: Path, source_root: Path, output_root: Path) -> Path:
    """
    Anonymize a single file and write it under output_root.

    Output path keeps source_path relative-to source_root, with each path
    segment anonymized.
    """
    relative_path = source_path.relative_to(source_root)
    anonymized_relative = Path(*[anonymize_text(part) for part in relative_path.parts])
    output_path = output_root / anonymized_relative
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_text, encoding = read_text_file(source_path)
    anonymized_text = anonymize_text(file_text)
    output_path.write_text(anonymized_text, encoding=encoding)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Anonymize client name in files by replacing 'Philips' with 'CLIENT_X'."
    )
    parser.add_argument(
        "--input-file",
        action="append",
        default=[],
        help=(
            "Specific file to anonymize. Can be provided multiple times. "
            "When set, only these files are processed."
        ),
    )
    parser.add_argument(
        "--input-dir",
        default="data",
        help="Directory containing files to anonymize (default: data).",
    )
    parser.add_argument(
        "--output-dir",
        default="data_anonymized",
        help="Directory where anonymized files are written (default: data_anonymized).",
    )
    return parser


def main() -> None:
    """
    Parse arguments, collect source files, anonymize them, and write outputs.

    - In file mode (--input-file), each selected file is written directly under
      output-dir (with anonymized filename/path segments).
    - In directory mode (--input-dir), the source directory tree structure is
      preserved under output-dir.
    """
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
        source_files = sorted([path for path in input_dir.rglob("*") if path.is_file()])

    if not source_files:
        source_label = ", ".join(str(path) for path in input_files) if input_files else str(input_dir)
        print(f"No files found for input: {source_label}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    processed_paths: list[Path] = []
    if input_files:
        # Use each file's parent as root for stable output naming in file-selection mode.
        for source_file in source_files:
            processed_paths.append(process_file(source_file, source_file.parent, output_dir))
    else:
        for source_file in source_files:
            processed_paths.append(process_file(source_file, input_dir, output_dir))

    print(f"Processed {len(processed_paths)} files.")
    print(f"Anonymized output written to: {output_dir}")


if __name__ == "__main__":
    main()
