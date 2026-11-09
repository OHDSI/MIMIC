#!/usr/bin/env python3
"""
Upload OMOP vocabulary tables (Athena + custom *_delta) into BigQuery
using the official OMOP vocabulary schemas.

This script:
  • Maps vocabulary filenames (concept.csv, concept_relationship.csv, etc.)
    to the correct OMOP table schemas.
  • Normalizes DATE columns where required (valid_start_date, valid_end_date)
    into ISO YYYY-MM-DD so that BigQuery can load them into DATE-typed columns.
  • Allows optional fast-path copying of *_delta files if the user asserts
    their date fields are already correctly formatted.
  • Strips the `_delta` suffix from filenames so `concept_delta.csv` loads
    into table `concept`.

--------------------------------------------------------------------------------
USAGE
--------------------------------------------------------------------------------

  python upload_vocab_to_bq.py \
      --input_dir /path/to/vocabs \
      --project <PROJECT_ID> \
      --dataset <DATASET> \
      --location US \
      --replace

Required arguments:
  --input_dir    Directory containing Athena vocab files or *_delta files.
  --project      BigQuery project ID.
  --dataset      BigQuery dataset ID.

Optional arguments:
  --location     BigQuery location (e.g., US, EU). Default: none.
  --replace      Overwrite existing tables (bq load --replace).
  --dry_run      Show commands that would be run, but perform no loads.
  --assume_delta_is_iso
                 If set, *_delta files are assumed to already contain
                 ISO-formatted YYYY-MM-DD date fields. They will be copied
                 directly (no normalization pass).

--------------------------------------------------------------------------------
EXAMPLES
--------------------------------------------------------------------------------

1. Load Athena vocabularies (normalize dates as needed):

  python upload_vocab_to_bq.py \
      --input_dir ~/athena_files \
      --project my-proj \
      --dataset omop_vocab \
      --location US \
      --replace

2. Load custom *_delta files, known to already use ISO YYYY-MM-DD dates:

  python upload_vocab_to_bq.py \
      --input_dir ./vocab_deltas \
      --project my-proj \
      --dataset omop_vocab_delta \
      --location US \
      --replace \
      --assume_delta_is_iso

3. Dry run (preview commands only):

  python upload_vocab_to_bq.py \
      --input_dir ./vocab_deltas \
      --project my-proj \
      --dataset omop_vocab_delta \
      --location US \
      --dry_run

--------------------------------------------------------------------------------
NOTES
--------------------------------------------------------------------------------

• BigQuery DATE columns *must* use ISO YYYY-MM-DD when loading from CSV/TSV.
  Non-ISO formats (e.g., YYYYMMDD) will fail in DATE-typed columns.

• Athena files typically contain YYYYMMDD in valid_start_date / valid_end_date.
  These will be normalized automatically unless you choose to load as STRING
  (this script loads into proper DATE fields).

• *_delta files:
      - By default, are processed the same way as Athena files (normalized).
      - If you set --assume_delta_is_iso, they will be copied directly, which
        avoids unnecessary rewriting if you know they already conform.

• Unknown filenames are skipped; a summary is printed at the end.

• All load operations use `bq load` with an explicit schema, strict parsing,
  and (optionally) `--replace` to avoid accidental table growth.
"""

import argparse
import csv
import json
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List, Tuple

# ------------------------------
# Standard OMOP vocabulary schemas (BigQuery JSON schema format)
# ------------------------------
SCHEMAS: Dict[str, List[Dict[str, str]]] = {
    'concept': [
        {"name": "concept_id", "type": "INTEGER"},
        {"name": "concept_name", "type": "STRING"},
        {"name": "domain_id", "type": "STRING"},
        {"name": "vocabulary_id", "type": "STRING"},
        {"name": "concept_class_id", "type": "STRING"},
        {"name": "standard_concept", "type": "STRING"},
        {"name": "concept_code", "type": "STRING"},
        {"name": "valid_start_date", "type": "DATE"},
        {"name": "valid_end_date", "type": "DATE"},
        {"name": "invalid_reason", "type": "STRING"},
    ],
    'concept_relationship': [
        {"name": "concept_id_1", "type": "INTEGER"},
        {"name": "concept_id_2", "type": "INTEGER"},
        {"name": "relationship_id", "type": "STRING"},
        {"name": "valid_start_date", "type": "DATE"},
        {"name": "valid_end_date", "type": "DATE"},
        {"name": "invalid_reason", "type": "STRING"},
    ],
    'concept_ancestor': [
        {"name": "ancestor_concept_id", "type": "INTEGER"},
        {"name": "descendant_concept_id", "type": "INTEGER"},
        {"name": "min_levels_of_separation", "type": "INTEGER"},
        {"name": "max_levels_of_separation", "type": "INTEGER"},
    ],
    'concept_synonym': [
        {"name": "concept_id", "type": "INTEGER"},
        {"name": "concept_synonym_name", "type": "STRING"},
        {"name": "language_concept_id", "type": "INTEGER"},
    ],
    'vocabulary': [
        {"name": "vocabulary_id", "type": "STRING"},
        {"name": "vocabulary_name", "type": "STRING"},
        {"name": "vocabulary_reference", "type": "STRING"},
        {"name": "vocabulary_version", "type": "STRING"},
        {"name": "vocabulary_concept_id", "type": "INTEGER"},
    ],
    'relationship': [
        {"name": "relationship_id", "type": "STRING"},
        {"name": "relationship_name", "type": "STRING"},
        {"name": "is_hierarchical", "type": "STRING"},
        {"name": "defines_ancestry", "type": "STRING"},
        {"name": "reverse_relationship_id", "type": "STRING"},
        {"name": "relationship_concept_id", "type": "INTEGER"},
    ],
    'drug_strength': [
        {"name": "drug_concept_id", "type": "INTEGER"},
        {"name": "ingredient_concept_id", "type": "INTEGER"},
        {"name": "amount_value", "type": "FLOAT"},
        {"name": "amount_unit_concept_id", "type": "INTEGER"},
        {"name": "numerator_value", "type": "FLOAT"},
        {"name": "numerator_unit_concept_id", "type": "INTEGER"},
        {"name": "denominator_value", "type": "FLOAT"},
        {"name": "denominator_unit_concept_id", "type": "INTEGER"},
        {"name": "box_size", "type": "INTEGER"},
        {"name": "valid_start_date", "type": "DATE"},
        {"name": "valid_end_date", "type": "DATE"},
        {"name": "invalid_reason", "type": "STRING"},
    ],
    'domain': [
        {"name": "domain_id", "type": "STRING"},
        {"name": "domain_name", "type": "STRING"},
        {"name": "domain_concept_id", "type": "INTEGER"},
    ],
    'concept_class': [
        {"name": "concept_class_id", "type": "STRING"},
        {"name": "concept_class_name", "type": "STRING"},
        {"name": "concept_class_concept_id", "type": "INTEGER"},
    ],
}

# Tables that actually have date columns
DATE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    'concept': ('valid_start_date', 'valid_end_date'),
    'concept_relationship': ('valid_start_date', 'valid_end_date'),
    'drug_strength': ('valid_start_date', 'valid_end_date'),
    # relationship and vocabulary do not have date columns per OMOP vocab specs
    # concept_ancestor, concept_synonym, domain, concept_class do not have date cols
}

_YYYYMMDD = re.compile(r'^\d{8}$')
_YYYY_MM_DD = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _set_csv_field_size_limit() -> None:
    """
    Increase Python csv module field size limit so very long Athena vocab
    fields (e.g., concept_synonym_name) don't trigger
    'field larger than field limit (131072)' during streaming.
    """
    import csv
    import sys

    try:
        # Try the largest possible value for this platform
        csv.field_size_limit(sys.maxsize)
    except (OverflowError, ValueError):
        # Fall back progressively to something large enough for vocab files
        for limit in (2**31 - 1, 2**30, 2**29, 16 * 1024 * 1024):
            try:
                csv.field_size_limit(limit)
                break
            except (OverflowError, ValueError):
                continue


def choose_csv_settings(is_delta: bool) -> tuple[str, str]:
    """
    Returns (field_delimiter, quote_char) for bq load.
    - *_delta: comma CSV with quote '"'
    - Athena:  tab TSV with quoting disabled (empty quote char)
    """
    if is_delta:
        return (',', '"')
    else:
        return ('\t', '')  # TSV; treat all quotes as literal


def _to_iso_date(value: str) -> str:
    if value is None:
        return value
    s = value.strip()
    if not s:
        return s
    if _YYYY_MM_DD.match(s):
        return s
    if _YYYYMMDD.match(s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s


def normalize_dates_streaming(src_path: Path, dst_path: Path, table_name: str) -> None:
    """Stream through a TSV/CSV file and normalize date columns in-place to YYYY-MM-DD.
    Writes a new file at dst_path. Delimiter is hard-coded to tab (Athena default).
    """
    # Only do work if this table uses date columns
    date_cols = DATE_COLUMNS.get(table_name)
    if not date_cols:
        # Just copy file without touching
        with src_path.open('rb') as rf, dst_path.open('wb') as wf:
            wf.write(rf.read())
        return

    with src_path.open('r', encoding='utf-8', newline='') as rf, \
         dst_path.open('w', encoding='utf-8', newline='') as wf:
        reader = csv.reader(rf, delimiter='\t', quotechar='"')
        writer = csv.writer(wf, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Read header
        try:
            header = next(reader)
        except StopIteration:
            # Empty file; just write nothing
            return

        # Map date column indices
        col_index = {name: i for i, name in enumerate(header)}
        date_idx = [col_index[c] for c in date_cols if c in col_index]

        # Write header unchanged
        writer.writerow(header)

        if not date_idx:
            # No date columns present in file; just stream rows
            for row in reader:
                writer.writerow(row)
            return

        for row in reader:
            # Defensive: ensure row has expected length
            if len(row) < len(header):
                row = row + [''] * (len(header) - len(row))
            for i in date_idx:
                if i < len(row):
                    row[i] = _to_iso_date(row[i])
            writer.writerow(row)


def write_schema_to_temp(schema: List[Dict[str, str]], tmpdir: Path) -> Path:
    path = tmpdir / 'schema.json'
    with path.open('w', encoding='utf-8') as f:
        json.dump(schema, f)
    return path


def build_bq_load_command(
    *, location: str, project: str, dataset: str, table: str,
    schema_path: Path, source_file: Path, replace: bool,
    field_delimiter: str, quote_char: str
) -> List[str]:
    cmd: List[str] = ['bq']
    if location:
        cmd += ['--location', location]
    cmd += [
        'load',
        '--source_format=CSV',
        f'--field_delimiter={field_delimiter}',
        '--skip_leading_rows=1',
        '--allow_quoted_newlines',
        '--max_bad_records=0',
        f'--schema={schema_path}',
    ]
    # IMPORTANT: pass --quote as two args; if quote_char == '' we still pass an empty value
    cmd += ['--quote', quote_char]

    cmd.append('--replace' if replace else '--noreplace')
    cmd += [f'{project}:{dataset}.{table}', str(source_file)]
    return cmd


def infer_table_name_from_file(path: Path) -> Tuple[str, bool]:
    stem = path.stem.lower()
    is_delta = False
    if stem.endswith('_delta'):
        is_delta = True
        stem = stem[:-6]
    return stem, is_delta


def main() -> int:
    parser = argparse.ArgumentParser(description='Upload OMOP vocabulary tables to BigQuery with standard schema.')
    parser.add_argument('--input_dir', required=True, help='Path to directory with vocab files (TSV/CSV).')
    parser.add_argument('--project', required=True, help='BigQuery project ID.')
    parser.add_argument('--dataset', required=True, help='BigQuery dataset ID.')
    parser.add_argument('--location', default='', help='BigQuery location (e.g., US, EU, us-east4). Optional.')
    parser.add_argument('--replace', action='store_true', help='Replace the destination table (truncate/overwrite).')
    parser.add_argument('--dry_run', action='store_true', help='Show commands without executing `bq`.')
    parser.add_argument('--assume_delta_is_iso', action='store_true', help='Assume *_delta files already use YYYY-MM-DD and skip normalization.')

    args = parser.parse_args()

    # Ensure the csv parser can handle very long fields
    _set_csv_field_size_limit()

    in_dir = Path(args.input_dir)
    if not in_dir.is_dir():
        print(f"Error: input_dir not found: {in_dir}", file=sys.stderr)
        return 2

    candidates = sorted(list(in_dir.glob('*.csv')) + list(in_dir.glob('*.tsv')))
    if not candidates:
        print(f"No .csv or .tsv files found in {in_dir}")
        return 0

    total = 0
    success = 0
    skipped: List[str] = []
    failed: List[str] = []

    for path in candidates:
        table, is_delta = infer_table_name_from_file(path)
        if table not in SCHEMAS:
            skipped.append(path.name)
            continue
        total += 1

        with tempfile.TemporaryDirectory(prefix='omop_vocab_') as td:
            tmpdir = Path(td)
            norm_path = tmpdir / f"{table}.normalized.tsv"

            # Decide if we bypass normalization entirely for *_delta
            fastcopy_only = bool(args.assume_delta_is_iso and is_delta and table in DATE_COLUMNS)

            try:
                if fastcopy_only:
                    with path.open('rb') as rf, norm_path.open('wb') as wf:
                        wf.write(rf.read())
                else:
                    normalize_dates_streaming(path, norm_path, table)
            except Exception as e:
                print(f"[!] Failed to prepare file {path.name}: {e}", file=sys.stderr)
                traceback.print_exc
                failed.append(table)
                continue

            schema_path = write_schema_to_temp(SCHEMAS[table], tmpdir)

            field_delimiter, quote_char = choose_csv_settings(is_delta)

            cmd = build_bq_load_command(
                location=args.location,
                project=args.project,
                dataset=args.dataset,
                table=table,
                schema_path=schema_path,
                source_file=norm_path,
                replace=bool(args.replace),
                field_delimiter=field_delimiter,
                quote_char=quote_char,
            )

            if args.dry_run:
                print('-' * 80)
                print(f"[DRY RUN] Would load: {path.name} → {args.project}:{args.dataset}.{table}")
                print(f"           Detected table: {table}  (is_delta={is_delta}, fastcopy_only={fastcopy_only})")
                print(f"           Command:       {' '.join(cmd)}")
                success += 1
                continue

            print('-' * 80)
            print(f"Loading {path.name} → {args.project}:{args.dataset}.{table}")
            try:
                proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
                if proc.returncode == 0:
                    print(f"✓ Loaded {table} successfully")
                    success += 1
                else:
                    print(f"✗ bq load failed for {table} (exit={proc.returncode})", file=sys.stderr)
                    if proc.stderr:
                        print(proc.stderr.strip(), file=sys.stderr)
                    if proc.stdout:
                        print(proc.stdout.strip(), file=sys.stderr)
                    failed.append(table)
            except FileNotFoundError:
                print("Error: `bq` CLI not found. Install Google Cloud SDK and ensure `bq` is on PATH.",
                      file=sys.stderr)
                return 1

    print('\n' + '=' * 80)
    print('Summary')
    print('=' * 80)
    print(f"Tables matched:   {total}")
    print(f"Successful loads: {success}")
    if skipped:
        print(f"Skipped (unrecognized filenames): {len(skipped)} → {', '.join(skipped)}")
    if failed:
        print(f"Failed: {len(failed)} → {', '.join(failed)}")
    print('=' * 80)

    return 0 if not failed else 1


if __name__ == '__main__':
    sys.exit(main())
