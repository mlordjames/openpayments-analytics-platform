#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple

import click

from src import dataset_ids
from src.download_general_payments import (
    DEFAULT_METADATA_CACHE,
    DEFAULT_OUT_ROOT,
    EXPECTED_MIN_COLS,
    MAX_PAGE_WORKERS_CAP,
    PAGE_LIMIT,
    download_big_parallel_pages,
    download_small_sequential,
    get_thread_session,
)


DATASET_CHOICES = ["general-payments"]


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")


def extract_expected_header_from_schema(schema_payload: Dict) -> List[str]:
    query = schema_payload.get("query") or {}
    properties = query.get("properties") or []
    resources = query.get("resources") or []
    schema_root = schema_payload.get("schema") or {}

    if not properties:
        raise ValueError("Schema payload missing query.properties")
    if not resources:
        raise ValueError("Schema payload missing query.resources")

    resource_id = str(resources[0]["id"])
    fields = (schema_root.get(resource_id) or {}).get("fields") or {}

    expected: List[str] = []
    for prop in properties:
        field_meta = fields.get(prop) or {}
        expected.append(field_meta.get("description") or prop)

    return expected


def read_csv_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def validate_header_exact(path: Path, expected_header: List[str]) -> Tuple[bool, str]:
    if not path.exists() or path.stat().st_size == 0:
        return False, "missing_or_empty"

    try:
        actual = [c.strip() for c in read_csv_header(path)]
    except Exception as e:
        return False, f"header_read_error:{e}"

    expected = [c.strip() for c in expected_header]

    if len(actual) < EXPECTED_MIN_COLS:
        return False, f"too_few_columns(actual={len(actual)}, expected_min={EXPECTED_MIN_COLS})"
    if actual == expected:
        return True, "ok"
    if len(actual) != len(expected):
        return False, f"column_count_mismatch(actual={len(actual)}, expected={len(expected)})"

    mismatches = [idx for idx, (a, b) in enumerate(zip(actual, expected)) if a != b]
    if mismatches:
        first = mismatches[0]
        return False, f"header_mismatch_at_index={first}: actual='{actual[first]}' expected='{expected[first]}'"

    return False, "unknown_header_mismatch"


def infer_company_id_from_path(path: Path) -> str:
    m = re.search(r"company_id=([^/\\]+)\.csv$", str(path))
    if not m:
        raise ValueError(f"Could not infer company_id from path: {path}")
    return m.group(1)


def load_expected_totals_from_latest_report(out_root: Path, dataset: str, year: int) -> Dict[str, int]:
    runs_root = out_root / "metadata" / "runs"
    reports = sorted(
        runs_root.glob(f"run_id=*/download_report_{dataset}_{year}.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not reports:
        raise FileNotFoundError(f"No download report found for dataset={dataset}, year={year}")

    report_path = reports[0]
    mapping: Dict[str, int] = {}
    with report_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping[str(row["company_id"])] = int(float(row["expected_total"]))

    logging.info("Using report for expected totals: %s", report_path)
    return mapping


def load_schema_for_year(year: int, metadata_cache: Path):
    _, schema_payload, schema_path = dataset_ids.getdatasetid_and_schema(year=year, cache_dir=metadata_cache)
    if schema_payload is None or schema_path is None:
        raise FileNotFoundError(f"No schema cache available for year={year}")
    return schema_payload, schema_path


def redownload_company_file(
    *,
    dataset: str,
    year: int,
    company_id: str,
    expected_total: int,
    out_root: Path,
    metadata_cache: Path,
    page_workers: int,
):
    dataset_id = dataset_ids.getdatasetids(cache_dir=str(metadata_cache))[str(year)]
    base_url = f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/download"
    year_dir = out_root / f"dataset={dataset}" / f"year={year}"
    session = get_thread_session(pool_size=max(6, page_workers * 3))

    if expected_total <= PAGE_LIMIT:
        _, _, ok, msg, _ = download_small_sequential(
            session=session,
            url=base_url,
            company_id=company_id,
            year=year,
            year_dir=year_dir,
            resume=False,
        )
        return ok, msg

    _, _, ok, msg, _ = download_big_parallel_pages(
        session=session,
        url=base_url,
        company_id=company_id,
        expected_total=expected_total,
        year=year,
        year_dir=year_dir,
        page_workers=min(page_workers, MAX_PAGE_WORKERS_CAP),
        resume=False,
    )
    return ok, msg


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--dataset", type=click.Choice(DATASET_CHOICES, case_sensitive=False), default="general-payments", show_default=True)
@click.option("--year", type=int, required=True)
@click.option("--out-root", type=click.Path(path_type=Path), default=DEFAULT_OUT_ROOT, show_default=True)
@click.option("--metadata-cache", type=click.Path(path_type=Path), default=DEFAULT_METADATA_CACHE, show_default=True)
@click.option("--max-redownload-attempts", type=int, default=3, show_default=True)
@click.option("--page-workers", type=int, default=5, show_default=True)
@click.option("--verbose", is_flag=True)
def cli(dataset, year, out_root, metadata_cache, max_redownload_attempts, page_workers, verbose):
    setup_logging(verbose)
    out_root = out_root.resolve()
    metadata_cache = metadata_cache.resolve()

    schema_payload, schema_path = load_schema_for_year(year, metadata_cache)
    expected_header = extract_expected_header_from_schema(schema_payload)
    logging.info("Loaded schema file: %s", schema_path)

    year_dir = out_root / f"dataset={dataset}" / f"year={year}"
    if not year_dir.exists():
        raise SystemExit(f"Year folder not found: {year_dir}")

    expected_totals = load_expected_totals_from_latest_report(out_root, dataset, year)
    bad_files: List[Path] = []

    for csv_path in sorted(year_dir.glob("company_id=*.csv")):
        ok, reason = validate_header_exact(csv_path, expected_header)
        if ok:
            continue

        logging.warning("Schema mismatch: %s | %s", csv_path, reason)
        company_id = infer_company_id_from_path(csv_path)
        expected_total = expected_totals.get(company_id)
        if expected_total is None:
            logging.error("Missing expected_total in report for company_id=%s", company_id)
            bad_files.append(csv_path)
            continue

        backup_path = csv_path.with_suffix(".csv.schema_mismatch.bak")
        if backup_path.exists():
            backup_path.unlink()
        shutil.move(str(csv_path), str(backup_path))

        validated = False
        last_msg = reason
        for attempt in range(1, max_redownload_attempts + 1):
            logging.info("Redownload attempt %d/%d for %s", attempt, max_redownload_attempts, csv_path)
            ok_dl, msg_dl = redownload_company_file(
                dataset=dataset,
                year=year,
                company_id=company_id,
                expected_total=expected_total,
                out_root=out_root,
                metadata_cache=metadata_cache,
                page_workers=page_workers,
            )
            last_msg = msg_dl
            if not ok_dl:
                logging.warning("Redownload failed for %s: %s", csv_path, msg_dl)
                continue

            ok_val, reason_val = validate_header_exact(csv_path, expected_header)
            if ok_val:
                validated = True
                logging.info("Schema validation passed after redownload: %s", csv_path)
                break

            last_msg = reason_val
            logging.warning("Redownloaded file still mismatched: %s | %s", csv_path, reason_val)
            if csv_path.exists():
                csv_path.unlink()

        if validated:
            if backup_path.exists():
                backup_path.unlink()
            continue

        if csv_path.exists():
            csv_path.unlink()
        if backup_path.exists():
            shutil.move(str(backup_path), str(csv_path))

        logging.error("Leaving original file in place after failed validation attempts: %s | %s", csv_path, last_msg)
        bad_files.append(csv_path)

    validation_dir = out_root / "metadata" / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    out_json = validation_dir / f"schema_mismatch_paths_{dataset}_{year}.json"
    out_json.write_text(json.dumps([str(p) for p in bad_files], indent=2), encoding="utf-8")

    logging.info("Validation complete. Mismatched files remaining: %d", len(bad_files))
    logging.info("Mismatch path list saved: %s", out_json)

    if bad_files:
        raise SystemExit(2)


if __name__ == "__main__":
    cli()
