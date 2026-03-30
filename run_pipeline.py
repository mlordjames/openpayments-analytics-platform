# run_pipeline.py
#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click

# Import the two CLI entrypoints as callable functions
# (These are the rewritten Airflow-ready scripts you placed in /scripts)
from scripts.download_general_payments import download_year  # type: ignore
from scripts.upload_run_to_s3_full_files import main as upload_main  # type: ignore


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_OUT_ROOT = REPO_ROOT / "data" / "out"
DEFAULT_TOTALS_DIR = REPO_ROOT / "data" / "totals"
DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
# --- Download args
@click.option("--dataset", type=click.Choice(["general-payments"], case_sensitive=False), required=True)
@click.option("--year", type=int, required=True)
@click.option("--out-root", type=click.Path(path_type=Path), default=DEFAULT_OUT_ROOT, show_default=True)
@click.option("--totals-dir", type=click.Path(path_type=Path), default=DEFAULT_TOTALS_DIR, show_default=True)
@click.option("--ensure-totals/--no-ensure-totals", default=True, show_default=True)
@click.option("--rescrape-totals", is_flag=True)
@click.option("--max-files", type=int, default=0, show_default=True)
@click.option("--id-workers", type=int, default=10, show_default=True)
@click.option("--page-workers", type=int, default=5, show_default=True)
@click.option("--totals-workers", type=int, default=10, show_default=True)
@click.option("--totals-limit", type=int, default=100, show_default=True)
@click.option("--totals-country", type=str, default="UNITED STATES", show_default=True)
@click.option("--slice", "slice_str", type=str, default=None)
@click.option("--resume/--no-resume", default=True, show_default=True)
@click.option("--verbose", is_flag=True)

# --- Upload args
@click.option("--output", type=click.Choice(["local", "s3"], case_sensitive=False), default="local", show_default=True)
@click.option("--bucket", type=str, default=DEFAULT_BUCKET, show_default=True)
@click.option("--overwrite/--no-overwrite", default=False, show_default=True)
@click.option("--include-metadata/--no-include-metadata", default=True, show_default=True)
@click.option("--include-latest-totals/--no-include-latest-totals", default=False, show_default=True)
@click.option("--delete-local", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--checksum-metadata", is_flag=True)
def cli(
    dataset: str,
    year: int,
    out_root: Path,
    totals_dir: Path,
    ensure_totals: bool,
    rescrape_totals: bool,
    max_files: int,
    id_workers: int,
    page_workers: int,
    totals_workers: int,
    totals_limit: int,
    totals_country: str,
    slice_str: Optional[str],
    resume: bool,
    verbose: bool,
    output: str,
    bucket: str,
    overwrite: bool,
    include_metadata: bool,
    include_latest_totals: bool,
    delete_local: bool,
    dry_run: bool,
    checksum_metadata: bool,
) -> None:
    """
    Runs:
      1) downloader -> writes manifest + outputs to out_root
      2) optional uploader -> pushes artifacts to S3 (idempotent)
    """

    manifest_path = download_year(
        dataset=dataset,
        year=year,
        out_root=out_root,
        totals_dir=totals_dir,
        ensure_totals=ensure_totals,
        rescrape_totals=rescrape_totals,
        max_files=max_files,
        id_workers=id_workers,
        page_workers=page_workers,
        totals_workers=totals_workers,
        totals_limit=totals_limit,
        totals_country=totals_country,
        slice_str=slice_str,
        resume=resume,
        verbose=verbose,
    )

    click.echo(f"[OK] Download complete. Manifest: {manifest_path}")

    if output.lower() != "s3":
        return

    # For upload script, call it as if from CLI (clean + keeps its click validations)
    # We'll pass bucket/out-root/totals-dir and flags.
    argv = [
        "upload_run_to_s3_full_files.py",
        "--bucket",
        bucket,
        "--out-root",
        str(out_root),
        "--totals-dir",
        str(totals_dir),
        "--include-metadata" if include_metadata else "--no-include-metadata",
        "--include-latest-totals" if include_latest_totals else "--no-include-latest-totals",
        "--overwrite" if overwrite else "--no-overwrite",
    ]
    if delete_local:
        argv.append("--delete-local")
    if dry_run:
        argv.append("--dry-run")
    if checksum_metadata:
        argv.append("--checksum-metadata")
    if verbose:
        argv.append("--verbose")

    # Click entrypoint expects sys.argv style
    sys.argv = argv
    upload_main(standalone_mode=True)

    click.echo("[OK] Upload complete.")


if __name__ == "__main__":
    cli()
