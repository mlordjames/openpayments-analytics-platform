#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import logging
import mimetypes
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import boto3
import click
from botocore.exceptions import ClientError

# -----------------------------
# Repo-aware defaults (/scripts)
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = REPO_ROOT / "data" / "out"
DEFAULT_TOTALS_DIR = REPO_ROOT / "data" / "totals"

DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"
TOTALS_PREFIX = "openpayments_companies_totals_by_year"


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def should_skip(p: Path) -> bool:
    if p.name.endswith(".part"):
        return True
    if "_parts" in p.parts:
        return True
    return False


def guess_content_type(path: Path) -> Optional[str]:
    if path.suffix.lower() == ".csv":
        return "text/csv"
    if path.suffix.lower() in (".json", ".jsonl"):
        return "application/json"
    ct, _ = mimetypes.guess_type(str(path))
    return ct


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def find_latest_totals_file(totals_dir: Path) -> Optional[Path]:
    if not totals_dir.exists():
        return None
    candidates = sorted(
        totals_dir.glob(f"{TOTALS_PREFIX}_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def upload_file(
    s3_client,
    bucket: str,
    local_path: Path,
    key: str,
    *,
    add_checksum_metadata: bool,
    overwrite: bool,
) -> str:
    if (not overwrite) and s3_object_exists(s3_client, bucket, key):
        return "skipped_exists"

    extra_args = {}
    ct = guess_content_type(local_path)
    if ct:
        extra_args["ContentType"] = ct

    if add_checksum_metadata:
        extra_args.setdefault("Metadata", {})
        extra_args["Metadata"]["sha256"] = sha256_file(local_path)

    if extra_args:
        s3_client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args)
    else:
        s3_client.upload_file(str(local_path), bucket, key)

    return "uploaded"


def build_raw_key(dataset_dir: Path, file_path: Path) -> str:
    rel = file_path.relative_to(dataset_dir).as_posix()
    return f"raw/{dataset_dir.name}/{rel}"


def build_metadata_key(metadata_root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(metadata_root).as_posix()
    return f"metadata/{rel}"


@dataclass
class UploadResult:
    local_path: Path
    s3_key: str
    ok: bool
    message: str


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--bucket", default=DEFAULT_BUCKET, show_default=True)
@click.option("--out-root", type=click.Path(path_type=Path), default=DEFAULT_OUT_ROOT, show_default=True)
@click.option("--include-metadata/--no-include-metadata", default=True, show_default=True)
@click.option("--include-latest-totals/--no-include-latest-totals", default=False, show_default=True)
@click.option("--totals-dir", type=click.Path(path_type=Path), default=DEFAULT_TOTALS_DIR, show_default=True)
@click.option("--overwrite/--no-overwrite", default=False, show_default=True, help="If false, skip objects that already exist in S3.")
@click.option("--delete-local", is_flag=True)
@click.option("--dry-run", is_flag=True)
@click.option("--checksum-metadata", is_flag=True)
@click.option("--verbose", is_flag=True)
def main(
    bucket: str,
    out_root: Path,
    include_metadata: bool,
    include_latest_totals: bool,
    totals_dir: Path,
    overwrite: bool,
    delete_local: bool,
    dry_run: bool,
    checksum_metadata: bool,
    verbose: bool,
) -> None:
    setup_logging(verbose)

    out_root = out_root.resolve()
    totals_dir = totals_dir.resolve()

    if not out_root.exists():
        raise SystemExit(f"out_root not found: {out_root}")

    s3 = boto3.client("s3")

    uploads: list[Tuple[Path, str]] = []

    # RAW dataset folders
    dataset_dirs = sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("dataset=")])
    for ddir in dataset_dirs:
        for f in iter_files(ddir):
            if should_skip(f):
                continue
            uploads.append((f, build_raw_key(ddir, f)))

    # METADATA
    metadata_root = out_root / "metadata"
    if include_metadata and metadata_root.is_dir():
        for f in iter_files(metadata_root):
            if should_skip(f):
                continue
            uploads.append((f, build_metadata_key(metadata_root, f)))

    # Latest totals
    if include_latest_totals:
        latest = find_latest_totals_file(totals_dir)
        if latest:
            uploads.append((latest, f"metadata/totals/{latest.name}"))

    if not uploads:
        logging.warning("Nothing to upload.")
        return

    logging.info("Bucket: %s", bucket)
    logging.info("Planned uploads: %d", len(uploads))

    results: list[UploadResult] = []

    for local_path, key in uploads:
        s3_uri = f"s3://{bucket}/{key}"

        if dry_run:
            logging.info("[DRY RUN] %s -> %s", local_path, s3_uri)
            results.append(UploadResult(local_path, key, True, "dry_run"))
            continue

        try:
            action = upload_file(
                s3,
                bucket,
                local_path,
                key,
                add_checksum_metadata=checksum_metadata,
                overwrite=overwrite,
            )
            results.append(UploadResult(local_path, key, True, action))
            logging.info("[OK] (%s) %s -> %s", action, local_path, s3_uri)

            if delete_local and action == "uploaded":
                local_path.unlink()
                logging.info("[OK] deleted local: %s", local_path)

        except Exception as e:
            results.append(UploadResult(local_path, key, False, f"error:{e}"))
            logging.error("[FAIL] %s -> %s", local_path, s3_uri)
            logging.exception(e)

    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count
    logging.info("Upload summary: ok=%d failed=%d", ok_count, fail_count)

    if fail_count:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
