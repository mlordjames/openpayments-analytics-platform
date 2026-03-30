#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src import records_total, dataset_ids


# -----------------------------
# Repo-aware defaults (/src)
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = REPO_ROOT / "data" / "out"
DEFAULT_TOTALS_DIR = REPO_ROOT / "data" / "totals"
DEFAULT_METADATA_CACHE = REPO_ROOT / "metadata"

# -----------------------------
# CONFIG
# -----------------------------
DATASET_CHOICES = ["general-payments"]


PAGE_LIMIT = 5000
CONNECT_TIMEOUT = 60
READ_TIMEOUT = 180

MAX_RETRIES = 4
BACKOFF_FACTOR = 0.8
MANUAL_ATTEMPTS = 5
MANUAL_BACKOFF_BASE = 2.0

EXPECTED_MIN_COLS = 80
MAX_VALIDATION_BYTES = 512 * 1024

DEFAULT_HEADERS = {
    "Referer": "https://openpaymentsdata.cms.gov/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8",
}

TOTALS_PREFIX = "openpayments_companies_totals_by_year"
TOTALS_DATE_FMT = "%d-%m-%Y"
TOTALS_MAX_AGE_DAYS = 10

# Avoid nested thread explosion
MAX_PAGE_WORKERS_CAP = 5

_thread_local = threading.local()


# -----------------------------
# Utilities
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, payload: Dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def setup_logging(out_root: Path, run_id: str, verbose: bool, airflow_mode: bool = False) -> Path:
    """Configure logging.

    - Always writes a run-scoped log file under out_root/metadata/logs
    - In Airflow mode, avoids clearing existing handlers (Airflow manages task logging).
    """
    logs_dir = out_root / "metadata" / "logs"
    ensure_dir(logs_dir)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"openpayments_download_runid={run_id}_{ts}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    if not airflow_mode:
        logger.handlers.clear()
        logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    logging.info("Logging to: %s", log_path.resolve())
    return log_path


# -----------------------------
# Totals cache management
# -----------------------------
@dataclass
class TotalsFile:
    path: Path
    date: datetime


def _parse_totals_date_from_name(name: str) -> Optional[datetime]:
    m = re.search(rf"{re.escape(TOTALS_PREFIX)}_(\d{{2}}-\d{{2}}-\d{{4}})\.json$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), TOTALS_DATE_FMT)
    except Exception:
        return None


def find_latest_totals_file(totals_dir: Path) -> Optional[TotalsFile]:
    if not totals_dir.exists():
        return None

    candidates: List[TotalsFile] = []
    for p in totals_dir.glob(f"{TOTALS_PREFIX}_*.json"):
        dt = _parse_totals_date_from_name(p.name)
        if dt:
            candidates.append(TotalsFile(path=p, date=dt))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x.date, reverse=True)
    return candidates[0]


def is_totals_fresh(totals_file: TotalsFile, max_age_days: int = TOTALS_MAX_AGE_DAYS) -> bool:
    return (datetime.now() - totals_file.date) <= timedelta(days=max_age_days)


def totals_file_for_today(totals_dir: Path) -> Path:
    ensure_dir(totals_dir)
    today = datetime.now().strftime(TOTALS_DATE_FMT)
    return totals_dir / f"{TOTALS_PREFIX}_{today}.json"


def load_company_totals_json(path: Path) -> pd.DataFrame:
    df = pd.read_json(path)
    df["company_id"] = df["company_id"].astype(str)
    return df


def save_totals_df(df: pd.DataFrame, out_path: Path) -> None:
    if "company_id" in df.columns:
        df = df.sort_values("company_id").reset_index(drop=True)
    df.to_json(out_path, orient="records", indent=2)
    logging.info("Totals JSON saved: %s", out_path.resolve())


def ensure_totals_for_year(
    year: int,
    totals_dir: Path,
    *,
    rescrape: bool,
    totals_workers: int,
    totals_limit: int,
    totals_country: str,
    verbose: bool,
    show_progress: bool = True,
) -> Path:
    ensure_dir(totals_dir)
    latest = find_latest_totals_file(totals_dir)

    if latest and is_totals_fresh(latest) and (not rescrape):
        df_old = load_company_totals_json(latest.path)
        col = f"total_{year}"
        if col in df_old.columns:
            logging.info("Using fresh totals file: %s", latest.path.name)
            return latest.path

        logging.info("Fresh totals missing %s. Scraping year and merging...", col)
        tmp_new = totals_dir / f"_tmp_totals_{year}_{int(time.time())}.json"

        records_total.build_totals_json(
            out_path=str(tmp_new),
            workers=totals_workers,
            limit=totals_limit,
            country=totals_country,
            min_year=year,
            max_year=year,
            verbose=verbose,
            show_progress=show_progress,
        )

        df_new = load_company_totals_json(tmp_new)
        keep_cols = ["company_id"]
        if col in df_new.columns:
            keep_cols.append(col)
        if "error" in df_new.columns:
            keep_cols.append("error")

        df_new = df_new[keep_cols].copy()
        df_merged = df_old.merge(df_new, on="company_id", how="outer", suffixes=("", "_new"))

        if "error_new" in df_merged.columns:
            if "error" in df_merged.columns:
                df_merged["error"] = df_merged["error"].fillna(df_merged["error_new"])
                df_merged.drop(columns=["error_new"], inplace=True)
            else:
                df_merged.rename(columns={"error_new": "error"}, inplace=True)

        save_totals_df(df_merged, latest.path)

        try:
            tmp_new.unlink(missing_ok=True)
        except Exception:
            pass

        return latest.path

    out_path = totals_file_for_today(totals_dir)
    logging.info("Creating totals file: %s", out_path.name)

    records_total.build_totals_json(
        out_path=str(out_path),
        workers=totals_workers,
        limit=totals_limit,
        country=totals_country,
        min_year=year,
        max_year=year,
        verbose=verbose,
        show_progress=show_progress,
    )
    return out_path


# -----------------------------
# HTTP helpers (thread-local sessions)
# -----------------------------
def make_session(pool_size: int) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update(DEFAULT_HEADERS)
    return sess


def get_thread_session(pool_size: int) -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = make_session(pool_size=pool_size)
        _thread_local.session = sess
    return sess


def truncate_trailing_partial_row(path: Path) -> None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return
        with path.open("rb+") as fh:
            fh.seek(-1, os.SEEK_END)
            if fh.read(1) == b"\n":
                return
            block = 131072
            pos = fh.tell()
            buffer = b""
            last_nl = -1
            while pos > 0:
                readpos = max(0, pos - block)
                fh.seek(readpos)
                chunk = fh.read(pos - readpos)
                buffer = chunk + buffer
                idx = buffer.rfind(b"\n")
                if idx != -1:
                    last_nl = readpos + idx
                    break
                pos = readpos
            if last_nl == -1:
                fh.truncate(0)
                return
            fh.truncate(last_nl + 1)
    except Exception:
        return


def validate_header_min_cols(path: Path, expected_min_cols: int) -> Tuple[bool, str]:
    try:
        sample = path.read_bytes()[:MAX_VALIDATION_BYTES]
        header_line = None
        for enc in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                text = sample.decode(enc, errors="replace")
                for line in text.splitlines():
                    if line.strip():
                        header_line = line
                        break
                if header_line:
                    break
            except Exception:
                continue

        if not header_line:
            return (False, "empty_or_binary_file")

        cols = next(csv.reader([header_line]))
        if len(cols) < expected_min_cols:
            return (False, f"too_few_columns({len(cols)}<{expected_min_cols})")
        return (True, "")
    except Exception as e:
        return (False, f"validation_error:{e}")


def build_params(company_id: str, offset: int) -> dict:
    return {
        "conditions[0][property]": "applicable_manufacturer_or_applicable_gpo_making_payment_id",
        "conditions[0][operator]": "=",
        "conditions[0][value]": company_id,
        "format": "csv",
        "limit": PAGE_LIMIT,
        "offset": offset,
    }


def request_with_manual_backoff(session: requests.Session, url: str, params: dict) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MANUAL_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(MANUAL_BACKOFF_BASE * (attempt**1.3))
            continue

        if resp.status_code in (403, 429):
            time.sleep(MANUAL_BACKOFF_BASE * (attempt**1.6))
            continue

        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_manual_backoff exhausted without response")


# -----------------------------
# Paths + manifest
# -----------------------------
def get_year_dir(out_root: Path, dataset: str, year: int) -> Path:
    return out_root / f"dataset={dataset}" / f"year={year}"


def get_run_dir(out_root: Path, run_id: str) -> Path:
    return out_root / "metadata" / "runs" / f"run_id={run_id}"


def build_run_id(dataset: str, year: int, dataset_id: str, totals_path: Path) -> str:
    payload = {
        "dataset": dataset,
        "year": year,
        "dataset_id": dataset_id,
        "totals_file": totals_path.name,
        "totals_mtime": int(totals_path.stat().st_mtime),
    }
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]


# -----------------------------
# Download routines
# -----------------------------
def download_small_sequential(
    *,
    session: requests.Session,
    url: str,
    company_id: str,
    year: int,
    year_dir: Path,
    resume: bool,
) -> Tuple[str, int, bool, str, Optional[Dict]]:
    final_path = year_dir / f"company_id={company_id}.csv"

    # Resume behavior: skip if already valid
    if resume and final_path.exists() and final_path.stat().st_size > 0:
        ok, reason = validate_header_min_cols(final_path, EXPECTED_MIN_COLS)
        if ok:
            audit = {
                "company_id": company_id,
                "year": year,
                "path": str(final_path),
                "bytes": int(final_path.stat().st_size),
                "sha256": sha256_file(final_path),
                "mode": "resume_skip_existing",
            }
            return (company_id, year, True, "skipped_existing_valid", audit)

    tmp_path = final_path.with_suffix(".csv.part")
    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except Exception:
            pass

    start = time.perf_counter()
    offset = 0
    total_data_rows = 0
    wrote_header = False

    while True:
        resp = request_with_manual_backoff(session, url, build_params(company_id, offset))
        if resp.status_code != 200:
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return (company_id, year, False, f"HTTP {resp.status_code} at offset {offset}", None)

        mode = "w" if offset == 0 else "a"
        enc = "utf-8-sig" if offset == 0 else "utf-8"

        header_seen = False
        data_lines = 0

        with tmp_path.open(mode, encoding=enc, newline="") as fh:
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if not header_seen:
                    header_seen = True
                    if offset == 0:
                        fh.write(line + "\n")
                        wrote_header = True
                    continue
                fh.write(line + "\n")
                data_lines += 1

        truncate_trailing_partial_row(tmp_path)

        if data_lines == 0:
            break

        total_data_rows += data_lines
        offset += PAGE_LIMIT
        time.sleep(0.1)

    if not wrote_header or total_data_rows == 0:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, year, False, "no_results_header_only", None)

    ok, reason = validate_header_min_cols(tmp_path, EXPECTED_MIN_COLS)
    if not ok:
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, year, False, f"validation_failed:{reason}", None)

    tmp_path.replace(final_path)

    elapsed = time.perf_counter() - start
    audit = {
        "company_id": company_id,
        "year": year,
        "path": str(final_path),
        "rows_estimate": int(total_data_rows),
        "bytes": int(final_path.stat().st_size),
        "sha256": sha256_file(final_path),
        "elapsed_seconds": round(elapsed, 4),
        "mode": "small_sequential",
    }
    return (company_id, year, True, f"downloaded_rows~{total_data_rows}", audit)


def merge_parts(parts: List[Path], final_path: Path) -> Tuple[bool, str]:
    try:
        if final_path.exists():
            final_path.unlink()
        ensure_dir(final_path.parent)
        with final_path.open("wb") as out:
            for p in parts:
                if p.exists() and p.stat().st_size > 0:
                    out.write(p.read_bytes())

        ok, reason = validate_header_min_cols(final_path, EXPECTED_MIN_COLS)
        if not ok:
            try:
                final_path.unlink()
            except Exception:
                pass
            return (False, f"validation_failed:{reason}")

        return (True, "merged_ok")
    except Exception as e:
        return (False, f"merge_error:{e}")


def fetch_page_to_partfile(
    *,
    session: requests.Session,
    url: str,
    company_id: str,
    offset: int,
    part_path: Path,
    is_first: bool,
) -> Tuple[bool, str, int]:
    resp = request_with_manual_backoff(session, url, build_params(company_id, offset))
    if resp.status_code != 200:
        return (False, f"HTTP {resp.status_code}", 0)

    if part_path.exists():
        part_path.unlink()

    enc = "utf-8-sig" if is_first else "utf-8"
    first_nonempty_seen = False
    data_lines = 0

    with part_path.open("w", encoding=enc, newline="") as fh:
        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue
            if not first_nonempty_seen:
                first_nonempty_seen = True
                if is_first:
                    fh.write(line + "\n")
                continue
            fh.write(line + "\n")
            data_lines += 1

    truncate_trailing_partial_row(part_path)

    if data_lines == 0:
        try:
            part_path.unlink()
        except Exception:
            pass
        return (True, "empty", 0)

    return (True, "ok", data_lines)


def download_big_parallel_pages(
    *,
    session: requests.Session,
    url: str,
    company_id: str,
    expected_total: int,
    year: int,
    year_dir: Path,
    page_workers: int,
    resume: bool,
) -> Tuple[str, int, bool, str, Optional[Dict]]:
    final_path = year_dir / f"company_id={company_id}.csv"

    if resume and final_path.exists() and final_path.stat().st_size > 0:
        ok, reason = validate_header_min_cols(final_path, EXPECTED_MIN_COLS)
        if ok:
            audit = {
                "company_id": company_id,
                "year": year,
                "path": str(final_path),
                "bytes": int(final_path.stat().st_size),
                "sha256": sha256_file(final_path),
                "mode": "resume_skip_existing",
            }
            return (company_id, year, True, "skipped_existing_valid", audit)

    start = time.perf_counter()

    pages = int(math.ceil(expected_total / PAGE_LIMIT))
    if pages <= 0:
        return (company_id, year, False, "bad_expected_total", None)

    parts_root = year_dir / "_parts" / company_id
    ensure_dir(parts_root)

    part_paths = [parts_root / f"part_{i:06d}.csv" for i in range(pages)]
    offsets = [i * PAGE_LIMIT for i in range(pages)]

    total_data_rows = 0
    with ThreadPoolExecutor(max_workers=page_workers) as ex:
        futs = {
            ex.submit(
                fetch_page_to_partfile,
                session=session,
                url=url,
                company_id=company_id,
                offset=offsets[i],
                part_path=part_paths[i],
                is_first=(i == 0),
            ): i
            for i in range(pages)
        }
        for fut in as_completed(futs):
            ok, msg, data_lines = fut.result()
            if not ok:
                return (company_id, year, False, f"page_failed:{msg}", None)
            total_data_rows += data_lines

    if total_data_rows == 0:
        return (company_id, year, False, "no_results_all_pages_empty", None)

    ok, msg = merge_parts(part_paths, final_path)
    if not ok:
        return (company_id, year, False, msg, None)

    elapsed = time.perf_counter() - start
    audit = {
        "company_id": company_id,
        "year": year,
        "path": str(final_path),
        "rows_estimate": int(total_data_rows),
        "bytes": int(final_path.stat().st_size),
        "sha256": sha256_file(final_path),
        "elapsed_seconds": round(elapsed, 4),
        "mode": "big_parallel_pages",
        "pages": int(pages),
    }
    return (company_id, year, True, f"downloaded_pages={pages}_rows~{total_data_rows}", audit)


def _parse_slice(slice_str: Optional[str]) -> Optional[slice]:
    if not slice_str:
        return None
    s = slice_str.strip()
    if ":" not in s:
        n = int(s)
        return slice(0, n)

    left, right = s.split(":", 1)
    start = int(left) if left.strip() != "" else None
    right = right.strip()
    end = None if right == "" else (None if int(right) == -1 else int(right))
    return slice(start, end)


def resolve_dataset_id(dataset: str, year: int, cache_dir: Path = DEFAULT_METADATA_CACHE) -> str:
    """Resolve the datastore dataset_id for a given dataset/year.

    Airflow note:
      - This function must be called at task-runtime only (no network calls at module import).
    """
    if dataset != "general-payments":
        raise ValueError(f"Unsupported dataset '{dataset}'. Supported: {DATASET_CHOICES}")

    mapping = dataset_ids.getdatasetids(cache_dir=str(cache_dir))
    key_str = str(int(year))
    if key_str in mapping:
        return mapping[key_str]
    if int(year) in mapping:  # type: ignore[operator]
        return mapping[int(year)]  # type: ignore[index]
    supported = sorted(int(k) for k in mapping.keys())
    raise ValueError(f"Unsupported year {year}. Supported: {supported}")


def download_year(
    *,
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
    run_id: Optional[str] = None,
    airflow_mode: bool = False,
    no_progress: bool = False,
) -> Path:
    page_workers = min(int(page_workers), MAX_PAGE_WORKERS_CAP)

    dataset_id = resolve_dataset_id(dataset, year)
    base_url = f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/download"

    totals_path: Optional[Path] = None
    if ensure_totals:
        totals_path = ensure_totals_for_year(
            year=year,
            totals_dir=totals_dir,
            rescrape=rescrape_totals,
            totals_workers=totals_workers,
            totals_limit=totals_limit,
            totals_country=totals_country,
            verbose=verbose,
            show_progress=(not airflow_mode and not no_progress),
        )
    else:
        latest = find_latest_totals_file(totals_dir)
        if latest and latest.path.exists():
            totals_path = latest.path

    if not totals_path or not totals_path.exists():
        raise FileNotFoundError(f"No totals JSON available in {totals_dir}")

    run_id = run_id or build_run_id(dataset=dataset, year=year, dataset_id=dataset_id, totals_path=totals_path)
    setup_logging(out_root=out_root, run_id=run_id, verbose=verbose, airflow_mode=airflow_mode)

    started_at = utc_now_iso()
    run_start_perf = time.perf_counter()

    run_dir = get_run_dir(out_root, run_id)
    ensure_dir(run_dir)
    manifest_path = run_dir / "manifest.json"

    # Write an initial "running" manifest (useful for crash recovery)
    write_json(
        manifest_path,
        {
            "run_id": run_id,
            "dataset": dataset,
            "year": year,
            "dataset_id": dataset_id,
            "status": "running",
            "started_at_utc": started_at,
            "totals_file": str(totals_path),
        },
    )

    df = load_company_totals_json(totals_path)
    total_col = f"total_{year}"
    if total_col not in df.columns:
        raise SystemExit(f"Totals JSON missing required column: {total_col}")

    temp = df[["company_id", total_col]].copy()
    temp[total_col] = temp[total_col].fillna(0).astype(int)
    temp = temp[temp[total_col] > 0].sort_values("company_id")

    tasks: List[Tuple[str, int]] = [(r["company_id"], int(r[total_col])) for _, r in temp.iterrows()]

    if slice_str:
        sl = _parse_slice(slice_str)
        tasks = tasks[sl] if sl is not None else tasks
        logging.info("Applied slice=%s -> tasks=%d", slice_str, len(tasks))

    if max_files and max_files > 0:
        tasks = tasks[:max_files]
        logging.info("Applied max_files=%d -> tasks=%d", max_files, len(tasks))

    year_dir = get_year_dir(out_root, dataset, year)
    ensure_dir(year_dir)

    pool_size = max(id_workers, page_workers) * 3

    results_rows: List[Dict] = []
    audits_ok: List[Dict] = []

    def _job(company_id: str, expected_total: int):
        session = get_thread_session(pool_size=pool_size)
        if expected_total <= PAGE_LIMIT:
            return download_small_sequential(
                session=session, url=base_url, company_id=company_id, year=year, year_dir=year_dir, resume=resume
            )
        return download_big_parallel_pages(
            session=session,
            url=base_url,
            company_id=company_id,
            expected_total=expected_total,
            year=year,
            year_dir=year_dir,
            page_workers=page_workers,
            resume=resume,
        )

    with ThreadPoolExecutor(max_workers=id_workers) as ex:
        futs = {ex.submit(_job, cid, exp): (cid, exp) for cid, exp in tasks}

        for fut in tqdm(as_completed(futs), total=len(futs), desc=f"Downloading ({dataset}:{year})", unit="job", disable=(airflow_mode or no_progress)):
            cid, expected_total = futs[fut]
            try:
                company_id, y, ok, msg, audit = fut.result()
                row = {"company_id": company_id, "year": y, "expected_total": expected_total, "ok": bool(ok), "message": msg}
                if audit:
                    row.update(
                        {
                            "bytes": audit.get("bytes"),
                            "sha256": audit.get("sha256"),
                            "rows_estimate": audit.get("rows_estimate"),
                            "elapsed_seconds": audit.get("elapsed_seconds"),
                            "mode": audit.get("mode"),
                            "pages": audit.get("pages"),
                            "path": audit.get("path"),
                        }
                    )
                    audits_ok.append(audit)
                results_rows.append(row)
            except Exception as e:
                results_rows.append({"company_id": cid, "year": year, "expected_total": expected_total, "ok": False, "message": f"executor_error:{e}"})

    report_path = run_dir / f"download_report_{dataset}_{year}.csv"
    pd.DataFrame(results_rows).to_csv(report_path, index=False)

    audits_path = run_dir / f"file_audits_{dataset}_{year}.jsonl"
    with audits_path.open("w", encoding="utf-8") as f:
        for a in audits_ok:
            f.write(json.dumps(a, sort_keys=True) + "\n")

    ok_count = sum(1 for r in results_rows if r.get("ok") is True)
    fail_count = len(results_rows) - ok_count

    finished_at = utc_now_iso()
    elapsed_total = time.perf_counter() - run_start_perf

    final_manifest = {
        "run_id": run_id,
        "dataset": dataset,
        "year": year,
        "dataset_id": dataset_id,
        "status": "completed" if fail_count == 0 else "completed_with_failures",
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "elapsed_seconds": round(elapsed_total, 4),
        "tasks_total": int(len(results_rows)),
        "tasks_ok": int(ok_count),
        "tasks_failed": int(fail_count),
        "totals_file": str(totals_path),
        "year_dir": str(year_dir),
        "report_csv": str(report_path),
        "audits_jsonl": str(audits_path),
        "notes": {"page_workers_capped_to": MAX_PAGE_WORKERS_CAP, "expected_min_cols": EXPECTED_MIN_COLS, "page_limit": PAGE_LIMIT},
    }
    write_json(manifest_path, final_manifest)

    logging.info("Manifest saved: %s", manifest_path.resolve())
    return manifest_path


# -----------------------------
# CLI
# -----------------------------
@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--dataset", type=click.Choice(DATASET_CHOICES, case_sensitive=False), required=True)
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
@click.option("--resume/--no-resume", default=True, show_default=True, help="Skip already-downloaded valid files.")
@click.option("--verbose", is_flag=True)
@click.option("--run-id", type=str, default=None, help="Optional override for run_id (e.g., Airflow run_id).")
@click.option("--airflow-mode", is_flag=True, help="Adjust logging/behavior for execution under Airflow.")
@click.option("--no-progress", is_flag=True, help="Disable tqdm/progress output (recommended for Airflow logs).")
def cli(**kwargs) -> None:
    manifest_path = download_year(**kwargs)
    click.echo(str(manifest_path.resolve()))


if __name__ == "__main__":
    cli()