#!/usr/bin/env python3
"""
openpayments_companies_totals_by_year.py

Library-first rewrite of your original script, so it can be imported and used by
your downloader (and still runnable as a CLI if you want later).

What it does:
1) Fetch ALL company IDs via the datastore query (paginated).
2) For each company ID, call:
   https://openpaymentsdata.cms.gov/api/1/entities/companies/{id}
3) Extract summaryByAvailableYear[].generalTransactions per year:
   total_2018 .. total_2024 (or any years you request)
4) Save JSON atomically:
   openpayments_companies_totals_by_year.json (or whatever you pass)

Key additions vs original:
- `build_totals_json()` programmatic entrypoint
- optional `years=[...]` override
- atomic JSON write (tmp -> rename)
- returns path or (path, df) via `return_df`
- keeps all helper functions in one file (so imports work)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import requests


# ---------------------------
# CONFIG
# ---------------------------
FIRST_API_URL = (
    "https://openpaymentsdata.cms.gov/api/1/datastore/query/"
    "1cf0c6c0-c377-466e-b78f-037b442559f8/0"
)
COMPANY_API_BASE = "https://openpaymentsdata.cms.gov/api/1/entities/companies"


# ---------------------------
# LOGGING
# ---------------------------
def setup_logging(verbose: bool) -> None:
    """
    Lightweight console logging for CLI usage.
    If you import this module from another script, configure logging there instead.
    """
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if called multiple times
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)s | %(message)s",
        )


# ---------------------------
# PROGRESS BAR (no deps)
# ---------------------------
def render_progress(done: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return ""
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(ratio * width)
    bar = "█" * filled + "░" * (width - filled)
    pct = int(ratio * 100)
    return f"[{bar}] {pct:3d}% ({done}/{total})"


def print_progress(done: int, total: int) -> None:
    msg = render_progress(done, total)
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")


# ---------------------------
# HTTP SESSION
# ---------------------------
def requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "user-agent": "openpayments-companies-totals-by-year/1.0",
        }
    )
    return s


# ---------------------------
# STEP 1: FETCH ALL COMPANY IDS
# ---------------------------
def fetch_all_company_ids(
    session: requests.Session,
    limit: int = 100,
    country: str = "UNITED STATES",
    timeout: int = 60,
) -> List[str]:
    base_params = {
        "keys": "true",
        "limit": limit,
        "conditions[0][property]": "amgpo_making_payment_country",
        "conditions[0][value]": country,
        "conditions[0][operator]": "=",
        "sorts[0][property]": "amgpo_making_payment_name",
        "sorts[0][order]": "asc",
    }

    all_ids: List[str] = []
    offset = 0

    while True:
        params = dict(base_params)
        params["offset"] = offset

        r = session.get(FIRST_API_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        results = data.get("results", []) or []
        if not results:
            break

        batch_ids = [
            str(item["amgpo_making_payment_id"])
            for item in results
            if item.get("amgpo_making_payment_id") is not None
        ]
        all_ids.extend(batch_ids)

        logging.info("Fetched %d IDs (offset %d -> %d)", len(batch_ids), offset, offset + len(results) - 1)
        offset += limit

    # De-dupe while preserving order
    seen = set()
    deduped: List[str] = []
    for cid in all_ids:
        if cid not in seen:
            seen.add(cid)
            deduped.append(cid)

    logging.info("Total IDs found: %d", len(deduped))
    return deduped


# ---------------------------
# STEP 2: EXTRACT TOTALS BY YEAR
# ---------------------------
def extract_totals_by_year(payload: Any, years: List[int]) -> Dict[str, Optional[int]]:
    """
    From payload["summaryByAvailableYear"], map programYear -> generalTransactions
    into columns: total_YYYY

    Missing years become 0.
    """
    out: Dict[str, Optional[int]] = {f"total_{y}": 0 for y in years}

    if not isinstance(payload, dict):
        return out

    items = payload.get("summaryByAvailableYear")
    if not isinstance(items, list):
        return out

    for row in items:
        if not isinstance(row, dict):
            continue
        py = row.get("programYear")
        gt = row.get("generalTransactions")

        if py is None:
            continue
        try:
            py_int = int(str(py))
        except Exception:
            continue

        if py_int in years:
            try:
                out[f"total_{py_int}"] = int(gt) if gt is not None else 0
            except Exception:
                out[f"total_{py_int}"] = 0

    return out


def fetch_company_totals_by_year(
    session: requests.Session,
    company_id: str,
    years: List[int],
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 1.6,
) -> Tuple[str, Dict[str, Optional[int]], Optional[str]]:
    """
    Returns (company_id, totals_dict, error_message)
    """
    url = f"{COMPANY_API_BASE}/{company_id}"

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            totals = extract_totals_by_year(payload, years)
            return company_id, totals, None
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            if attempt >= max_retries:
                return company_id, {f"total_{y}": 0 for y in years}, msg
            time.sleep(backoff_base ** attempt)

    return company_id, {f"total_{y}": 0 for y in years}, "Unknown error"


# ---------------------------
# PROGRAMMATIC ENTRYPOINT
# ---------------------------
def build_totals_json(
    out_path: str = "openpayments_companies_totals_by_year.json",
    workers: int = 10,
    limit: int = 100,
    country: str = "UNITED STATES",
    min_year: int = 2018,
    max_year: int = 2024,
    years: Optional[Sequence[int]] = None,
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 1.6,
    verbose: bool = False,
    return_df: bool = False,
    show_progress: bool = True,
) -> Union[str, Tuple[str, pd.DataFrame]]:
    """
    Programmatic entrypoint so other scripts can generate totals before downloading.

    - If years is provided, it overrides min_year/max_year.
    - Writes JSON atomically (tmp then rename).
    - Returns out_path by default. If return_df=True returns (out_path, df).

    NOTE:
    - This function does NOT reset global logging handlers. If verbose=True, it only raises root level.
    - For CLI usage, call setup_logging(verbose) before calling this.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    year_list = list(years) if years is not None else list(range(min_year, max_year + 1))

    session = requests_session()

    # 1) Fetch IDs
    company_ids = fetch_all_company_ids(
        session=session,
        limit=limit,
        country=country,
        timeout=timeout,
    )
    if not company_ids:
        logging.warning("No company IDs returned. Exiting.")
        return (out_path, pd.DataFrame()) if return_df else out_path

    # 2) Fetch company totals concurrently
    total = len(company_ids)
    logging.info("Fetching totals for %d companies with %d workers...", total, workers)

    rows: List[Dict[str, Any]] = []
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(
                fetch_company_totals_by_year,
                session,
                cid,
                year_list,
                timeout,
                max_retries,
                backoff_base,
            ): cid
            for cid in company_ids
        }

        for fut in as_completed(futures):
            cid, totals_dict, err = fut.result()
            rows.append({"company_id": cid, **totals_dict, "error": err})

            done += 1
            if show_progress:
                print_progress(done, total)

    df = pd.DataFrame(rows).sort_values("company_id").reset_index(drop=True)

    # Atomic write
    out_path_p = Path(out_path)
    out_path_p.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = out_path_p.with_suffix(out_path_p.suffix + ".tmp")
    df.to_json(tmp_path, orient="records", indent=2)
    tmp_path.replace(out_path_p)

    ok = int(df["error"].isna().sum()) if "error" in df.columns else 0
    fail = int(df["error"].notna().sum()) if "error" in df.columns else 0
    logging.info("Saved %d rows to %s", len(df), out_path_p)
    logging.info("Success: %d | Failed: %d", ok, fail)

    if return_df:
        return str(out_path_p), df
    return str(out_path_p)


# ---------------------------
# OPTIONAL CLI (kept close to original behavior)
# ---------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Open Payments: company totals by year (generalTransactions).")
    parser.add_argument("--workers", type=int, default=10, help="Thread workers (default: 10)")
    parser.add_argument("--limit", type=int, default=100, help="Pagination limit for ID query (default: 100)")
    parser.add_argument("--country", default="UNITED STATES", help="Country filter (default: UNITED STATES)")
    parser.add_argument("--min-year", type=int, default=2018, help="Min year column (default: 2018)")
    parser.add_argument("--max-year", type=int, default=2024, help="Max year column (default: 2024)")
    parser.add_argument("--out", default="openpayments_companies_totals_by_year.json", help="Output JSON path")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds (default: 60)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per company request (default: 3)")
    parser.add_argument("--backoff-base", type=float, default=1.6, help="Backoff base (default: 1.6)")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar output")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = parser.parse_args()

    setup_logging(args.verbose)

    build_totals_json(
        out_path=args.out,
        workers=args.workers,
        limit=args.limit,
        country=args.country,
        min_year=args.min_year,
        max_year=args.max_year,
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff_base=args.backoff_base,
        verbose=args.verbose,
        return_df=False,
        show_progress=not args.no_progress,
    )


if __name__ == "__main__":
    main()
