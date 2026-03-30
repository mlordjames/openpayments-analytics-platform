#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import requests


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
JS_URL = "https://openpaymentsdata.cms.gov/frontend/build/static/js/index.js?ta9low"

HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept": "*/*",
    "referer": "https://openpaymentsdata.cms.gov/",
}

CACHE_FILE = "dataset_ids_cache.json"
MAX_CACHE_DAYS = 7

# The resolver endpoint you described
RESOLVE_URL_TMPL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/{js_id}/0"


# -------------------------------------------------------------------
# HTTP
# -------------------------------------------------------------------
def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_js(session: requests.Session) -> str:
    r = session.get(JS_URL, timeout=30)
    r.raise_for_status()
    return r.text


# -------------------------------------------------------------------
# STEP 1: extract JS ids per year (not final download ids)
# -------------------------------------------------------------------
def extract_general_payment_js_ids(js_text: str) -> Dict[str, str]:
    """
    Returns: {"2024": "<js_id>", "2023": "<js_id>", ...}
    These IDs are NOT the final datastore distribution IDs.
    """
    results: Dict[str, str] = {}

    # Match each PGYRYYYY block
    year_blocks = re.findall(r"PGYR(\d{4}):\[(.*?)\]", js_text, re.DOTALL)

    for year, block in year_blocks:
        match = re.search(r'type:"generalPayments",id:"([^"]+)"', block)
        if match:
            results[year] = match.group(1)

    return results


# -------------------------------------------------------------------
# STEP 2: resolve JS id -> FINAL datastore query id
# -------------------------------------------------------------------
def resolve_to_final_dataset_id(session: requests.Session, js_id: str) -> str:
    """
    Given the ID scraped from index.js (js_id),
    call /api/1/datastore/query/{js_id}/0?results=false&count=true&schema=true
    then take response.json()["query"]["resources"][0]["id"]

    Returns final_id (string)
    Raises RuntimeError on unexpected response shape.
    """
    params = {
        "results": "false",
        "count": "true",
        "schema": "true",
    }
    url = RESOLVE_URL_TMPL.format(js_id=js_id)
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()

    try:
        resources = data["query"]["resources"]
        if not resources or "id" not in resources[0]:
            raise KeyError("resources[0].id missing")
        final_id = resources[0]["id"]
    except Exception as e:
        raise RuntimeError(
            f"Failed to resolve js_id={js_id}. Unexpected response shape. Error={e}"
        ) from e

    return str(final_id)


def get_final_general_payment_ids(session: requests.Session) -> Dict[str, str]:
    """
    Returns: {"2024": "<final_id>", "2023": "<final_id>", ...}
    Final IDs are what you can use in:
      /api/1/datastore/query/<final_id>/download
    """
    js_text = fetch_js(session)
    year_to_js_id = extract_general_payment_js_ids(js_text)

    year_to_final_id: Dict[str, str] = {}
    for year, js_id in year_to_js_id.items():
        final_id = resolve_to_final_dataset_id(session, js_id)
        year_to_final_id[year] = final_id

    return year_to_final_id


# -------------------------------------------------------------------
# CACHING
# -------------------------------------------------------------------
def _is_cache_fresh(cache_path: Path) -> bool:
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=MAX_CACHE_DAYS)


def getdatasetids(cache_dir: Path | str = ".") -> Dict[str, str]:
    """
    Returns YEAR -> FINAL dataset_id mapping (download-ready).
    Uses local cache if fresh, otherwise fetches, resolves, and updates cache.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / CACHE_FILE

    # Use cache if fresh
    if _is_cache_fresh(cache_path):
        with cache_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    session = _get_session()

    # Fetch & resolve
    ids = get_final_general_payment_ids(session)

    # Save cache
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(ids, f, indent=2)

    return ids


if __name__ == "__main__":
    # Prints final ids that should work with /download endpoint
    print(json.dumps(getdatasetids(), indent=2))
