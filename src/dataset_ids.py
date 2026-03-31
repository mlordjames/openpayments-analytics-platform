#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests


JS_URL = "https://openpaymentsdata.cms.gov/frontend/build/static/js/index.js?ta9low"

HEADERS = {
    "user-agent": "Mozilla/5.0",
    "accept": "*/*",
    "referer": "https://openpaymentsdata.cms.gov/",
}

CACHE_FILE = "dataset_ids_cache.json"
MAX_CACHE_DAYS = 7
RESOLVE_URL_TMPL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/{js_id}/0"


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_js(session: requests.Session) -> str:
    r = session.get(JS_URL, timeout=30)
    r.raise_for_status()
    return r.text


def extract_general_payment_js_ids(js_text: str) -> Dict[str, str]:
    """
    Returns year -> JS resolver id mapping.

    This directly matches the generalPayments object inside each PGYRYYYY block.
    It is safer than extracting a whole year block and searching inside it.
    """
    pattern = re.compile(
        r'PGYR(?P<year>\d{4})\s*:\s*\[\s*\{\s*type:"generalPayments",\s*id:"(?P<id>[^"]+)"',
        re.DOTALL,
    )

    results: Dict[str, str] = {}
    for m in pattern.finditer(js_text):
        results[m.group("year")] = m.group("id")

    if not results:
        raise RuntimeError("Failed to extract any generalPayments dataset ids from index.js")

    return results


def resolve_to_final_dataset_metadata(session: requests.Session, js_id: str) -> Dict[str, Any]:
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
        final_id = str(resources[0]["id"])
    except Exception as e:
        raise RuntimeError(
            f"Failed to resolve js_id={js_id}. Unexpected response shape. Error={e}"
        ) from e

    return {
        "js_id": str(js_id),
        "final_id": final_id,
        "count": data.get("count"),
        "schema": data.get("schema"),
        "query": data.get("query"),
        "resolved_at_utc": datetime.utcnow().isoformat() + "Z",
    }


def resolve_to_final_dataset_id(session: requests.Session, js_id: str) -> str:
    return resolve_to_final_dataset_metadata(session, js_id)["final_id"]


def get_general_payment_js_id_for_year(session: requests.Session, year: int | str) -> str:
    js_text = fetch_js(session)
    year_to_js_id = extract_general_payment_js_ids(js_text)
    year_str = str(int(year))

    if year_str not in year_to_js_id:
        raise ValueError(f"No generalPayments JS id found for year={year_str}")

    return year_to_js_id[year_str]


def get_final_general_payment_metadata(session: requests.Session) -> Dict[str, Dict[str, Any]]:
    """
    Resolve all years, but do NOT fail the whole refresh if one year breaks.
    Broken years are skipped with a warning.
    """
    js_text = fetch_js(session)
    year_to_js_id = extract_general_payment_js_ids(js_text)

    year_to_metadata: Dict[str, Dict[str, Any]] = {}

    for year, js_id in sorted(year_to_js_id.items()):
        try:
            year_to_metadata[year] = resolve_to_final_dataset_metadata(session, js_id)
        except Exception as e:
            logging.warning("Skipping year=%s js_id=%s due to resolution error: %s", year, js_id, e)

    if not year_to_metadata:
        raise RuntimeError("Failed to resolve metadata for all available years.")

    return year_to_metadata


def _is_cache_fresh(cache_path: Path) -> bool:
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=MAX_CACHE_DAYS)


def _schema_cache_path(cache_dir: Path, year: str) -> Path:
    return cache_dir / "schemas" / "general-payments" / f"year={year}" / "schema.json"


def save_schema_cache(cache_dir: Path | str, year: str | int, payload: Dict[str, Any]) -> Path:
    cache_dir = Path(cache_dir)
    year_str = str(int(year))
    out_path = _schema_cache_path(cache_dir, year_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def load_schema_cache(cache_dir: Path | str, year: str | int) -> Optional[Dict[str, Any]]:
    cache_dir = Path(cache_dir)
    path = _schema_cache_path(cache_dir, str(int(year)))
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def refresh_general_payment_metadata(
    cache_dir: Path | str = ".",
) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
    """
    Refresh cache for all resolvable years.

    Important:
    - if one year fails, it is skipped
    - successful years are still written to id cache + schema cache
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / CACHE_FILE

    session = _get_session()
    year_to_metadata = get_final_general_payment_metadata(session)
    year_to_id = {year: payload["final_id"] for year, payload in year_to_metadata.items()}

    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(year_to_id, f, indent=2)

    for year, payload in year_to_metadata.items():
        save_schema_cache(cache_dir, year, payload)

    return year_to_id, year_to_metadata


def getdatasetids(cache_dir: Path | str = ".") -> Dict[str, str]:
    """
    Returns YEAR -> FINAL dataset_id mapping (download-ready).
    Uses local cache if fresh, otherwise refreshes all resolvable years.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / CACHE_FILE

    if _is_cache_fresh(cache_path):
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    ids, _ = refresh_general_payment_metadata(cache_dir)
    return ids


def getdatasetid_and_schema(
    year: int,
    cache_dir: Path | str = ".",
) -> Tuple[str, Optional[Dict[str, Any]], Optional[Path]]:
    """
    Resolve only the requested year if needed.

    This avoids crashing a 2023 run because 2025 is broken.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    year_str = str(int(year))
    cache_path = cache_dir / CACHE_FILE

    ids: Dict[str, str] = {}
    if _is_cache_fresh(cache_path):
        try:
            ids = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            ids = {}

    schema_payload = load_schema_cache(cache_dir, year_str)
    schema_path = _schema_cache_path(cache_dir, year_str)

    if year_str in ids and schema_payload is not None and schema_path.exists():
        return ids[year_str], schema_payload, schema_path

    session = _get_session()
    js_id = get_general_payment_js_id_for_year(session, year_str)
    metadata = resolve_to_final_dataset_metadata(session, js_id)

    if cache_path.exists():
        try:
            ids = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            ids = {}

    ids[year_str] = metadata["final_id"]
    cache_path.write_text(json.dumps(ids, indent=2), encoding="utf-8")

    save_schema_cache(cache_dir, year_str, metadata)
    schema_payload = metadata
    schema_path = _schema_cache_path(cache_dir, year_str)

    return metadata["final_id"], schema_payload, schema_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    ids, metadata = refresh_general_payment_metadata()
    print(json.dumps(ids, indent=2))
    print(f"Saved schema files for years: {', '.join(sorted(metadata.keys()))}")