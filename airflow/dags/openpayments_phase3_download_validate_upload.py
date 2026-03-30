from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import Param, get_current_context, task
from docker.types import Mount

# -------------------------------------------------------------------
# HOST PATHS (EC2 host filesystem)
# -------------------------------------------------------------------
# IMPORTANT:
# - HOST_REPO_DIR must be the ABSOLUTE path to the repo on the EC2 host
# - DockerOperator bind mounts use HOST paths, not container paths
HOST_REPO_DIR = os.environ.get("HOST_REPO_DIR", "/opt/openpayments-analytics-platform")
HOST_DATA_DIR = os.path.join(HOST_REPO_DIR, "data")

# -------------------------------------------------------------------
# AIRFLOW CONTAINER PATHS
# -------------------------------------------------------------------
# In docker-compose we mount the repo root to /opt/project
AIRFLOW_REPO_DIR = Path("/opt/project")
AIRFLOW_DATA_DIR = AIRFLOW_REPO_DIR / "data"

# -------------------------------------------------------------------
# PIPELINE CONTAINER PATHS (inside openpayments image)
# -------------------------------------------------------------------
CONTAINER_WORKDIR = "/app"
CONTAINER_DATA_DIR = "/app/data"
CONTAINER_OUT_ROOT = "/app/data/out"
CONTAINER_TOTALS_DIR = "/app/data/totals"
CONTAINER_METADATA_CACHE = "/app/metadata"

DEFAULT_IMAGE = "openpayments:latest"
DEFAULT_BUCKET = "openpayments-dezoomcamp2026-us-west-1-1f83ec"

DATA_MOUNT = Mount(
    source=HOST_DATA_DIR,
    target=CONTAINER_DATA_DIR,
    type="bind",
)

with DAG(
    dag_id="openpayments_phase3_download_validate_upload",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["openpayments", "aws", "athena", "phase3"],
    params={
        "image": Param(DEFAULT_IMAGE, type="string"),
        "year": Param(2023, type="integer", minimum=2018, maximum=2035),
        "max_files": Param(2, type="integer", minimum=0, maximum=100000),
        "ensure_totals": Param(True, type="boolean"),
        "rescrape_totals": Param(False, type="boolean"),
        "id_workers": Param(10, type="integer", minimum=1, maximum=50),
        "page_workers": Param(5, type="integer", minimum=1, maximum=10),
        "totals_workers": Param(2, type="integer", minimum=1, maximum=20),
        "totals_limit": Param(10, type="integer", minimum=1, maximum=5000),
        "totals_country": Param("UNITED STATES", type="string"),
        "resume": Param(True, type="boolean"),
        "verbose": Param(False, type="boolean"),
        "run_validation": Param(True, type="boolean"),
        "max_redownload_attempts": Param(3, type="integer", minimum=1, maximum=10),
        "upload_to_s3": Param(False, type="boolean"),
        "bucket": Param(DEFAULT_BUCKET, type="string"),
        "overwrite": Param(False, type="boolean"),
        "include_metadata": Param(True, type="boolean"),
        "include_latest_totals": Param(False, type="boolean"),
        "delete_local": Param(False, type="boolean"),
        "dry_run": Param(False, type="boolean"),
        "checksum_metadata": Param(False, type="boolean"),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    download = DockerOperator(
        task_id="download_in_container",
        image="{{ params.image }}",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="bridge",
        working_dir=CONTAINER_WORKDIR,
        entrypoint="python",
        command=[
            "src/download_general_payments.py",
            "--dataset", "general-payments",
            "--year", "{{ params.year }}",
            "--out-root", CONTAINER_OUT_ROOT,
            "--totals-dir", CONTAINER_TOTALS_DIR,
            "{{ '--ensure-totals' if params.ensure_totals else '--no-ensure-totals' }}",
            "{{ '--rescrape-totals' if params.rescrape_totals else '' }}",
            "--max-files", "{{ params.max_files }}",
            "--id-workers", "{{ params.id_workers }}",
            "--page-workers", "{{ params.page_workers }}",
            "--totals-workers", "{{ params.totals_workers }}",
            "--totals-limit", "{{ params.totals_limit }}",
            "--totals-country", "{{ params.totals_country }}",
            "{{ '--resume' if params.resume else '--no-resume' }}",
            "{{ '--verbose' if params.verbose else '' }}",
            "--run-id", "{{ run_id }}",
            "--airflow-mode",
            "--no-progress",
        ],
        mounts=[DATA_MOUNT],
        auto_remove="success",
    )

    @task
    def validate_manifest() -> str:
        ctx = get_current_context()
        run_id = ctx["run_id"]

        manifest_path = (
            AIRFLOW_DATA_DIR
            / "out"
            / "metadata"
            / "runs"
            / f"run_id={run_id}"
            / "manifest.json"
        )

        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")

        manifest = json.loads(manifest_path.read_text())

        status = manifest.get("status")
        if status not in {"completed", "completed_with_failures"}:
            raise ValueError(f"Unexpected manifest status: {status}")

        for key in ("report_csv", "audits_jsonl"):
            p = Path(manifest[key])
            if not p.exists():
                raise FileNotFoundError(f"{key} missing: {p}")

        if int(manifest.get("tasks_total", 0)) <= 0:
            raise ValueError("tasks_total <= 0; expected at least 1 task.")

        return str(manifest_path)

    validated_manifest = validate_manifest()
    validated_manifest.set_upstream(download)

    should_run_validation = ShortCircuitOperator(
        task_id="should_run_validation",
        python_callable=lambda **kwargs: bool(kwargs["params"]["run_validation"]),
    )
    should_run_validation.set_upstream(validated_manifest)

    validate_schema = DockerOperator(
        task_id="validate_schema_in_container",
        image="{{ params.image }}",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="bridge",
        working_dir=CONTAINER_WORKDIR,
        entrypoint="python",
        command=[
            "validation/validate_schema_and_redownload.py",
            "--dataset", "general-payments",
            "--year", "{{ params.year }}",
            "--out-root", CONTAINER_OUT_ROOT,
            "--metadata-cache", CONTAINER_METADATA_CACHE,
            "--max-redownload-attempts", "{{ params.max_redownload_attempts }}",
            "--page-workers", "{{ params.page_workers }}",
            "{{ '--verbose' if params.verbose else '' }}",
        ],
        mounts=[DATA_MOUNT],
        auto_remove="success",
    )
    validate_schema.set_upstream(should_run_validation)

    should_upload = ShortCircuitOperator(
        task_id="should_upload",
        python_callable=lambda **kwargs: bool(kwargs["params"]["upload_to_s3"]),
    )
    should_upload.set_upstream(validate_schema)
    should_upload.set_upstream(should_run_validation)

    upload = DockerOperator(
        task_id="upload_in_container",
        image="{{ params.image }}",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode="bridge",
        working_dir=CONTAINER_WORKDIR,
        entrypoint="python",
        command=[
            "src/upload_run_to_s3.py",
            "--bucket", "{{ params.bucket }}",
            "--out-root", CONTAINER_OUT_ROOT,
            "--totals-dir", CONTAINER_TOTALS_DIR,
            "{{ '--include-metadata' if params.include_metadata else '--no-include-metadata' }}",
            "{{ '--include-latest-totals' if params.include_latest_totals else '--no-include-latest-totals' }}",
            "{{ '--overwrite' if params.overwrite else '--no-overwrite' }}",
            "{{ '--delete-local' if params.delete_local else '' }}",
            "{{ '--dry-run' if params.dry_run else '' }}",
            "{{ '--checksum-metadata' if params.checksum_metadata else '' }}",
            "{{ '--verbose' if params.verbose else '' }}",
        ],
        mounts=[DATA_MOUNT],
        auto_remove="success",
    )
    upload.set_upstream(should_upload)

    @task
    def write_marker(manifest_path: str) -> str:
        ctx = get_current_context()
        run_id = ctx["run_id"]
        params = ctx["params"]

        mp = Path(manifest_path)
        run_dir = mp.parent

        marker = {
            "airflow_run_id": run_id,
            "dag_id": ctx["dag"].dag_id,
            "manifest_path": str(mp),
            "run_validation": bool(params["run_validation"]),
            "upload_to_s3": bool(params["upload_to_s3"]),
            "bucket": str(params["bucket"]),
            "written_at_utc": datetime.utcnow().isoformat() + "Z",
        }

        out = run_dir / "airflow_marker.json"
        out.write_text(json.dumps(marker, indent=2))
        return str(out)

    marker = write_marker(validated_manifest)
    marker.set_upstream(validate_schema)
    marker.set_upstream(upload)
    marker.set_upstream(should_upload)

    start >> download >> validated_manifest >> should_run_validation
    should_run_validation >> validate_schema >> should_upload
    should_upload >> upload >> marker >> end
