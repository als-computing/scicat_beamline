from pathlib import Path
from typing import Any, Dict, Optional
import typer

from prefect import flow, get_run_logger

from scicat_beamline import ingest


@flow(name="scicat-ingest-flow")
def scicat_ingest_flow(
    dataset_path: Path,
    dataset_files: list[Path],
    ingester_spec: str | None = None,
    owner_username: str | None = None,
    scicat_url: str | None = None,
    scicat_username: str | None = None,
    scicat_password: str | None = None,
    datasettracker_url: str | None = None,
    datasettracker_username: str | None = None,
    datasettracker_password: str | None = None,
    datasettracker_share_identifier: str | None = None
) -> Dict[str, Any]:
    """
    Runs the SciCat ingestion process implemented for the given spec identifier,
    on the given folder or file.
    Args:
        dataset_path: Path of the asset to ingest. May be file or directory depending on the spec.
        If SICAT_INGEST_INTERNAL_BASE_FOLDER or SCICAT_INGEST_BASE_FOLDER is set, this path is
        considered relative to that base folder.
    These remaining args are optional; if not provided, environment variables will be used.
        ingester_spec: Spec to ingest with. (or set SCICAT_INGEST_INGESTER_SPEC)
        owner_username: User doing the ingesting. May be different from the user_name, especially if using a token (or set SCICAT_INGEST_OWNER_USERNAME)
        scicat_url: Scicat server base url. If not provided, will try localhost default (or set SCICAT_INGEST_URL)
        scicat_username: Scicat server username (or set SCICAT_INGEST_USERNAME)
        scicat_password: Scicat server password (or set SCICAT_INGEST_PASSWORD)
    Returns:
        Dict containing task results or skip message
    """
    # Get the Prefect logger for the current flow run
    prefect_adapter = get_run_logger()
    # TODO: Get this from environment?
    prefect_flow_run_id = None

    return ingest(
        dataset_path=dataset_path,
        dataset_files=dataset_files,
        ingester_spec=ingester_spec,
        owner_username=owner_username,
        scicat_url=scicat_url,
        scicat_username=scicat_username,
        scicat_password=scicat_password,
        datasettracker_url=datasettracker_url,
        datasettracker_username=datasettracker_username,
        datasettracker_password=datasettracker_password,
        datasettracker_share_identifier=datasettracker_share_identifier,
        prefect_flow_run_id=prefect_flow_run_id,
        logger=prefect_adapter.logger
    )


if __name__ == "__main__":
    pass