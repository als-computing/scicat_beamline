"""
Prefect flow that ingests data into SciCat
"""

import os
from pathlib import Path
from typing import Any, Dict

import typer
from prefect import flow, get_run_logger

from scicat_beamline_ingestion.ingest import ingest


@flow(name="scicat-ingest-flow")
def scicat_ingest_flow(
    ingester_spec: str = typer.Argument(
        default="blTEST",
        envvar="INGEST_SPEC",
        help="Spec to ingest with"),
    dataset_path: Path = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=True,
        help=(
            "Path of the asset to ingest. May be file or directory depending on the spec."
        ),
    ),
    ingest_user: str = typer.Argument(
        "ingester",
        envvar="INGEST_USER",
        help="User doing the ingesting. May be different from the user_name.",
    ),
    base_url: str = typer.Argument(
        "http://localhost:3000/api/v3",
        envvar="SCICAT_URL",
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    username: str = typer.Option(
        None, 
        envvar="SCICAT_USERNAME",
        help="Scicat server username"
    ),
    password: str = typer.Option(
        None, 
        envvar="SCICAT_PASSWORD", 
        help="Scicat server password"
    )
) -> Dict[str, Any]:
    """
    Flow that runs the SciCat ingestion process implemented for the given spec identifier,
    on the given folder or file.
    Args:
        ingester_spec: Spec to ingest with
        dataset_path: Path of the asset to ingest. May be file or directory depending on the spec.
        ingest_user: User doing the ingesting. May be different from the user_name, especially if using a token
        base_url: Scicat server base url. If not provided, will try localhost default
        username: Scicat server username
        password: Scicat server password
    Returns:
        Dict containing task results or skip message
    """
    # Get the Prefect logger for the current flow run
    prefect_adapter = get_run_logger()
    dataset_full_path = Path(os.getenv("INTERNAL_BASE_FOLDER", os.getenv("BASE_FOLDER", ".")), dataset_path).resolve()

    return ingest(
        ingester_spec=ingester_spec,
        dataset_path=dataset_full_path,
        ingest_user=ingest_user,
        base_url=base_url,
        username=username,
        password=password,
        logger=prefect_adapter.logger
    )
