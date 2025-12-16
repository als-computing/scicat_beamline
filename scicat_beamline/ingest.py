import os
import logging
import tempfile
from pathlib import Path
import glob

import typer
from typing import Any, Dict

from pyscicat.client import from_credentials

from scicat_beamline.common_ingester_utils import ( Issue )

from scicat_beamline.ingesters import (
    als_733_saxs_ingest,
    als_832_dx_4_ingest,
    als_11012_ccd_theta_ingest,
    als_11012_igor_ingest,
    als_11012_scattering_ingest,
    nexafs_ingest,
    als_test_ingest,
    nsls2_nexafs_sst1_ingest,
    nsls2_rsoxs_sst1_ingest,
    nsls2_TREXS_smi_ingest,
    polyfts_dscft_ingest,
)


def standard_iterator(pattern: str):
    return glob.iglob(pattern)


def ingest(
    ingester_spec: str = typer.Argument(..., help="Spec to ingest"),
    dataset_path: Path = typer.Argument(
        ...,
        help=(
            "Path of the asset to ingest. "
            "May be file or directory depending on the spec "
            "and its ingester"
        ),
    ),
    ingest_user: str = typer.Argument(
        "ingester",
        help="User doing the ingesting. May be different from the user_name.",
    ),
    base_url: str = typer.Argument(
        "http://localhost:3000/api/v3",
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    username: str = typer.Option(None, help="Scicat server username"),
    password: str = typer.Option(None, help="Scicat server password"),
    logger: logging.Logger = typer.Option(None, help="Logger to use"),
):

    # At the same time we're streaming logs to the console,
    # we'll write them to a file in the dataset folder.

    if logger is None:
        logger = logging.getLogger("scicat_ingest")
        logger.setLevel("INFO")

    logger.info(f"Setting up ingester logfile.")

    # A visibity test
    logger.info(f"Testing datafiles visibility in {dataset_path}")

    datafiles = []
    totalSize = 0

    for file in glob.iglob(str(dataset_path) + "/**", recursive=True):
        file = Path(file)
        size = 0
        if file.is_file() is True:
            size = file.lstat().st_size
        datafiles.append({"path": str(file), "size": size})
        totalSize += size

    logger.info(f"Datafiles visibility test found {len(datafiles)} files totaling {totalSize} bytes.")
    for df in datafiles:
        logger.info(f"  Datafile: {df["path"]} size {df["size"]} bytes")

    logfile = Path(dataset_path, "scicat_ingester_log.txt")
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    fileHandler = logging.FileHandler(
        logfile, mode="a", encoding=None, delay=False, errors=None
    )
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    logger.info(f"Using ingester spec {ingester_spec}")

    results:Dict[str, Any] = {}

    try:
        ingestion_function = None
        ingest_files_iter = []

        if ingester_spec == "bltest":
            temp_iter = standard_iterator(f"{dataset_path}/*.txt")
            for file_str in temp_iter:
                ingest_files_iter.append(file_str)
            ingestion_function = als_test_ingest

        elif ingester_spec == "als_11012_igor":
            ingest_files_iter = standard_iterator(f"{dataset_path}/CCD/*/dat/")
            ingestion_function = als_733_saxs_ingest

        elif ingester_spec == "als_832_dx_4":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            ingestion_function = als_832_dx_4_ingest

        elif ingester_spec == "als_11012_scattering":
            ingest_files_iter = standard_iterator(f"{dataset_path}/CCD/*/")
            ingestion_function = als_11012_scattering_ingest

        elif ingester_spec == "als_11012_nexafs":
            ingest_files_iter = standard_iterator(f"{dataset_path}/Nexafs/*")
            ingestion_function = nexafs_ingest

        elif ingester_spec == "nsls2_rsoxs_sst1":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            ingestion_function = nsls2_rsoxs_sst1_ingest

        elif ingester_spec == "nsls2_nexafs_sst1":
            temp_iter = standard_iterator(f"{dataset_path}/*")
            for file_str in temp_iter:
                if (
                    file_str.endswith(".log")
                    or file_str.endswith(".csv")
                    or file_str.endswith(".txt")
                ):
                    continue
                ingest_files_iter.append(file_str)
            ingestion_function = nsls2_nexafs_sst1_ingest

        elif ingester_spec == "als733_saxs":
            temp_iter = standard_iterator(f"{dataset_path}/*.txt")
            for file_str in temp_iter:
                # Matt Landsman said not to include these in ingestion
                if "autoexpose" in file_str or "beamstop_test" in file_str:
                    continue
                ingest_files_iter.append(file_str)
            ingestion_function = als_733_saxs_ingest

        elif ingester_spec == "nsls2_trexs_smi":
            ingest_files_iter = standard_iterator("{dataset_path}/*/")
            ingestion_function = nsls2_TREXS_smi_ingest

        elif ingester_spec == "polyfts_dscft":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            ingestion_function = polyfts_dscft_ingest

        else:
            logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
            return results

        if username and password:
            pyscicat_client = from_credentials(base_url, username, password)
        else:
            typer.echo("Must provide a SciCat username and password")
            return results

        with tempfile.TemporaryDirectory() as temp_dir:
            issues = []
            dataset_id = None
            for ingest_file_str in ingest_files_iter:
                ingest_file_path = Path(ingest_file_str)
                temp_path = Path(temp_dir)
                if ingest_file_path.exists():
                    logger.info(f"Ingesting {ingest_file_path}")
                    dataset_id = ingestion_function(
                        pyscicat_client, ingest_user, ingest_file_path, temp_path, issues
                    )
                else:
                    logger.warning(
                        f"Ingest file path {ingest_file_path} does not exist"
                    )

            if len(issues) > 0:
                logger.info("The following issues were encountered during ingestion:")
                for issue in issues:
                    if issue.severity == "error":
                        logger.error(f"{issue.msg}")
                    else:
                        logger.warning(f"{issue.msg}")

            if dataset_id is not None:
                results["dataset_id"] = dataset_id
                logger.info(f"Dataset ID: {dataset_id}")
            else:
                logger.warning(f"No dataset ID returned.")

            logger.info(f"Ingestion finished.")

    except Exception:
        logger.exception(f" Error running ingester {ingester_spec}")

    return results

if __name__ == "__main__":
    typer.run(ingest)
