import glob
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import typer
from pyscicat.client import from_credentials

from scicat_beamline.ingesters import (als_733_saxs_ingest,
                                       als_832_dx_4_ingest,
                                       als_11012_ccd_theta_ingest,
                                       als_11012_igor_ingest,
                                       als_11012_scattering_ingest,
                                       als_test_ingest, nexafs_ingest,
                                       nsls2_nexafs_sst1_ingest,
                                       nsls2_rsoxs_sst1_ingest,
                                       nsls2_TREXS_smi_ingest,
                                       polyfts_dscft_ingest)
from scicat_beamline.utils import Issue


def standard_iterator(pattern: str):
    return glob.iglob(pattern)


def ingest(
    dataset_path: Path = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=True,
        help=(
            "Path or sub-path of the asset to ingest. May be file or directory depending on the spec. Prepended with SCICAT_INGEST_INTERNAL_BASE_FOLDER or SCICAT_INGEST_BASE_FOLDER if set."
        ),
    ),
    ingester_spec: Optional[str] = typer.Option(None,
        help="Spec to ingest with"
    ),
    owner_username: Optional[str] = typer.Option(None,
        help="User doing the ingesting. May be different from the user_name.",
    ),
    scicat_url: Optional[str] = typer.Option(None,
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    scicat_username: Optional[str] = typer.Option(None,
        help="Scicat server username"
    ),
    scicat_password: Optional[str] = typer.Option(None,
        help="Scicat server password"
    ),
    logger: Optional[logging.Logger] = typer.Option(None,
        help="Logger to use"
    ),
):
    results:Dict[str, Any] = {}

    if logger is None:
        logger = logging.getLogger("scicat_ingest")
        logger.setLevel("INFO")

    if not ingester_spec:
        scicat_url = os.getenv("SCICAT_INGEST_SPEC", "")
        if not ingester_spec:
            logger.exception(f"Cannot resolve ingester spec.")
            return results

    if not scicat_url:
        scicat_url = os.getenv("SCICAT_INGEST_URL", "")
        if not scicat_url:
            scicat_url = "http://localhost:3000/api/v3"
            logger.warning(f"Using default SciCat URL {scicat_url}")

    if not scicat_username:
        scicat_username = os.getenv("SCICAT_INGEST_USERNAME", "")
        if not scicat_username:
            logger.exception(f"Cannot resolve SciCat username.")
            return results

    if not scicat_password:
        scicat_password = os.getenv("SCICAT_INGEST_PASSWORD", "")
        if not scicat_password:
            logger.exception(f"Cannot resolve SciCat password.")
            return results

    if not owner_username:
        owner_username = os.getenv("SCICAT_INGEST_OWNER_USERNAME", "")
        if not owner_username:
            logger.info(f"Using SciCat username as owner username.")
            owner_username = scicat_username

    if "SCICAT_INGEST_INTERNAL_BASE_FOLDER" in os.environ:
        dataset_full_path = Path(os.getenv("SCICAT_INGEST_INTERNAL_BASE_FOLDER", "."), dataset_path).resolve()
        logger.info(f"Using internal base folder; resolved dataset path: {dataset_full_path}") 
    elif "SCICAT_INGEST_BASE_FOLDER" in os.environ:
        dataset_full_path = Path(os.getenv("SCICAT_INGEST_BASE_FOLDER", "."), dataset_path).resolve()
        logger.info(f"Using base folder; resolved dataset path: {dataset_full_path}") 
    else:
        dataset_full_path = Path(dataset_path).resolve()
        logger.info(f"No base folder set; resolved dataset path: {dataset_full_path}")

    # At the same time we're streaming logs to the console,
    # we'll write them to a file in the dataset folder.

    logger.info(f"Setting up ingester logfile.")

    logfile = Path(dataset_full_path, "scicat_ingest_log.log")
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    fileHandler = logging.FileHandler(
        logfile, mode="a", encoding=None, delay=False, errors=None
    )
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    logger.info(f"Using ingester spec {ingester_spec}")

    try:
        ingestion_function = None
        ingest_files_iter = []

        if ingester_spec == "bltest":
            temp_iter = standard_iterator(f"{dataset_full_path}/*.txt")
            for file_str in temp_iter:
                ingest_files_iter.append(file_str)
            ingestion_function = als_test_ingest

        elif ingester_spec == "als_11012_igor":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/CCD/*/dat/")
            ingestion_function = als_733_saxs_ingest

        elif ingester_spec == "als_832_dx_4":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/*/")
            ingestion_function = als_832_dx_4_ingest

        elif ingester_spec == "als_11012_scattering":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/CCD/*/")
            ingestion_function = als_11012_scattering_ingest

        elif ingester_spec == "als_11012_nexafs":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/Nexafs/*")
            ingestion_function = nexafs_ingest

        elif ingester_spec == "nsls2_rsoxs_sst1":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/*/")
            ingestion_function = nsls2_rsoxs_sst1_ingest

        elif ingester_spec == "nsls2_nexafs_sst1":
            temp_iter = standard_iterator(f"{dataset_full_path}/*")
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
            temp_iter = standard_iterator(f"{dataset_full_path}/*.txt")
            for file_str in temp_iter:
                # Matt Landsman said not to include these in ingestion
                if "autoexpose" in file_str or "beamstop_test" in file_str:
                    continue
                ingest_files_iter.append(file_str)
            ingestion_function = als_733_saxs_ingest

        elif ingester_spec == "nsls2_trexs_smi":
            ingest_files_iter = standard_iterator("{dataset_full_path}/*/")
            ingestion_function = nsls2_TREXS_smi_ingest

        elif ingester_spec == "polyfts_dscft":
            ingest_files_iter = standard_iterator(f"{dataset_full_path}/*/")
            ingestion_function = polyfts_dscft_ingest

        else:
            logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
            return results

        pyscicat_client = from_credentials(scicat_url, scicat_username, scicat_password)

        with tempfile.TemporaryDirectory() as temp_dir:
            issues: List[Issue] = []
            dataset_id = None
            for ingest_file_str in ingest_files_iter:
                ingest_file_path = Path(ingest_file_str)
                temp_path = Path(temp_dir)
                if ingest_file_path.exists():
                    logger.info(f"Ingesting {ingest_file_path}")
                    dataset_id = ingestion_function(
                        pyscicat_client, owner_username, ingest_file_path, temp_path, issues
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
