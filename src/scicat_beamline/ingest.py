import glob
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List
import typer

from pyscicat.client import from_credentials

from scicat_beamline.ingesters import (als_733_saxs_ingest,
                                       als_832_dx_4_ingest,
                                       #    als_11012_ccd_theta_ingest,
                                       #    als_11012_igor_ingest,
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
    dataset_path: Path | list[Path] = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=True,
        help=(
            "Path or sub-path of the asset to ingest. May be file or directory depending on the spec."
            "Prepended with SCICAT_INGEST_INTERNAL_BASE_FOLDER or SCICAT_INGEST_BASE_FOLDER if set."
        ),
    ),
    ingester_spec: str | None = typer.Option(
        None,
        help="Spec to ingest with"
    ),
    owner_username: str | None = typer.Option(
        None,
        help="User doing the ingesting. May be different from the user_name.",
    ),
    scicat_url: str | None = typer.Option(
        None,
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    scicat_username: str | None = typer.Option(
        None,
        help="Scicat server username"
    ),
    scicat_password: str | None = typer.Option(
        None,
        help="Scicat server password"
    ),
    logger: logging.Logger | None = typer.Option(
        None,
        help="Logger to use"
    ),
):
    results: Dict[str, Any] = {}

    if logger is None:
        logger = logging.getLogger("scicat_operation")
        logger.setLevel("INFO")

    if not ingester_spec:
        ingester_spec = os.getenv("SCICAT_INGEST_SPEC", "")
        if not ingester_spec:
            logger.exception("Cannot resolve ingester spec.")
            return results

    if not scicat_url:
        scicat_url = os.getenv("SCICAT_INGEST_URL", "")
        if not scicat_url:
            scicat_url = "http://localhost:3000/api/v3"
            logger.warning(f"Using default SciCat URL {scicat_url}")

    if not scicat_username:
        scicat_username = os.getenv("SCICAT_INGEST_USERNAME", "")
        if not scicat_username:
            logger.exception("Cannot resolve SciCat username.")
            return results

    if not scicat_password:
        scicat_password = os.getenv("SCICAT_INGEST_PASSWORD", "")
        if not scicat_password:
            logger.exception("Cannot resolve SciCat password.")
            return results

    if not owner_username:
        owner_username = os.getenv("SCICAT_INGEST_OWNER_USERNAME", "")
        if not owner_username:
            logger.info("Using SciCat username as owner username.")
            owner_username = scicat_username

    logger.info(f"Using ingester spec {ingester_spec}")

    ingestion_function = None

    if ingester_spec == "bltest":
        ingestion_function = als_test_ingest
    elif ingester_spec == "als_11012_igor":
        ingestion_function = als_733_saxs_ingest
    elif ingester_spec == "als_832_dx_4":
        ingestion_function = als_832_dx_4_ingest
    elif ingester_spec == "als_11012_scattering":
        ingestion_function = als_11012_scattering_ingest
    elif ingester_spec == "als_11012_nexafs":
        ingestion_function = nexafs_ingest
    elif ingester_spec == "nsls2_rsoxs_sst1":
        ingestion_function = nsls2_rsoxs_sst1_ingest
    elif ingester_spec == "nsls2_nexafs_sst1":
        ingestion_function = nsls2_nexafs_sst1_ingest
    elif ingester_spec == "als733_saxs":
        ingestion_function = als_733_saxs_ingest
    elif ingester_spec == "nsls2_trexs_smi":
        ingestion_function = nsls2_TREXS_smi_ingest
    elif ingester_spec == "polyfts_dscft":
        ingestion_function = polyfts_dscft_ingest
    else:
        logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
        return results

    prepend_path = None
    if "SCICAT_INGEST_INTERNAL_BASE_FOLDER" in os.environ:
        prepend_path = os.getenv("SCICAT_INGEST_INTERNAL_BASE_FOLDER", ".")
        logger.info(f"Using internal base folder: {prepend_path}")
    elif "SCICAT_INGEST_BASE_FOLDER" in os.environ:
        prepend_path = os.getenv("SCICAT_INGEST_BASE_FOLDER", ".")
        logger.info(f"Using base folder: {prepend_path}")
    else:
        logger.info(f"No base folder set.")

    if not isinstance(dataset_path, list):
        dataset_path = [dataset_path]

    folders = []
    files = []

    for one_path in dataset_path:
        if prepend_path:
            full_path = Path(prepend_path, one_path).resolve()
        else:
            full_path = one_path.resolve()
        logger.info(f"Resolved dataset file path: {full_path}")

        if not full_path.exists():
            logger.error(f"Given a path does not exist: {full_path}")
            continue

        if full_path.is_symlink():
            logger.warning(f"Symlink detected. (Should not happen!) Skipping: {full_path}")
        elif full_path.is_file():
            files.append(full_path)
        elif full_path.is_dir():
            folders.append(full_path)

    if len(folders) + len(files) == 0:
        logger.error("No valid files or folders to ingest.")
        return results

    if len(folders) > 1:
        logger.error("Given more than one folder to ingest. Only one folder is supported.")
        return results

    elif len(folders) == 0: # Only files given
        # We're going to assume that every file handed directly to the ingester
        # is valid for the ingester spec.
        ingest_files_iter = files

    else: # One folder given
        ingestion_search_path = folders[0]

        # If we've been given one folder, we attempt to write a logfile to it.
        # At the same time we're streaming logs to the console, we'll write them here.

        logger.info("Setting up ingester logfile.")

        logfile = Path(ingestion_search_path, "scicat_ingest_log.log")
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
        )
        fileHandler = logging.FileHandler(
            logfile, mode="a", encoding=None, delay=False, errors=None
        )
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

        # Since we haven't been handed a list of files, we need to walk the folder
        # and make our own.  The criteria vary based on the ingester spec.

        ingest_files_iter = []

        if ingester_spec == "bltest":
            temp_iter = standard_iterator(f"{ingestion_search_path}/*.txt")
            for file_str in temp_iter:
                ingest_files_iter.append(file_str)

        elif ingester_spec == "als_11012_igor":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/CCD/*/dat/")

        elif ingester_spec == "als_832_dx_4":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")

        elif ingester_spec == "als_11012_scattering":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/CCD/*/")

        elif ingester_spec == "als_11012_nexafs":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/Nexafs/*")

        elif ingester_spec == "nsls2_rsoxs_sst1":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")

        elif ingester_spec == "nsls2_nexafs_sst1":
            temp_iter = standard_iterator(f"{ingestion_search_path}/*")
            for file_str in temp_iter:
                if (
                    file_str.endswith(".log")
                    or file_str.endswith(".csv")
                    or file_str.endswith(".txt")
                ):
                    continue
                ingest_files_iter.append(file_str)

        elif ingester_spec == "als733_saxs":
            temp_iter = standard_iterator(f"{ingestion_search_path}/*.txt")
            for file_str in temp_iter:
                ingest_files_iter.append(file_str)

        elif ingester_spec == "nsls2_trexs_smi":
            ingest_files_iter = standard_iterator("{ingestion_search_path}/*/")

        elif ingester_spec == "polyfts_dscft":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")

        else:
            logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
            return results

    try:
        pyscicat_client = from_credentials(scicat_url, scicat_username, scicat_password)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            issues: List[Issue] = []
            dataset_id = None
            for ingest_file_str in ingest_files_iter:
                ingest_file_path = Path(ingest_file_str)
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
                logger.warning("No dataset ID returned.")

            logger.info("Ingestion finished.")

    except Exception:
        logger.exception(f" Error running ingester {ingester_spec}")

    return results


if __name__ == "__main__":
    typer.run(ingest)
