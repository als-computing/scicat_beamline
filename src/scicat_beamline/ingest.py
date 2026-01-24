import glob
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List
import typer

from dataset_metadata_schemas.utilities import (read_als_metadata_file, write_als_metadata_file)
from dataset_tracker_client.client import DatasettrackerClient
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
    datasettracker_url: str | None = typer.Option(
        None,
        help="Dataset Tracker server base url. Using the Dataset Tracker is optional."
    ),
    datasettracker_username: str | None = typer.Option(
        None,
        help="Dataset Tracker server username. Using the Dataset Tracker is optional."
    ),
    datasettracker_password: str | None = typer.Option(
        None,
        help="Dataset Tracker server password. Using the Dataset Tracker is optional."
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

    # (Must attempt to resolve scicat_username first above)
    if not owner_username:
        owner_username = os.getenv("SCICAT_INGEST_OWNER_USERNAME", "")
        if not owner_username:
            logger.info("Using SciCat username as owner username.")
            owner_username = scicat_username

    if not datasettracker_url:
        datasettracker_url = os.getenv("DATASETTRACKER_URL", "")
        if not datasettracker_url:
            logger.info("Dataset Tracker URL not set. Dataset Tracker will not be used.")

    if not datasettracker_username:
        datasettracker_username = os.getenv("DATASETTRACKER_USERNAME", "")
        if not datasettracker_username:
            logger.warning("Cannot resolve Dataset Tracker username.")

    if not datasettracker_password:
        datasettracker_password = os.getenv("DATASETTRACKER_PASSWORD", "")
        if not datasettracker_password:
            logger.warning("Cannot resolve Dataset Tracker password.")

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

    internal_base_folder = os.getenv("SCICAT_INGEST_INTERNAL_BASE_FOLDER", "")
    base_folder = os.getenv("SCICAT_INGEST_BASE_FOLDER", "")

    prepend_path = None
    # If an internal base folder is set (because we're accessing a mounted volume inside a container),
    # use that.
    if internal_base_folder:
        prepend_path = internal_base_folder
        logger.info(f"Using internal base folder: {prepend_path}")
    # If there's no internal base folder set, look for a regular base folder.
    elif base_folder:
        prepend_path = base_folder
        logger.info(f"Using base folder: {prepend_path}")
    else:
        logger.info(f"No base folder set.")

    # Sort the given paths into files and folders, and validate them.

    folders = []
    files = []

    if not isinstance(dataset_path, list):
        dataset_path = [dataset_path]
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

    als_dataset_metadata = None
    ingestion_search_path = None

    if len(folders) + len(files) == 0:
        logger.error("No valid files or folders to ingest.")
        return results

    if len(folders) > 1:
        logger.error("Given more than one folder to ingest. Only one folder is supported.")
        return results

    elif len(folders) == 0: # Only files given
        # We're going to assume that every file handed directly to the ingester
        # is valid for the ingester spec.

        # This is not a good path, because it means there is no base folder to look in,
        # so we can't:
        #  * Look for an als-dataset-metadata.json file
        #  * Create a logfile in the folder being ingested
        #  * Make a share sublocation record in the Dataset Tracker,
        #    which means we can't use the Dataset Tracker at all.
        # This path should be avoided.  It will get files into SciCat but not in a way
        # where we can track them.
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

        # Since we have a folder, we'll check for an existing als-dataset-metadata.json file.
        try:
            als_dataset_metadata = read_als_metadata_file(file_path="/Users/gwbirkel/Documents/scicat_beamline/src/scicat_beamline/testing/test_data/bltest/als-dataset-metadata.json")
        except Exception as e:
            logger.warning("Did not find a als-dataset-metadata.json file.")

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
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")

        elif ingester_spec == "polyfts_dscft":
            ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")

        else:
            logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
            return results

    try:
        pyscicat_client = from_credentials(scicat_url, scicat_username, scicat_password)
        datasettracker_client = None
        if datasettracker_username and datasettracker_password and datasettracker_url:
            try:
                datasettracker_client = DatasettrackerClient(
                    base_url=datasettracker_url,
                    username=datasettracker_username,
                    password=datasettracker_password,
                )
            except Exception as e:
                logger.warning(
                    f"Cannot connect to Dataset Tracker client. Dataset Tracker will not be used. Error: {e}"
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            issues: List[Issue] = []
            for ingest_file_str in ingest_files_iter:
                ingest_file_path = Path(ingest_file_str)
                if ingest_file_path.exists():
                    logger.info(f"Ingesting {ingest_file_path}")
                    als_dataset_metadata = ingestion_function(
                        pyscicat_client, datasettracker_client, owner_username, als_dataset_metadata, ingest_file_path, temp_path, issues
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

            if als_dataset_metadata is not None:

                dataset_id = None
                try:
                    dataset_id = als_dataset_metadata.scicat_dataset_id
                except Exception:
                    pass

                if dataset_id is not None:
                    logger.info(f"Dataset ID: {dataset_id}")
                else:
                    logger.warning("No dataset ID returned.")

                results = als_dataset_metadata.model_dump(mode="json")

                if ingestion_search_path is not None:
                    # Write back the ALS dataset metadata file with any updates.
                    try:
                        write_als_metadata_file(
                            metadata=als_dataset_metadata,
                            file_path=Path(ingestion_search_path, "als-dataset-metadata.json"),
                        )
                        logger.info(
                            "Wrote updated als-dataset-metadata.json file."
                        )
                    except Exception as e:
                        logger.warning(
                            f"Could not write updated als-dataset-metadata.json file. Error: {e}"
                        )

            logger.info("Ingestion finished.")

    except Exception:
        logger.exception(f" Error running ingester {ingester_spec}")

    return results


if __name__ == "__main__":
    typer.run(ingest)
