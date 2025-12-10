import logging
import tempfile
from pathlib import Path

import typer

from pyscicat.client import from_credentials, from_token
from scicat_beamline.common_ingester_utils import Issue


def standard_iterator(pattern: str):
    import glob

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
        help="User doing the ingesting. May be different from the user_name, especially if using a token",
    ),
    base_url: str = typer.Argument(
        "http://localhost:3000/api/v3",
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    token: str = typer.Option(None, help="Scicat api token"),
    username: str = typer.Option(None, help="Scicat server username"),
    password: str = typer.Option(None, help="Scicat server password"),
):

    # At the same time we're streaming logs to console,
    # we also want to write them to a file in the dataset folder.

    logger = logging.getLogger("scicat_ingest")
    logger.setLevel("INFO")
    logfile = Path(dataset_path, "scicat_ingester_log.txt")

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )

    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    fileHandler = logging.FileHandler(
        logfile, mode="a", encoding=None, delay=False, errors=None
    )
    fileHandler.setFormatter(formatter)

    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)

    try:
        ingestion_function = None

        ingest_files_iter = []
        if ingester_spec == "als_11012_igor":
            ingest_files_iter = standard_iterator(f"{dataset_path}/CCD/*/dat/")
            import scicat_beamline.ingesters.als_11012_igor as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "als_832_dx_4":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            import scicat_beamline.ingesters.als_832_dx_4 as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "als_11012_scattering":
            ingest_files_iter = standard_iterator(f"{dataset_path}/CCD/*/")
            import scicat_beamline.ingesters.als_11012_scattering as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "als_11012_nexafs":
            ingest_files_iter = standard_iterator(f"{dataset_path}/Nexafs/*")
            import scicat_beamline.ingesters.nexafs as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "nsls2_rsoxs_sst1":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            import scicat_beamline.ingesters.nsls2_RSoXS as ingester_module

            ingestion_function = ingester_module.ingest

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
            import scicat_beamline.ingesters.nsls2_nexafs_sst1 as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "als733_saxs":
            temp_iter = standard_iterator(f"{dataset_path}/*.txt")
            for file_str in temp_iter:
                # Matt Landsman said not to include these in ingestion
                if "autoexpose" in file_str or "beamstop_test" in file_str:
                    continue
                ingest_files_iter.append(file_str)
            import scicat_beamline.ingesters.als_733_SAXS as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "nsls2_trexs_smi":
            ingest_files_iter = standard_iterator("{dataset_path}/*/")
            import scicat_beamline.ingesters.nsls2_TREXS_smi as ingester_module

            ingestion_function = ingester_module.ingest

        elif ingester_spec == "polyfts_dscft":
            ingest_files_iter = standard_iterator(f"{dataset_path}/*/")
            import scicat_beamline.ingesters.polyfts_dscft as ingester_module

            ingestion_function = ingester_module.ingest

        else:
            logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
            return

        logger.info(f"Using ingester spec {ingester_spec}")

        if token:
            client = from_token(base_url, token)
        elif username and password:
            client = from_credentials(base_url, username, password)
        else:
            typer.echo("Must provide either SciCat token or username and password")
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            issues = []
            dataset_id = None
            for ingest_file_str in ingest_files_iter:
                ingest_file_path = Path(ingest_file_str)
                temp_path = Path(temp_dir)
                if ingest_file_path.exists():
                    logger.info(f"Ingesting {ingest_file_path}")
                    dataset_id = ingestion_function(
                        client, ingest_user, ingest_file_path, temp_path, issues
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


    except Exception:
        logger.exception(f" Error running ingester {ingester_module}")


if __name__ == "__main__":
    typer.run(ingest)
