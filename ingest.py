
from importlib.util import spec_from_file_location, module_from_spec
import logging
from pathlib import Path
import tempfile
from typing import List

import typer

from pyscicat.client import from_credentials, from_token

from scicat_beamline.utils import Issue

logger = logging.getLogger('scicat_ingest')
logger.setLevel('INFO')
logname = "data_movement_733.log"
handler = logging.StreamHandler()
handler.suffix = "%Y%m%d"
handler.setLevel('INFO')
formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
logger.addHandler(handler)
app = typer.Typer()


def ingest(
    ingestor_module: Path = typer.Argument(..., help='Spec to ingest'),
    dataset_path: Path = typer.Argument(
        ...,
        help=('Path of the asset to ingest. '
              'May be file or directory depending on the spec '
              'and its ingestor'),
        ),
    ingest_user: str = typer.Argument("ingestor", help="User doing the ingesting. May be different from the user_name, especially if using a token"),
    base_url: str = typer.Argument("http://localhost:3000/api/v3", help="Scicat server base url. If not provided, will use pyscicat default"),
    token: str = typer.Option(None, help="Scicat api token"),
    username: str = typer.Option(None, help="Scicat server username"),
    password: str = typer.Option(None, help="Scicat server password"),

):
    try:
        spec = spec_from_file_location(ingestor_module.stem, ingestor_module)
        ingestor_module = module_from_spec(spec)
        spec.loader.exec_module(ingestor_module)

        logger.info(
            f"loaded ingestor with spec {ingestor_module.ingest_spec} "
            f"from {ingestor_module}"
        )

        if token:
            client = from_token(base_url, token)
        elif username and password:
            client = from_credentials(base_url, username, password)
        else:
            typer.echo("Must provide either token or username and password")
            return

        issues = []
        with tempfile.TemporaryDirectory() as thumbs_dir:
            ingestor_module.ingest(client, ingest_user, dataset_path, thumbs_dir, issues)
            if len(issues) > 0:
                logger.info(f"Issues found {[str(issue) for issue in issues]}")
    except Exception:
        logger.exception(f" Error loading {ingestor_module}")


if __name__ == '__main__':
    typer.run(ingest)
