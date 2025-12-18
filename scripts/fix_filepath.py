# This fixes an error where the projectdirs component of the path was
# for some reason changed to a symlink.

import os
from pathlib import Path

from dotenv import load_dotenv
from pyscicat.client import from_credentials
from pyscicat.model import Dataset

load_dotenv()

SCICAT_INGEST_URL = os.getenv("SCICAT_INGEST_URL")
SCICAT_INGEST_USERNAME = os.getenv("SCICAT_INGEST_USERNAME")
SCICAT_INGEST_PASSWORD = os.getenv("SCICAT_INGEST_PASSWORD")

if not SCICAT_INGEST_URL:
    raise ValueError("SCICAT_INGEST_URL environment variable is required")
if not SCICAT_INGEST_USERNAME:
    raise ValueError("SCICAT_INGEST_USERNAME environment variable is required")
if not SCICAT_INGEST_PASSWORD:
    raise ValueError("SCICAT_INGEST_PASSWORD environment variable is required")

client = from_credentials(SCICAT_INGEST_URL, SCICAT_INGEST_USERNAME, SCICAT_INGEST_PASSWORD)

datasets = client.datasets_get_many()

if datasets is not None:
    for dataset in datasets:
        dataset = Dataset(**dataset)

        path_array = list(Path(dataset["sourceFolder"]).parts)
        path_array[3] = "projectdirs"
        actualPath = os.path.join(*path_array)

        dataset.sourceFolder = actualPath

        client.update_dataset(dataset, dataset.pid)
