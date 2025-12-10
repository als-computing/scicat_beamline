# This fixes an error where the projectdirs component of the path was
# for some reason changed to a symlink.

import os
from pathlib import Path

from dotenv import load_dotenv
from pyscicat.client import from_credentials
from pyscicat.model import Dataset

load_dotenv()

SCICAT_URL = os.getenv("SCICAT_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

if not SCICAT_URL:
    raise ValueError("SCICAT_URL environment variable is required")
if not USERNAME:
    raise ValueError("USERNAME environment variable is required")
if not PASSWORD:
    raise ValueError("PASSWORD environment variable is required")

client = from_credentials(SCICAT_URL, USERNAME, PASSWORD)

datasets = client.datasets_get_many()

if datasets is not None:
    for dataset in datasets:
        dataset = Dataset(**dataset)

        path_array = list(Path(dataset["sourceFolder"]).parts)
        path_array[3] = "projectdirs"
        actualPath = os.path.join(*path_array)

        dataset.sourceFolder = actualPath

        client.update_dataset(dataset, dataset.pid)
