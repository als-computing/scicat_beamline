# This fixes an error where the projectdirs component of the path was 
# for some reason changed to a symlink.

from pyscicat.client import from_credentials
from pyscicat.model import RawDataset, DerivedDataset
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

SCICAT_URL = os.getenv("SCICAT_URL")
USERNAME = os.getenv("USERNAME")
INGEST_USER = os.getenv("INGEST_USER")
PASSWORD = os.getenv("PASSWORD")

assert type(SCICAT_URL) == str and len(SCICAT_URL) != 0
assert type(USERNAME) == str and len(USERNAME) != 0
assert type(PASSWORD) == str and len(PASSWORD) != 0
assert type(INGEST_USER) == str and len(INGEST_USER) != 0

client = from_credentials(SCICAT_URL, USERNAME, PASSWORD)

datasets = client.datasets_get_many()

for dataset in datasets:
    path_array = list(Path(dataset["sourceFolder"]).parts)
    path_array[3] = "projectdirs"
    actualPath = os.path.join(*path_array)
    dataset["sourceFolder"] = actualPath

    if (dataset["type"] == "raw"):
        dataset = RawDataset(**dataset)
    elif dataset["type"] == "derived":
        dataset = DerivedDataset(**dataset)

    client.update_dataset(dataset, dataset.pid)
