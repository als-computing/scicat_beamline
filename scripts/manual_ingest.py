import os
import pathlib
from pathlib import Path
import glob

from dotenv import load_dotenv

from scicat_beamline.ingest import ingest

load_dotenv()

# Examples of the vars from the .env file
# ROOT_FOLDER=/home/j/programming/work/October_data
# SCICAT_INGEST_URL=https://mwet.lbl.gov/api/v3
# SCICAT_INGEST_USERNAME=username
# SCICAT_INGEST_OWNER_USERNAME=username
# SCICAT_INGEST_PASSWORD=password
# SCICAT_INGEST_SPEC=als_11012_igor OR als_11012_scattering OR als_11012_nexafs

SCICAT_INGEST_BASE_FOLDER = os.getenv("SCICAT_INGEST_BASE_FOLDER")
SCICAT_INGEST_SUBFOLDER = os.getenv("SCICAT_INGEST_SUBFOLDER", ".")
SCICAT_INGEST_URL = os.getenv("SCICAT_INGEST_URL")
SCICAT_INGEST_USERNAME = os.getenv("SCICAT_INGEST_USERNAME")
SCICAT_INGEST_OWNER_USERNAME = os.getenv("SCICAT_INGEST_OWNER_USERNAME")
SCICAT_INGEST_PASSWORD = os.getenv("SCICAT_INGEST_PASSWORD")
SCICAT_INGEST_SPEC = os.getenv("SCICAT_INGEST_SPEC")
DATASETTRACKER_URL = os.getenv("DATASETTRACKER_URL")
DATASETTRACKER_USERNAME = os.getenv("DATASETTRACKER_USERNAME")
DATASETTRACKER_PASSWORD = os.getenv("DATASETTRACKER_PASSWORD")
DATASETTRACKER_SHARE_IDENTIFIER = os.getenv("DATASETTRACKER_SHARE_IDENTIFIER")

assert type(SCICAT_INGEST_BASE_FOLDER) == str and len(SCICAT_INGEST_BASE_FOLDER) != 0
assert type(SCICAT_INGEST_URL) == str and len(SCICAT_INGEST_URL) != 0
assert type(SCICAT_INGEST_USERNAME) == str and len(SCICAT_INGEST_USERNAME) != 0
assert type(SCICAT_INGEST_PASSWORD) == str and len(SCICAT_INGEST_PASSWORD) != 0
assert type(SCICAT_INGEST_SPEC) == str and len(SCICAT_INGEST_SPEC) != 0

dataset_path = Path(SCICAT_INGEST_BASE_FOLDER, SCICAT_INGEST_SUBFOLDER).resolve()
dataset_files = [p for p in map(Path, glob.iglob(str(dataset_path) + "/**", recursive=True))]

ingest(
    dataset_path=dataset_path,
    dataset_files=dataset_files,
    ingester_spec=SCICAT_INGEST_SPEC,
    owner_username=SCICAT_INGEST_OWNER_USERNAME,
    scicat_url=SCICAT_INGEST_URL,
    scicat_username=SCICAT_INGEST_USERNAME,
    scicat_password=SCICAT_INGEST_PASSWORD,
    datasettracker_url=DATASETTRACKER_URL,
    datasettracker_username=DATASETTRACKER_USERNAME,
    datasettracker_password=DATASETTRACKER_PASSWORD,
    datasettracker_share_identifier=DATASETTRACKER_SHARE_IDENTIFIER
)
