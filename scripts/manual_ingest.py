import os
import pathlib

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

assert type(SCICAT_INGEST_BASE_FOLDER) == str and len(SCICAT_INGEST_BASE_FOLDER) != 0
assert type(SCICAT_INGEST_URL) == str and len(SCICAT_INGEST_URL) != 0
assert type(SCICAT_INGEST_USERNAME) == str and len(SCICAT_INGEST_USERNAME) != 0
assert type(SCICAT_INGEST_PASSWORD) == str and len(SCICAT_INGEST_PASSWORD) != 0
assert type(SCICAT_INGEST_OWNER_USERNAME) == str and len(SCICAT_INGEST_OWNER_USERNAME) != 0
assert type(SCICAT_INGEST_SPEC) == str and len(SCICAT_INGEST_SPEC) != 0

dataset_path = pathlib.Path(SCICAT_INGEST_BASE_FOLDER, SCICAT_INGEST_SUBFOLDER).resolve()

ingest(
    ingester_spec=SCICAT_INGEST_SPEC,
    dataset_path=dataset_path,
    owner_username=SCICAT_INGEST_OWNER_USERNAME,
    base_url=SCICAT_INGEST_URL,
    username=SCICAT_INGEST_USERNAME,
    password=SCICAT_INGEST_PASSWORD,
)
