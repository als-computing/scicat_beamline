import glob
import os
import pathlib

from dotenv import load_dotenv

from scicat_beamline.ingest import ingest

load_dotenv()

# Examples of the vars from the .env file
# ROOT_FOLDER=/home/j/programming/work/October_data
# SCICAT_URL=https://mwet.lbl.gov/api/v3
# USERNAME=username
# INGEST_USER=username
# PASSWORD=password
# INGEST_SPEC=als_11012_igor OR als_11012_scattering OR als_11012_nexafs

ROOT_FOLDER = os.getenv("ROOT_FOLDER")
SCICAT_URL = os.getenv("SCICAT_URL")
USERNAME = os.getenv("USERNAME")
INGEST_USER = os.getenv("INGEST_USER")
PASSWORD = os.getenv("PASSWORD")
INGEST_SPEC = os.getenv("INGEST_SPEC")

assert type(ROOT_FOLDER) == str and len(ROOT_FOLDER) != 0
assert type(SCICAT_URL) == str and len(SCICAT_URL) != 0
assert type(USERNAME) == str and len(USERNAME) != 0
assert type(PASSWORD) == str and len(PASSWORD) != 0
assert type(INGEST_USER) == str and len(INGEST_USER) != 0
assert type(INGEST_SPEC) == str and len(INGEST_SPEC) != 0

ROOT_FOLDER = pathlib.Path(ROOT_FOLDER)

ingest(
    INGEST_SPEC,
    ROOT_FOLDER,
    INGEST_USER,
    SCICAT_URL,
    username=USERNAME,
    password=PASSWORD,
)
