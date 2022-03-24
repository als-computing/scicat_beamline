import glob
from importlib.resources import path
from dotenv import load_dotenv
import os
from ingest import ingest
import pathlib
load_dotenv()



# Examples of the vars from the .env file
#ROOT_FOLDER=/home/j/programming/work/October_data
#SCICAT_URL=https://mwet.lbl.gov/api/v3
#USERNAME=username
#INGEST_USER=username
#PASSWORD=password
#INGEST_SPEC=als_11012_igor OR als_11012_scattering OR als_11012_nexafs

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

pattern = None
ingestor_location = None
if INGEST_SPEC == "als_11012_igor":
    pattern = f"{ROOT_FOLDER}/*/dat/"
    ingestor_location = pathlib.Path(os.getcwd(), "scicat_beamline/ingestors/als_11012_igor.py")
elif INGEST_SPEC == "als_11012_scattering":
    pattern = f"{ROOT_FOLDER}/*/"
    ingestor_location = pathlib.Path(os.getcwd(), "scicat_beamline/ingestors/als_11012_scattering.py")
elif INGEST_SPEC == f"als_11012_nexafs":
    pattern = f"{ROOT_FOLDER}/Nexafs/*"
    ingestor_location = pathlib.Path(os.getcwd(), "scicat_beamline/ingestors/nexafs.py")
else:
    raise Exception("Environment variable 'INGEST_SPEC' is invalid.")


ingest_files_iter = glob.iglob(pattern)

for ingest_file_str in ingest_files_iter:
    ingest_file_path = pathlib.Path(ingest_file_str)
    if ingest_file_path.exists():
        print(ingest_file_path)
        ingest(ingestor_location, ingest_file_path, INGEST_USER, SCICAT_URL, token=None, username=USERNAME, password=PASSWORD)