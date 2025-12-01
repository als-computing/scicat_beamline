This repository stores scripts used at various beamlines to ingest data into Scicat.

The primary way to invoke these is through the scripts in the root folder, e.g. `ingest_script.py`

You can configure necessary settings by creating an `.env` file like so:

```
ROOT_FOLDER=/home/j/programming/work/October_data
SCICAT_URL=https://mwet.lbl.gov/api/v3
USERNAME=username
INGEST_USER=username
PASSWORD=password
INGEST_SPEC=als_11012_igor OR als_11012_scattering OR als_11012_nexafs
```

If you're developing locally, install dependencies and work in a virtual environment like so:

```
uv venv --python 3.12 # or greater
source .venv/bin/activate
uv pip install -e .
```