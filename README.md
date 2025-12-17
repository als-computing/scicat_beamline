This repository stores code used at various beamlines to ingest data into Scicat.

One can manually invoke these through the `manual_ingest.py` script in the root folder.

You can configure necessary settings by creating an `.env` based on `.env-example` like so:

```
BASE_FOLDER=/home/j/programming/work/October_data
INGEST_SUBFOLDER=733/2025-12/latest
SCICAT_URL=https://dataportal-staging.als.lbl.gov/api/v3
INGEST_USER=datasetIngestor
SCICAT_USERNAME=datasetIngestor
SCICAT_PASSWORD=PASSWORD
INGEST_SPEC=als_11012_igor OR als_11012_scattering OR als_11012_nexafs, etc

# For when we're running inside our "ingest_worker" Docker container
INTERNAL_BASE_FOLDER=/opt/prefect/ingest_folder

# For local testing with Prefect server
PREFECT_API_URL=http://localhost:4200/api
GITHUB_TOKEN=TOKEN
```

If you're developing locally, install dependencies and work in a virtual environment like so:

```
uv venv --python 3.12 # or greater
source .venv/bin/activate
uv pip install -e .
```

Then try running the tests:

```
./testing/setup.sh --clean
python3 testing/create_deployment.py
python3 testing/run_deployment.py
```

And go to http://localhost:4200/runs/ in Chrome. (Safari is too strict with domains to let the front end reach the API)
