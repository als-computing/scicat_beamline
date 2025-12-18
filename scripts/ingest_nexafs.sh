# NOTE: Run this from the root of the repository, and ideally in the virtual environment

# Change working directory to the location of the file only works with Linux, not Windows or Mac
cd "$(dirname "$0")"

if [[ ! -f .env ]] ; then
    echo 'File ".env" is missing, aborting.'
    exit 1
fi

# get variables from .env files
set -o allexport
source .env
set +o allexport

if [ -z "${ROOT_FOLDER}" ] || [ -z "${SCICAT_INGEST_URL}" ] || [ -z "${SCICAT_INGEST_USERNAME}" ] || [ -z "${SCICAT_INGEST_OWNER_USERNAME}" ] || [ -z "${SCICAT_INGEST_PASSWORD}" ] ; then 
    echo "All .env variables must be defined and not empty strings."
    exit 1
fi

# Examples of the vars from the .env file
#ROOT_FOLDER=/home/j/programming/work/October_data
#SCICAT_INGEST_URL=https://mwet.lbl.gov/api/v3
#SCICAT_INGEST_USERNAME=username
#SCICAT_INGEST_OWNER_USERNAME=username
#SCICAT_INGEST_PASSWORD=password

# Will not run if no directories are available
python scicat_beamline_ingestion/ingest.py --username "$SCICAT_INGEST_USERNAME" --password "$SCICAT_INGEST_PASSWORD" "als_11012_nexafs" "$ROOT_FOLDER" "$SCICAT_INGEST_OWNER_USERNAME" "$SCICAT_INGEST_URL"
