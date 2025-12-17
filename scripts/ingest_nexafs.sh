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

if [ -z "${ROOT_FOLDER}" ] || [ -z "${SCICAT_URL}" ] || [ -z "${USERNAME}" ] || [ -z "${INGEST_USER}" ] || [ -z "${PASSWORD}" ] ; then 
    echo "All .env variables must be defined and not empty strings."
    exit 1
fi

# Examples of the vars from the .env file
#ROOT_FOLDER=/home/j/programming/work/October_data
#SCICAT_URL=https://mwet.lbl.gov/api/v3
#USERNAME=username
#INGEST_USER=username
#PASSWORD=password

# Will not run if no directories are available
python scicat_beamline_ingestion/ingest.py --username "$USERNAME" --password "$PASSWORD" "als_11012_nexafs" "$ROOT_FOLDER" "$INGEST_USER" "$SCICAT_URL"
