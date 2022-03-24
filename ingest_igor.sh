# NOTE: Run this in the same directory as the ingest.py file AND run it while you are in the virtual environment



# Change working directory to the location of the file only works with Linux, not Windows or Mac
cd "$(dirname "$0")"


if [[ ! -f .env ]] ; then
    echo 'File ".env" is not there, aborting.'
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

for d in "$ROOT_FOLDER"/*/dat/ ; do
    # if directory exists 
    if [ -d "$d" ]; then
        # Will not run if no directories are available
        python ingest.py --username "$USERNAME" --password "$PASSWORD" scicat_beamline/ingestors/als_11012_igor.py "$d" "$INGEST_USER" "$SCICAT_URL"
    fi
done