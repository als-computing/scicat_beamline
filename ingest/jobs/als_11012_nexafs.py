from datetime import datetime
import os
from pathlib import Path
import pandas

from dotenv import load_dotenv

from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size
)

from pyscicat.model import (
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Ownable
  )

load_dotenv('.env')

SCICAT_BASEURL = os.getenv('SCICAT_BASEURL')
SCICAT_INGEST_USER = os.getenv('SCICAT_INGEST_USER')
SCICAT_INGEST_PASSWORD = os.getenv('SCICAT_INGEST_PASSWORD')


def ingest(nexafs_file_path: Path) -> str:
    "Ingest a folder of 11012 scattering folders"
    now_str = datetime.isoformat(datetime.utcnow()) + "Z"
    ownable = Ownable(
            owner="MWET",
            contactEmail="dmcreynolds@lbl.gov",
            createdBy="dylan",
            updatedBy="dylan",
            updatedAt=now_str,
            createdAt=now_str,
            ownerGroup="MWET",
            accessGroups=["MWET", "ingestor"])

    ingestor = ScicatClient()
    headers = []
    lines_to_skip = 0
    with open(nexafs_file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith('Time of'):
                lines_to_skip = line_num - 1
                break
            headers.append(line.rstrip())

    table = pandas.read_table(nexafs_file_path, skiprows=lines_to_skip)
    scientific_metadata = {}
    scientific_metadata["headers"] = headers
    scientific_metadata.update(table.to_dict(orient='list'))

    folder_size = get_file_size(nexafs_file_path)
    sample_name = nexafs_file_path.name

    description = sample_name[:-4].replace("_", " ")
    appended_keywords = description.split()
    dataset = Dataset(
        owner="test",
        contactEmail="cbabay1993@gmail.com",
        creationLocation="ALS11021",
        datasetName=sample_name,
        type=DatasetType.raw,
        instrumentId="11012",
        proposalId="unknown",
        dataFormat="BCS",
        principalInvestigator="Lynn Katz",
        sourceFolder=nexafs_file_path.as_posix(),
        size=folder_size,
        scientificMetadata=scientific_metadata,
        sampleId=sample_name,
        isPublished=False,
        description=description,
        keywords=["scattering", "rsoxs", "11.0.1.2", "ccd"] + appended_keywords,
        creationTime=get_file_mod_time(nexafs_file_path),
        **ownable.dict())

    dataset_id = ingestor.upload_raw_dataset(dataset)

    datafiles = [DataFile(
                path=nexafs_file_path.name,
                size=get_file_size(nexafs_file_path),
                time=get_file_mod_time(nexafs_file_path),
                type="RawDatasets"
            )]

    data_block = Datablock(
        datasetId=dataset_id,
        size=get_file_size(nexafs_file_path),
        dataFileList=datafiles,
        **ownable.dict()
    )
    ingestor.upload_datablock(data_block)
    return dataset_id


if __name__ == "__main__":
    folder = Path('/home/j/programming/work/Oct_2021_scattering/Nexafs')
    for path in folder.iterdir():
        print(path)
        if not path.is_file():
            continue
        try:
            dataset_id = ingest(path)
            print(f"Ingested {path} as {dataset_id}.")
        except Exception as e:
            print('ERROR:')
            print(e)
            print(f"Error ingesting {path} with {e}")
            raise e
