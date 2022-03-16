from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import pandas

from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Issue,
    Ownable,
)


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: str,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
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
        accessGroups=["MWET", "ingestor"],
    )

    issues: List[Issue] = []
    ingestor = scicat_client(issues)

    headers = []
    lines_to_skip = 0
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith("Time of"):
                lines_to_skip = line_num - 1
                break
            headers.append(line.rstrip())

    table = pandas.read_table(file_path, skiprows=lines_to_skip)
    scientific_metadata = {}
    scientific_metadata["headers"] = headers
    scientific_metadata.update(table.to_dict(orient="list"))

    folder_size = get_file_size(file_path)
    sample_name = file_path.name

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
        sourceFolder=file_path.as_posix(),
        size=folder_size,
        scientificMetadata=scientific_metadata,
        sampleId=sample_name,
        isPublished=False,
        description=description,
        keywords=["scattering", "rsoxs", "11.0.1.2", "ccd"] + appended_keywords,
        creationTime=get_file_mod_time(file_path),
        **ownable.dict(),
    )

    dataset_id = ingestor.upload_raw_dataset(dataset)

    datafiles = [
        DataFile(
            path=file_path.name,
            size=get_file_size(file_path),
            time=get_file_mod_time(file_path),
            type="RawDatasets",
        )
    ]

    data_block = Datablock(
        datasetId=dataset_id,
        size=get_file_size(file_path),
        dataFileList=datafiles,
        **ownable.dict(),
    )
    ingestor.upload_datablock(data_block)
    return dataset_id, issues


if __name__ == "__main__":
    from pprint import pprint

    folder = Path("/home/j/programming/work/Oct_2021_scattering/Nexafs")
    for path in folder.iterdir():
        print(path)
        if not path.is_file():
            continue
        try:
            dataset_id, issues = ingest(path)
            print(f"Ingested {path} as {dataset_id}. Issues:")
            pprint(issues)
        except Exception as e:
            print("ERROR:")
            print(e)
            print(f"Error ingesting {path} with {e}")
            raise e
