from datetime import datetime
import glob
from pathlib import Path
from typing import List
import numpy
import pandas
import os

from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    Datablock,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
)

from scicat_beamline.utils import Issue

ingest_spec = 'nsls2_nexafs_sst1'


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a folder of nsls-ii sst-1 nexafs files"
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

    headers = []
    lines_to_skip = 0
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith("----"):
                lines_to_skip = line_num
                break
            if line.isspace():
                continue
            headers.append(line.rstrip())

    table = pandas.read_table(file_path, skiprows=lines_to_skip, delim_whitespace=True)
    metadata_table_filename = glob.glob(str(file_path.parent) + "/*.csv")[0]
    metadata_table = pandas.read_csv(metadata_table_filename)
    metadata_row = None

    for idx, entry in enumerate(metadata_table["file_name"]):
        entry = entry.split(",")
        for nexafs_file_name in entry:
            nexafs_file_name = nexafs_file_name.strip()
            if nexafs_file_name == file_path.name:
                metadata_row = metadata_table.iloc[idx]

    # https://stackoverflow.com/a/54403705/
    table = table.replace({numpy.nan: None})
    scientific_metadata = {}
    scientific_metadata["headers"] = headers
    scientific_metadata.update(table.to_dict(orient="list"))
    appended_keywords = []
    if metadata_row is not None:
        scientific_metadata["incident_angle"] = str(metadata_row["incident_angle"])
        scientific_metadata["saf_id"] = str(metadata_row["saf_id"])
        scientific_metadata["measurement"] = metadata_row["measurement"]
        scientific_metadata["project_name"] = metadata_row["project_name"]
        appended_keywords = [str(metadata_row[key]) for key in ["measurement", "element_edge", "sample_id", "proposal_id", "saf_id", "institution", "project_name", "sample_description"]] + [file_path.name]

    parent_folder = file_path.parent.absolute()
    log_file_path_strings = glob.glob(str(parent_folder) + '/*.log')

    file_size = 0

    for log_path_string in log_file_path_strings:
        file_size += get_file_size(Path(log_path_string))

    file_size += get_file_size(file_path)
    file_name = file_path.name

    # description = file_name.replace("_", " ")'
    dataset = None
    if metadata_row is not None:
        dataset = RawDataset(
            owner=metadata_row["sample_owner"],
            contactEmail=metadata_row["owner_email"],
            creationLocation="nsls-ii SST-1 NEXAFS",
            datasetName=file_name,  # + "_" + metadata_row["element_edge"],
            type=DatasetType.raw,
            instrumentId="SST-1 NEXAFS",
            proposalId=metadata_row["proposal_id"],
            dataFormat="",
            principalInvestigator=metadata_row["PI"],
            sourceFolder=file_path.as_posix(),
            size=file_size,
            scientificMetadata=scientific_metadata,
            sampleId=metadata_row["sample_id"],
            isPublished=False,
            description=metadata_row["sample_description"] + ". " + metadata_row["sample_description.1"],
            keywords=["nexafs", "nsls-ii", "ccd", "SST-1"] + appended_keywords,
            creationTime=get_file_mod_time(file_path),
            **ownable.dict(),
        )
    else:
        dataset = RawDataset(
            owner="Matt Landsman",
            contactEmail="mrlandsman@lbl.gov",
            creationLocation="nsls-ii SST-1 NEXAFS",
            datasetName=file_name,
            type=DatasetType.raw,
            instrumentId="SST-1 NEXAFS",
            proposalId="GU-309898",
            dataFormat="",
            principalInvestigator="Greg Su",
            sourceFolder=file_path.as_posix(),
            size=file_size,
            scientificMetadata=scientific_metadata,
            sampleId="",
            isPublished=False,
            description=file_name.replace("_", " "),
            keywords=["nexafs", "nsls-ii", "ccd", "SST-1"] + appended_keywords,
            creationTime=get_file_mod_time(file_path),
            **ownable.dict(),
        )

    dataset_id = scicat_client.upload_raw_dataset(dataset)

    log_datafiles = []
    for log_path_str in log_file_path_strings:
        log_path_obj = Path(log_path_str)
        log_datafiles.append(
            DataFile(
                path=log_path_obj.name,
                size=get_file_size(log_path_obj),
                time=get_file_mod_time(log_path_obj),
                type="RawDatasets"
            )
        )

    datafiles = [
        DataFile(
            path=file_path.name,
            size=get_file_size(file_path),
            time=get_file_mod_time(file_path),
            type="RawDatasets",
        ),
        *log_datafiles
    ]

    data_block = Datablock(
        datasetId=dataset_id,
        size=get_file_size(file_path),
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(data_block)
    return dataset_id, issues


# if __name__ == "__main__":
#     from pprint import pprint

#     folder = Path("/home/j/programming/work/Oct_2021_scattering/Nexafs")
#     for path in folder.iterdir():
#         print(path)
#         if not path.is_file():
#             continue
#         try:
#             dataset_id, issues = ingest(path)
#             print(f"Ingested {path} as {dataset_id}. Issues:")
#             pprint(issues)
#         except Exception as e:
#             print("ERROR:")
#             print(e)
#             print(f"Error ingesting {path} with {e}")
#             raise e
