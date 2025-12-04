import glob
from datetime import datetime
from pathlib import Path
from typing import List

import numpy
import pandas

from pyscicat.client import ScicatClient, get_file_mod_time, get_file_size
from pyscicat.model import (DataFile, DatasetType, OrigDatablock, Ownable,
                            RawDataset)
from scicat_beamline.common_ingestor_code import (
    Issue, add_to_sci_metadata_from_bad_headers)

ingest_spec = "nsls2_nexafs_sst1"


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

    lines_to_skip = 0
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith("----"):
                lines_to_skip = line_num
                break

    table = pandas.read_table(file_path, skiprows=lines_to_skip, delim_whitespace=True)
    metadata_table_filename = glob.glob(str(file_path.parent) + "/*.csv")[0]
    metadata_table = pandas.read_csv(metadata_table_filename)
    metadata_row = None

    metadata_table = metadata_table.replace({numpy.nan: None})
    for idx, entry in enumerate(metadata_table["file_name"]):
        entry = str(entry).split(",")
        for nexafs_file_name in entry:
            nexafs_file_name = nexafs_file_name.strip()
            if nexafs_file_name == file_path.name:
                metadata_row = metadata_table.iloc[idx]

    # https://stackoverflow.com/a/54403705/
    table = table.replace({numpy.nan: None})

    scientific_metadata = {}
    add_to_sci_metadata_from_bad_headers(
        scientific_metadata,
        file_path,
        when_to_stop=lambda line: line.startswith("----"),
    )
    scientific_metadata.update(table.to_dict(orient="list"))
    appended_keywords = []
    if metadata_row is not None:
        # TODO: before ingestion change the keys used for the keywords depending on how they are labelled in the csv file
        scientific_metadata["incident_angle"] = str(metadata_row["incident_angle"])
        scientific_metadata["saf_id"] = str(metadata_row["saf_id"])
        scientific_metadata["measurement"] = metadata_row["measurement"]
        scientific_metadata["project_name"] = metadata_row["project_name"]

        scientific_metadata["element_edge"] = metadata_row["element_edge"]

        scientific_metadata["institution"] = metadata_row["sample_source"]
        scientific_metadata["scan_id"] = metadata_row["scan_id"]

        scientific_metadata["notes"] = metadata_row["notes"]
        scientific_metadata["x_coordinate"] = metadata_row["x_coordinate"]
        scientific_metadata["bar_location"] = metadata_row["bar_location"]
        scientific_metadata["z_coordinate"] = metadata_row["z_coordinate"]

        def modifyKeyword(key, keyword):
            if key == "saf_id":
                return "SAF " + keyword
            if key == "institution":
                return keyword.lower()
            return keyword

        # TODO: before ingestion change the keys used for the keywords depending on which are available in the csv file
        appended_keywords = [
            modifyKeyword(key, scientific_metadata[key])
            for key in [
                "measurement",
                "saf_id",
                "institution",
                "project_name",
                "sample_name",
            ]
            if scientific_metadata[key] is not None
            and str(scientific_metadata[key]).strip() != ""
        ]

        appended_keywords += [metadata_row["proposal_id"]]

        # Remove empty values that we got from the spreadsheet
        empty_keys = []
        for key, value in scientific_metadata.items():
            if str(value).strip() == "" or value == None:
                empty_keys.append(key)
        for key in empty_keys:
            scientific_metadata.pop(key)

    parent_folder = file_path.parent.absolute()
    log_file_path_strings = glob.glob(str(parent_folder) + "/*.log")

    files_size = 0

    for log_path_string in log_file_path_strings:
        files_size += get_file_size(Path(log_path_string))

    files_size += get_file_size(file_path)
    file_name = file_path.name

    # description = file_name.replace("_", " ")'
    dataset = None
    if metadata_row is not None:
        dataset = RawDataset(
            owner=metadata_row["sample_owner"],
            contactEmail=metadata_row["owner_email"],
            creationLocation="NSLS-II SST-1 NEXAFS",
            datasetName=file_name,  # + "_" + metadata_row["element_edge"],
            type=DatasetType.raw,
            instrumentId="SST-1 NEXAFS",
            proposalId=metadata_row["proposal_id"],
            dataFormat="NSLS-II",
            principalInvestigator=metadata_row["PI"],
            sourceFolder=file_path.parent.as_posix(),
            scientificMetadata=scientific_metadata,
            sampleId=metadata_row["sample_id"],
            isPublished=False,
            description=metadata_row[
                "sample_description"
            ],  # + ". " + metadata_row["sample_description.1"],
            keywords=["NEXAFS", "NSLS-II", "SST-1", "SST-1 NEXAFS"] + appended_keywords,
            creationTime=get_file_mod_time(file_path),
            **ownable.model_dump(),
        )
    else:
        raise Exception("No metadata_row")

    dataset_id = scicat_client.datasets_create(dataset)

    log_datafiles = []
    for log_path_str in log_file_path_strings:
        log_path_obj = Path(log_path_str)
        log_datafiles.append(
            DataFile(
                path=log_path_obj.name,
                size=get_file_size(log_path_obj),
                time=get_file_mod_time(log_path_obj),
            )
        )

    datafiles = [
        DataFile(
            path=file_path.name,
            size=get_file_size(file_path),
            time=get_file_mod_time(file_path),
        ),
        *log_datafiles,
    ]

    data_block = OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=files_size,
        dataFileList=datafiles,
        **ownable.model_dump(),
    )
    scicat_client.datasets_origdatablock_create(dataset_id, data_block)
    return dataset_id


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
