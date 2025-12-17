from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import List

import numpy
import pandas
from pyscicat.client import ScicatClient
from pyscicat.model import DatasetType, OrigDatablock, Ownable, RawDataset

from common_ingester_utils import (Issue, add_to_sci_metadata_from_bad_headers,
                                   create_data_file, get_file_mod_time,
                                   get_file_size)

ingest_spec = "als_11012_nexafs"


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a nexafs folder"
    now_str = datetime.isoformat(datetime.utcnow()) + "Z"
    ownable = Ownable(
        createdBy="dylan",
        updatedBy="dylan",
        updatedAt=now_str,
        createdAt=now_str,
        ownerGroup="MWET",
        accessGroups=["MWET", "ingestor"],
    )

    scientific_metadata = OrderedDict()

    lines_to_skip = 0
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith("Time of"):
                lines_to_skip = line_num - 1
                break

    add_to_sci_metadata_from_bad_headers(
        scientific_metadata,
        file_path,
        when_to_stop=lambda line: line.startswith("Time of"),
    )

    table = pandas.read_table(file_path, skiprows=lines_to_skip)
    # https://stackoverflow.com/a/54403705/
    table = table.replace({numpy.nan: None})

    scientific_metadata.update(table.to_dict(orient="list"))

    sample_name = file_path.name

    description = sample_name[:-4].replace("_", " ")
    dataset = RawDataset(
        owner="Cameron McKay",
        contactEmail="cbabay1993@gmail.com",
        creationLocation="ALS 11.0.1.2",
        datasetName=sample_name,
        type=DatasetType.raw,
        instrumentId="11.0.1.2",
        proposalId="unknown",
        dataFormat="ALS BCS",
        principalInvestigator="Lynn Katz",
        sourceFolder=file_path.parent.as_posix(),
        scientificMetadata=scientific_metadata,
        sampleId=sample_name,
        isPublished=False,
        description=description,
        keywords=["NEXAFS", "11.0.1.2", "ALS", "absorption", "11.0.1.2 NEXAFS"],
        creationTime=get_file_mod_time(file_path),
        **ownable.model_dump(),
    )

    dataset_id = scicat_client.datasets_create(dataset)

    datafiles = [create_data_file(file_path)[0]]

    data_block = OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=get_file_size(file_path),
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
