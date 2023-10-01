from collections import OrderedDict
from datetime import datetime
import os
from pathlib import Path
from typing import List
from dateutil import parser


from pyscicat.client import (
    ScicatClient,
    get_file_size,
)

from pyscicat.model import (
    Attachment,
    OrigDatablock,
    RawDataset,
    DatasetType,
    Ownable,
)
from scicat_beamline.ingestors.common_ingestor_code import create_data_file
from scicat_beamline.scicat_utils import encode_image_2_thumbnail

from scicat_beamline.utils import Issue

ingest_spec = 'als_632_nexafs'


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

    scientific_metadata = OrderedDict()
    scientific_metadata["absorbing element"] = get_absorbing_element_march2023(file_path)

    creation_time = None
    if os.path.isdir(file_path):
        print(f'Skipping \"{file_path}\" because it is a folder.')
        return

    if "ctrl" in file_path.stem:
        print(f'Skipping \"{file_path}\" because it is a ctrl file.')
        return

    with open(file_path) as nexafs_file:
        creation_time = parser.parse(nexafs_file.readline())

    filename = file_path.name

    sampleid = get_sample_id_march2023(filename)
    sample_keyword = get_sample_keyword_march2023(filename)

    description = file_path.stem.replace("_", " ")
    dataset = RawDataset(
        owner="Cameron McKay",
        contactEmail="cbabay1993@gmail.com",
        creationLocation="ALS 6.3.2",
        datasetName=filename,
        type=DatasetType.raw,
        instrumentId="6.3.2",
        proposalId=proposal_id_march2023(),
        dataFormat="ALS BCS",
        principalInvestigator="Greg Su",
        sourceFolder=file_path.parent.as_posix(),
        scientificMetadata=scientific_metadata,
        sampleId=sampleid,
        isPublished=False,
        description=description,
        keywords=["NEXAFS", "6.3.2", "ALS", "absorption", sample_keyword],
        creationTime=str(creation_time),
        **ownable.dict(),
    )

    graph_folder = file_path.parent/"nexafs_graphs"

    graph_txt = next(graph_folder.glob("*"+file_path.stem+"*.txt"))
    graph_png = next(graph_folder.glob("*"+file_path.stem+"*.png"))
    ctrl_file = get_ctrl_file_march2023(file_path)

    datafiles = [
        create_data_file(file_path)[0],
        create_data_file(graph_txt)[0],
        create_data_file(ctrl_file)[0]
    ]

    encoded_image = encode_image_2_thumbnail(graph_png)

    dataset_id = scicat_client.upload_raw_dataset(dataset)

    data_block = OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=get_file_size(file_path)+get_file_size(graph_txt)+get_file_size(ctrl_file),
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(data_block)
    attachment = Attachment(
        datasetId=dataset_id,
        thumbnail=encoded_image,
        caption="NEXAFS plot",
        ** ownable.dict(),
    )
    scicat_client.upload_attachment(attachment)
    return dataset_id, issues


# TODO: add new functions and change the references in the code to the new ones for each dataset we upload
def proposal_id_march2023():
    return "ALS-11579"


def get_absorbing_element_march2023(filepath):
    return filepath.stem.split("_")[1]


def get_ctrl_file_march2023(filepath: Path) -> Path:
    element_pattern = "_" + get_absorbing_element_march2023(filepath) + "_"
    ctrl_files = list(filepath.parent.glob("ctrl"+element_pattern+"*"))
    if len(ctrl_files) == 0:
        raise Exception("Could not find corresponding ctrl file for: "+str(filepath))
    if len(ctrl_files) > 1:
        raise Exception("Found more than one ctrl files for: "+str(filepath))
    return ctrl_files[0]


def get_sample_id_march2023(filename: str):
    parts = filename.split("_", 1)
    return parts[0]


def get_sample_keyword_march2023(filename):
    keywords = ["algca", "bp-iv", "ctrl", "CuNP", "mnom", "psp4vp", "p4vp", "peo", "polystyrene", "srfa", "srha",
                "xle"]
    for keyword in keywords:
        if keyword in filename:
            return keyword
    raise ValueError(f'Could not find a keyword in the file name: {filename}.')

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
