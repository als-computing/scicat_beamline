from datetime import datetime
from pathlib import Path
from typing import Dict, List, OrderedDict

import pandas
from pyscicat.client import (
    ScicatClient,
    encode_thumbnail,
)
from pyscicat.model import (
    Attachment,
    Dataset,
    DatasetType,
    DerivedDataset,
    OrigDatablock,
    Ownable,
)

from scicat_beamline.common_ingester_utils import (
    Issue,
    create_data_files_list,
    glob_non_hidden_in_folder,
)

ingest_spec = "als_11012_igor"

"""A DatasetReader for Igor generated dat files
Reader exepects a folder that contains dat files as
well as a jpeg files of graphs. Jpeg files will be ingested
as attachments/thumbnails.

Scientific Metadata is built as a dictionary of energy value keys each with a dict of the
associated dat headers
"""


# def create_data_files(folder: Path) -> Tuple[List[DataFile], int]:
#     "Collects all files"
#     datafiles = []
#     size = 0
#     for file in folder.iterdir():
#         datafile = DataFile(
#             path=file.name,
#             size=get_file_size(file),
#             time=get_file_mod_time(file),
#             type="DerivedDatasets",
#         )
#         datafiles.append(datafile)
#         size += get_file_size(file)
#     return datafiles, size


def create_data_block(datafiles, dataset_id, ownable: Ownable, size) -> OrigDatablock:
    "Creates a datablock of all files"

    return OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=size,
        dataFileList=datafiles,
        **ownable.model_dump(),
    )


def create_dataset(
    scicat_client: ScicatClient, folder: Path, ownable: Ownable
) -> Dataset:
    "Creates a dataset object"
    datasetName = folder.parent.name + "_IGOR_ANALYSIS"
    inputDatasetName = folder.parent.name
    a = scicat_client.get_datasets({"datasetName": inputDatasetName})
    ai_file_name = next(glob_non_hidden_in_folder(folder.parent, "*.txt")).name[:-7]

    sci_md = create_scientific_metadata(folder)
    creationTime = list(sci_md.values())[0]["Processed on"]

    description = ai_file_name.replace("_", " ")
    description = description.replace("-", " ")
    description = (
        inputDatasetName.replace("_", " ").replace("-", " ")
        + " "
        + description
        + " igor analysis"
    )
    dataset = DerivedDataset(
        investigator="Cameron McKay",
        inputDatasets=[a[0]["pid"]],
        usedSoftware=["Igor", "Irena", "Nika"],
        owner="Cameron McKay",
        contactEmail="cbabay1993@gmail.com",
        datasetName=datasetName,
        type=DatasetType.derived,
        instrumentId="11.0.1.2",
        proposalId="unknown",
        dataFormat="dat",
        # principalInvestigator="Lynn Katz",
        sourceFolder=folder.as_posix(),
        scientificMetadata=sci_md,
        sampleId=datasetName,
        isPublished=False,
        description=description,
        keywords=[
            "scattering",
            "RSoXS",
            "11.0.1.2 RSoXS" "11.0.1.2",
            "ALS",
            "igor",
            "analysis",
            "Irena",
            "Nika",
        ],
        creationTime=creationTime,
        **ownable.model_dump(),
    )
    return dataset


def create_attachment(file: Path, dataset_id: str, ownable: Ownable) -> Attachment:
    "Creates a thumbnail jpg"
    return Attachment(
        datasetId=dataset_id,
        thumbnail=encode_thumbnail(file),
        caption="scattering image",
        **ownable.model_dump(),
    )


def create_scientific_metadata(folder: Path) -> Dict:
    """Generate a json dict of scientific metadata
    by reading each dat file header

    Args:
        folder (Path):  folder in which to scan fits files
    """
    dat_filenames = glob_non_hidden_in_folder(folder, "*.dat")
    sci_metadata = {}
    column_converters = {0: lambda string: string.strip("#").strip()}
    for dat_filename in dat_filenames:
        headers = None
        counter = 0
        with open(dat_filename) as dat_file:
            # Count the number of lines we will parse
            for line in dat_file:
                if not line.isspace() and line.strip()[0] != "#":
                    break
                counter += 1
            dat_file.seek(0)
            headers = (
                pandas.read_csv(
                    dat_file,
                    index_col=[0],
                    sep="=",
                    nrows=counter,
                    header=None,
                    converters=column_converters,
                    skip_blank_lines=True,
                )
                .squeeze("columns")
                .dropna()
            )

        # Re order headers in a new dict
        ordered_headers_dict = OrderedDict()

        # We are essentially swapping the top and bottom section of the headers
        atBottomHeaders = False
        for key, value in headers.items():
            if key == "Processed on":
                atBottomHeaders = True
            if atBottomHeaders is True:
                ordered_headers_dict[key] = value

        for key, value in headers.items():
            if key == "Processed on":
                break
            ordered_headers_dict[key] = value

        energy = ordered_headers_dict["Nika_XrayEnergy"]

        energy = energy.replace(".", "_")

        if energy not in sci_metadata:
            sci_metadata[energy] = ordered_headers_dict
        else:
            i = 1
            while True:
                if f"{energy} ({i})" not in sci_metadata:
                    sci_metadata[f"{energy} ({i})"] = ordered_headers_dict
                i += 1
    sci_metadata = OrderedDict(sorted(sci_metadata.items(), key=lambda t: t[0]))
    return sci_metadata


# def ingest(folder: Path) -> Tuple[str, List[Issue]]:
def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a folder of 11012 Igor analysis"
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

    datafiles, size = create_data_files_list(file_path)
    dataset = create_dataset(scicat_client, file_path, ownable)
    dataset_id = scicat_client.upload_derived_dataset(dataset)
    # TODO: ensure that all jpg files are uploaded as attachments
    # And maybe pngs
    jpg_files = list(glob_non_hidden_in_folder(file_path, "*.jpg"))
    if len(list(jpg_files)) > 0:
        thumbnail = create_attachment(jpg_files[0], dataset_id, ownable)
        scicat_client.datasets_attachment_create(
            thumbnail, datasetType="DerivedDatasets"
        )

    data_block = create_data_block(datafiles, dataset_id, ownable, size)
    scicat_client.datasets_origdatablock_create(dataset_id, data_block)

    return dataset_id


# if __name__ == "__main__":
#     from pprint import pprint
#     folder = Path('/home/j/programming/work/Oct_2021_scattering/CCD')
#     for path_obj in folder.iterdir():
#         print(f"Folder: {path_obj}")
#         if not path_obj.is_dir():
#             continue
#         dat_folder = path_obj / "dat"
#         if dat_folder.exists() and dat_folder.is_dir() and len(os.listdir(dat_folder)) > 0:
#             dataset_id, issues = ingest(dat_folder)
#             print(f"Ingested {path_obj} as {dataset_id}. Issues:")
#             pprint(issues)
