from datetime import datetime
from pathlib import Path
from typing import Dict, List, OrderedDict
import pandas

from pyscicat.client import (
    ScicatClient,
    encode_thumbnail,
    get_file_mod_time,
    get_file_size,
)
from pyscicat.model import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    DerivedDataset,
    Ownable,
)


from scicat_beamline.utils import Issue

ingest_spec = "als_11012_igor"

"""A DatasetReader for Igor generated dat files
Reader exepects a folder that contains dat files as
well as a jpeg files of graphs. Jpeg files will be ingested
as attachments/thumbnails.

Scientific Metadata is built as a dictionary of energy value keys each with a dict of the
associated dat headers
"""


def create_data_files(folder: Path) -> List[DataFile]:
    "Collects all files"
    datafiles = []
    for file in folder.iterdir():
        datafile = DataFile(
            path=file.name,
            size=get_file_size(file),
            time=get_file_mod_time(file),
            type="DerivedDatasets",
        )
        datafiles.append(datafile)
    return datafiles


def create_data_block(folder, dataset_id, ownable: Ownable) -> Datablock:
    "Creates a datablock of all files"
    datafiles = create_data_files(folder)

    return Datablock(
        datasetId=dataset_id,
        size=get_file_size(folder),
        dataFileList=datafiles,
        **ownable.dict(),
    )


def create_dataset(
    scicat_client: ScicatClient, folder: Path, ownable: Ownable
) -> Dataset:
    "Creates a dataset object"
    folder_size = get_file_size(folder)
    datasetName = folder.parent.name + "_IGOR_ANALYSIS"
    inputDatasetName = folder.parent.name
    a = scicat_client.get_datasets({"datasetName": inputDatasetName})
    ai_file_name = next(folder.parent.glob("*.txt")).name[:-7]

    description = ai_file_name.replace("_", " ")
    description = description.replace("-", " ")
    appended_keywords = description.split()
    dataset = DerivedDataset(
        investigator="Cameron McKay",
        inputDatasets=[a[0]["pid"]],
        usedSoftware=["Igor", "Irena", "Nika"],
        owner="test",
        contactEmail="cbabay1993@gmail.com",
        # creationLocation="ALS11012",
        datasetName=datasetName,
        type=DatasetType.derived,
        instrumentId="11012",
        proposalId="unknown",
        dataFormat="dat",
        # principalInvestigator="Lynn Katz",
        sourceFolder=folder.as_posix(),
        size=folder_size,
        scientificMetadata=create_scientific_metadata(folder),
        sampleId=datasetName,
        isPublished=False,
        description=description,
        keywords=["scattering", "rsoxs", "11.0.1.2", "als", "ccd", "igor", "analysis"]
        + appended_keywords,
        creationTime=get_file_mod_time(folder),
        **ownable.dict(),
    )
    return dataset


def create_attachment(file: Path, dataset_id: str, ownable: Ownable) -> Attachment:
    "Creates a thumbnail jpg"
    return Attachment(
        datasetId=dataset_id,
        thumbnail=encode_thumbnail(file),
        caption="scattering image",
        **ownable.dict(),
    )


def create_scientific_metadata(folder: Path) -> Dict:

    """Generate a json dict of scientific metadata
    by reading each fits file header

    Args:
        folder (Path):  folder in which to scan fits files
    """
    dat_filenames = folder.glob("*.dat")
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
    file_path: str,
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

    issues: List[Issue] = []

    dataset = create_dataset(scicat_client, file_path, ownable)
    dataset_id = scicat_client.upload_derived_dataset(dataset)
    # TODO: ensure that all jpg files are uploaded as attachments
    # And maybe pngs
    jpg_files = list(file_path.glob("*.jpg"))
    if len(list(jpg_files)) > 0:
        thumbnail = create_attachment(jpg_files[0], dataset_id, ownable)
        scicat_client.upload_attachment(thumbnail, datasetType="DerivedDatasets")

    data_block = create_data_block(file_path, dataset_id, ownable)
    scicat_client.upload_datablock(data_block, datasetType="DerivedDatasets")
    return dataset_id, issues


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
