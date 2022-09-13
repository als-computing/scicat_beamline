from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import numpy as np
from PIL import Image, ImageOps
import os
import json


from pyscicat.client import (
    ScicatClient,
    encode_thumbnail,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    Attachment,
    OrigDatablock,
    DataFile,
    Dataset,
    RawDataset,
    DatasetType,
    Ownable,
)
from scicat_beamline.ingestors.common_ingestor_code import create_data_files_list

from scicat_beamline.utils import Issue, glob_non_hidden_in_folder

ingest_spec = "nsls2_rsoxs_sst1"


class ScatteringNsls2Sst1Reader():
    """A DatasetReader for reading nsls2 rsoxs datasets.
    Reader expects a folder that contains the jsonl file as
    well as all tiff files and some png files. Png files will be ingested
    as attachments/thumbnails. Tiff files will be ingested as DataFiles.

    Scientific Metadata is a dictionary that copies the jsonl file
    """

    dataset_id: str = None
    _issues = []

    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable

    # def create_data_files(self) -> Tuple[List[DataFile], int]:
    #     "Collects all files"
    #     datafiles = []
    #     size = 0
    #     for file in self._folder.iterdir():
    #         # We exclude directories within this, directories within will probably be folders of corresponding dat
    #         # files.
    #         if file.name == 'dat':
    #             continue
    #         datafile = DataFile(
    #             path=file.name,
    #             size=get_file_size(file),
    #             time=get_file_mod_time(file),
    #             type="RawDatasets",
    #         )
    #         datafiles.append(datafile)
    #         size += get_file_size(file)
    #     return datafiles, size

    def create_data_block(self, datafiles, size: int) -> OrigDatablock:
        "Creates a datablock of all files"

        return OrigDatablock(
            datasetId=self.dataset_id,
            instrumentGroup="instrument-default",
            size=size,
            dataFileList=datafiles,
            **self._ownable.dict(),
        )

    def create_dataset(self) -> Dataset:
        "Creates a dataset object"
        jsonl_file_path = next(glob_non_hidden_in_folder(self._folder, "*.jsonl"))

        metadata_dict = {}
        with open(jsonl_file_path) as file:
            metadata_dict = json.load(file)[1]

        jsonl_file_name = jsonl_file_path.name[:-6]
        appended_keywords = jsonl_file_name.replace("_", " ").replace("-", " ").split()
        appended_keywords += [metadata_dict["beamline_id"], metadata_dict["project_name"]]

        dataset = RawDataset(
            owner="Matt Landsman",  # owner=metadata_dict["user_name"]
            contactEmail="mrlandsman@lbl.gov",  # contactEmail=metadata_dict["user_email"]
            creationLocation="NSLS-II" + " " + metadata_dict["beamline_id"],
            datasetName=jsonl_file_name,
            type=DatasetType.raw,
            instrumentId=metadata_dict["beamline_id"],
            proposalId=metadata_dict["proposal_id"],
            dataFormat="NSLS-II",
            principalInvestigator="Lynn Katz",
            sourceFolder=self._folder.as_posix(),
            scientificMetadata=metadata_dict,
            sampleId=metadata_dict["sample_id"],
            isPublished=False,
            description=metadata_dict["sample_desc"],
            keywords=["scattering", "rsoxs", "nsls-ii"] + appended_keywords,
            creationTime=str(datetime.fromtimestamp(metadata_dict["time"])),
            **self._ownable.dict(),
        )
        return dataset

    def create_attachment(self, file: Path) -> Attachment:
        "Creates a thumbnail png"
        return Attachment(
            datasetId=self.dataset_id,
            thumbnail=encode_thumbnail(file),
            caption="scattering image",
            **self._ownable.dict(),
        )


# def ingest(folder: Path) -> Tuple[str, List[Issue]]:
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
    reader = ScatteringNsls2Sst1Reader(file_path, ownable)
    issues: List[Issue] = []

    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))
    if len(list(png_files)) == 0:
        tiff_filenames = sorted(glob_non_hidden_in_folder(file_path, "*.tiff"))
        tiff_filenames.extend(glob_non_hidden_in_folder(file_path, "*.tif"))
        tiff_filename = tiff_filenames[len(tiff_filenames) // 2]
        image_data = Image.open(tiff_filename)
        image_data = np.array(image_data)
        build_thumbnail(image_data, tiff_filename.name[:-5], file_path)
    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))

    datafiles, size = create_data_files_list(file_path, excludeCheck=lambda x: x.name == 'dat')
    dataset = reader.create_dataset()
    dataset_id = scicat_client.upload_raw_dataset(dataset)
    reader.dataset_id = dataset_id
    thumbnail = reader.create_attachment(png_files[0])
    scicat_client.upload_attachment(thumbnail)

    data_block = reader.create_data_block(datafiles, size)
    scicat_client.upload_datablock(data_block)
    return dataset_id, issues


def build_thumbnail(fits_data, name, directory):
    log_image = fits_data
    log_image = log_image - np.min(log_image) + 1.001
    log_image = np.log(log_image)
    log_image = 205 * log_image / (np.max(log_image))
    auto_contrast_image = Image.fromarray(log_image.astype("uint8"))
    auto_contrast_image = ImageOps.autocontrast(auto_contrast_image, cutoff=0.1)
    dir = Path(directory)
    filename = name + ".png"
    # file = io.BytesIO()
    file = dir / Path(filename)
    auto_contrast_image.save(file, format="PNG")
    return file


# if __name__ == "__main__":
#     from pprint import pprint

#     folder = Path("/home/j/programming/work/Oct_2021_scattering/CCD")
#     for path in folder.iterdir():
#         print(path)
#         if not path.is_dir():
#             continue
#         try:
#             png_files = list(path.glob("*.png"))
#             if len(png_files) == 0:
#                 fits_filenames = sorted(path.glob("*.fits"))
#                 fits_filename = fits_filenames[len(fits_filenames) // 2]
#                 image_data = fits.getdata(fits_filename, ext=2)
#                 build_thumbnail(image_data, fits_filename.name[:-5], path)

#             dataset_id, issues = ingest(path)
#             print(f"Ingested {path} as {dataset_id}. Issues:")
#             pprint(issues)
#         except Exception as e:
#             print("ERROR:")
#             print(e)
#             print(f"Error ingesting {path} with {e}")
#             raise e
