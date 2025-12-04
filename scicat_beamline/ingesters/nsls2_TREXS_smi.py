import json
import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import numpy as np
from PIL import Image, ImageOps

from pyscicat.client import (ScicatClient, encode_thumbnail, get_file_mod_time,
                             get_file_size)
from pyscicat.model import (Attachment, DataFile, Dataset, DatasetType,
                            OrigDatablock, Ownable, RawDataset)
from scicat_beamline.common_ingester_utils import (Issue,
                                                  create_data_files_list,
                                                  glob_non_hidden_in_folder)

ingest_spec = "nsls2_trexs_smi"


class TREXSNsls2SMIReader:
    """A DatasetReader for reading nsls2 TREXS datasets.
    Reader expects a folder that contains multiple child folders
    with tiff files."""

    dataset_id: str = None
    _issues = []

    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable

    # def create_data_files(self) -> Tuple[List[DataFile], int]:
    #     "Collects all files"
    #     datafiles = []
    #     size = 0
    #     for file in self._folder.iglob():
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

    def create_dataset(self, creationTime) -> Dataset:
        "Creates a dataset object"
        proposalId = self._folder.name.split("_")[
            0
        ]  # TODO: change to make more general
        print(self._folder)

        dataset = RawDataset(
            owner="Matt Landsman",  # owner=metadata_dict["user_name"]
            contactEmail="mrlandsman@lbl.gov",  # contactEmail=metadata_dict["user_email"]
            creationLocation="NSLS-II SMI TREXS",
            datasetName=self._folder.name,  # TODO: change to make more general
            type=DatasetType.raw,
            instrumentId="SMI TREXS",
            proposalId=proposalId,  # TODO: change to make more general
            dataFormat="NSLS-II",
            principalInvestigator="Lynn Katz",
            sourceFolder=self._folder.as_posix(),
            # scientificMetadata=metadata_dict,
            sampleId="",
            isPublished=False,
            description=self._folder.name,
            keywords=["TREXS", "nsls-ii", "SMI", "scattering", proposalId, "SMI TREXS"],
            creationTime=creationTime,
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
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> Tuple[str, List[Issue]]:
    "Ingest a TREXS folder"
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
    reader = TREXSNsls2SMIReader(file_path, ownable)
    issues: List[Issue] = []

    png_files = list(glob_non_hidden_in_folder(file_path, "*/**.png"))
    if len(list(png_files)) == 0:
        # collect all tiff and tif files in sub folders of the root folder, but no further subfolders
        tiff_filenames = sorted(glob_non_hidden_in_folder(file_path, "*/**.tiff"))
        tiff_filenames.extend(sorted(glob_non_hidden_in_folder(file_path, "*/**.tif")))
        tiff_filename = tiff_filenames[len(tiff_filenames) // 2]
        image_data = Image.open(tiff_filename)
        image_data = np.array(image_data)
        build_thumbnail(
            image_data, tiff_filename.name[:-5], tiff_filename.absolute().parent
        )
        png_files = list(glob_non_hidden_in_folder(file_path, "*/**.png"))

    datafiles, size = create_data_files_list(file_path, recursive=True)
    creationTime = get_file_mod_time(Path(str(file_path) + "/" + datafiles[0].path))
    dataset = reader.create_dataset(creationTime)
    dataset_id = scicat_client.datasets_create(dataset)
    reader.dataset_id = dataset_id

    thumbnail = reader.create_attachment(png_files[0])
    scicat_client.datasets_attachment_create(thumbnail)

    data_block = reader.create_data_block(datafiles, size)
    scicat_client.datasets_origdatablock_create(dataset_id, data_block)
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
