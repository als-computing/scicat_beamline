import json
import os
from datetime import datetime
from pathlib import Path
from typing import List

import numpy as np
import PyHyperScattering
import xarray as xr
from PIL import Image, ImageOps
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (Attachment, Dataset, DatasetType, OrigDatablock,
                            Ownable, RawDataset)

from scicat_beamline.thumbnails import build_RSoXS_thumb_SST1
from scicat_beamline.utils import (Issue, create_data_files_list,
                                   glob_non_hidden_in_folder)

ingest_spec = "nsls2_rsoxs_sst1"


class ScatteringNsls2Sst1Reader:
    """A DatasetReader for reading nsls2 rsoxs datasets.
    Reader expects a folder that contains the jsonl file as
    well as all tiff files and some png files. Png files will be ingested
    as attachments/thumbnails. Tiff files will be ingested as DataFiles.

    Scientific Metadata is a dictionary that copies the jsonl file

    Note that you need a copy of the primary csv inside and outside of the folder,
    this allows the script to read it from within the folder
    for purposes of uploading to scicat and the PyHyperScattering library to read the
    outside folder. May change this later so we need only outside folder.
    """

    dataset_id: str = None
    _issues = []
    scan_id = ""

    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable
        jsonl_file_path = next(glob_non_hidden_in_folder(self._folder, "*.jsonl"))

        metadata_dict = {}
        with open(jsonl_file_path) as file:
            metadata_dict = json.load(file)[1]

        self.scan_id = metadata_dict["scan_id"]

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

        def modifyKeyword(key, keyword):
            if key == "saf_id":
                return "SAF " + str(keyword)
            if key == "institution":
                if keyword.lower() == "utaustin":
                    return "texas"
                return keyword.lower()
            return keyword

        # TODO: before ingestion change the keys used for the keywords depending on which are available in the JSON file
        appended_keywords = [
            modifyKeyword(key, metadata_dict[key])
            for key in ["saf_id", "institution", "project_name", "sample_name"]
            if metadata_dict[key] is not None and str(metadata_dict[key]).strip() != ""
        ]

        dataset = RawDataset(
            owner="Matt Landsman",  # TODO: change before ingest # owner=metadata_dict["user_name"]
            contactEmail="mrlandsman@lbl.gov",  # TODO: change before ingest  # contactEmail=metadata_dict["user_email"]
            creationLocation="NSLS-II" + " " + metadata_dict["beamline_id"],
            datasetName=jsonl_file_name,
            type=DatasetType.raw,
            instrumentId=metadata_dict["beamline_id"],
            proposalId=metadata_dict["proposal_id"],
            dataFormat="NSLS-II",
            principalInvestigator="Lynn Katz",  # TODO: change before ingestion
            sourceFolder=self._folder.as_posix(),
            scientificMetadata=metadata_dict,
            sampleId=metadata_dict["sample_id"],
            isPublished=False,
            description=metadata_dict["sample_desc"],
            keywords=["scattering", "RSoXS", "NSLS-II"] + appended_keywords,
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
    owner_username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a folder of 11012 scattering folders"
    now_str = datetime.isoformat(datetime.utcnow()) + "Z"
    ownable = Ownable(
        createdBy="dylan",
        updatedBy="dylan",
        updatedAt=now_str,
        createdAt=now_str,
        ownerGroup="MWET", # Shouldn't this be owner_username with a default of MWET?
        accessGroups=["MWET", "ingestor"],
    )
    reader = ScatteringNsls2Sst1Reader(file_path, ownable)

    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))
    if len(list(png_files)) == 0:

        # Only glob primary images because those are the only ones with something interesting to look at.
        tiff_filenames = sorted(glob_non_hidden_in_folder(file_path, "*primary*.tiff"))
        tiff_filenames.extend(glob_non_hidden_in_folder(file_path, "*primary*.tif"))

        tiff_filename = tiff_filenames[len(tiff_filenames) // 2]
        image_data = None

        try:
            file_loader = PyHyperScattering.load.SST1RSoXSLoader(corr_mode="none")
            image_data = file_loader.loadSingleImage(tiff_filename)
        except KeyError as e:
            # Could be too specific as this checks if en_energy_setpoint was not found in the csv file
            # by checking if it is raised in a key error.
            # However, it is possible that the underlying library could throw an error about another key
            # not being found in the primary csv. If so then we should also pass on that as well.
            if "en_energy_setpoint" in repr(e):
                image_data = Image.open(tiff_filename)
                image_data = xr.DataArray(np.array(image_data))
            else:
                raise e

        build_RSoXS_thumb_SST1(
            image_data, tiff_filename.stem, file_path, reader.scan_id
        )
    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))

    datafiles, size = create_data_files_list(
        file_path, excludeCheck=lambda x: x.name == "dat"
    )

    primary_csv_found = False
    for datafile in datafiles:
        filename = Path(datafile.path).name
        if ".csv" in filename and "primary" in filename:
            if primary_csv_found:
                raise Exception("Must only have one primary CSV inside folder")
            primary_csv_found = True

    if not primary_csv_found:
        raise FileNotFoundError("Primary CSV does not exist inside folder")

    dataset = reader.create_dataset()
    dataset_id = scicat_client.datasets_create(dataset)
    reader.dataset_id = dataset_id
    thumbnail = reader.create_attachment(png_files[0])
    scicat_client.datasets_attachment_create(thumbnail)

    data_block = reader.create_data_block(datafiles, size)
    scicat_client.datasets_origdatablock_create(dataset_id, data_block)
    return dataset_id


def build_thumbnail(image_data, name, directory):
    log_image = image_data
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
