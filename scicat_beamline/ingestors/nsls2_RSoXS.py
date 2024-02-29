from datetime import datetime
from pathlib import Path
from typing import List, Tuple
import numpy as np
from PIL import Image, ImageOps
import os
import PyHyperScattering
import json
import xarray as xr
import pandas as pd


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
from scicat_beamline.ingestors.common_ingestor_code import create_data_file, create_data_files_list

from scicat_beamline.utils import Issue, glob_non_hidden_in_folder
from scicat_beamline.scicat_utils import build_RSoXS_thumb_SST1

ingest_spec = "nsls2_rsoxs_sst1"

class ScatteringNsls2Sst1Reader():
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

    def __init__(self, folder: Path, source_folder: Path, ownable: Ownable) -> None:
        self._source_folder = source_folder
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
            if (key == "saf_id"):
                return "SAF " + str(keyword)
            if (key == "institution"):
                if keyword.lower() == "utaustin":
                    return "texas"
                return keyword.lower()
            return keyword
        # TODO: before ingestion change the keys used for the keywords depending on which are available in the JSON file
        appended_keywords = [
            modifyKeyword(key, metadata_dict[key])
            for key in [
                "institution",
                "project_name",
                # "sample_name"
            ]
            if metadata_dict[key] is not None
            and str(metadata_dict[key]).strip() != ""
        ]
        # TODO: change SAF id and transmission/reflection based on rsoxs folder
         appended_keywords += ["SAF " + metadata_dict["SAF"], "rsoxs_transmission"]
        owner, contactEmail = get_owner_jul2023(metadata_dict["project_name"])

        df = pd.read_csv(self._source_folder/"master_sample_list.csv")
        sample_keywords = None
        for i, row in df.iterrows():
            if row["sample_id"] == metadata_dict["sample_id"]:
                sample_keywords: str = row['sample_keywords']
                assert "not m-wet" not in sample_keywords
                assert "not mwet" not in sample_keywords
                sample_keywords = sample_keywords.split(',')
                sample_keywords = [x.strip() for x in sample_keywords]

        dataset = RawDataset(
            owner=owner,  # TODO: change before ingest # owner=metadata_dict["user_name"]
            contactEmail=contactEmail,  # TODO: change before ingest  # contactEmail=metadata_dict["user_email"]
            creationLocation="NSLS-II" + " " + metadata_dict["beamline_id"],
            datasetName=jsonl_file_name,
            type=DatasetType.raw,
            instrumentId=metadata_dict["beamline_id"],
            proposalId=metadata_dict["proposal_id"],
            dataFormat="NSLS-II",
            principalInvestigator="Greg Su", #TODO: change before ingestion
            sourceFolder=self._source_folder.as_posix(),
            scientificMetadata=metadata_dict,
            sampleId=metadata_dict["sample_id"],
            isPublished=False,
            description=metadata_dict["sample_name"]+": " + metadata_dict["sample_desc"],
            keywords=["scattering", "RSoXS", "NSLS-II", "SST-1 RSoXS", "SST-1"] + appended_keywords + sample_keywords,
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
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a folder of 11012 scattering folders"
    #TODO: change source folder depending on dataset and where masks are
    SOURCE_FOLDER = file_path.parent.parent

    if file_path.name == "spirals" or file_path.name == "alignments":
        return
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
        instrumentGroup="instrument-default",
    )
    reader = ScatteringNsls2Sst1Reader(file_path, SOURCE_FOLDER, ownable)
    issues: List[Issue] = []

    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))
    if len(list(png_files)) == 0:

        # Only glob primary images because those are the only ones with something interesting to look at.
        tiff_filenames = sorted(glob_non_hidden_in_folder(file_path, '*primary*.tiff'))
        tiff_filenames.extend(glob_non_hidden_in_folder(file_path, '*primary*.tif'))

        tiff_filename = tiff_filenames[len(tiff_filenames) // 2]
        image_data = None

        try:
            image_data = loadImageCorrIsNone(tiff_filename)
        except KeyError as e:
            # Could be too specific as this checks if en_energy_setpoint was not found in the csv file
            # by checking if it is raised in a key error.
            # However, it is possible that the underlying library could throw an error about another key
            # not being found in the primary csv. If so then we should also pass on that as well.
            if 'en_energy_setpoint' in repr(e):
                image_data = Image.open(tiff_filename)
                image_data = xr.DataArray(np.array(image_data))
            else:
                raise e

        build_RSoXS_thumb_SST1(image_data, tiff_filename.stem, thumbnail_dir, reader.scan_id)
    png_files = list(glob_non_hidden_in_folder(thumbnail_dir, "*.png"))

    datafiles, size = create_data_files_list(file_path, excludeCheck=lambda x: x.name == 'dat', relativeTo=SOURCE_FOLDER)

    primary_csv = list(file_path.parent.glob("*"+file_path.name+"*.csv"))
    assert len(primary_csv) == 1
    primary_csv_datafile, primary_csv_size = create_data_file(primary_csv[0], relativePath=primary_csv[0].relative_to(SOURCE_FOLDER))

    datafiles.append(primary_csv_datafile)
    size += primary_csv_size

    # TODO: change based on if there are masks or not
    masks, masks_size = create_data_files_list(SOURCE_FOLDER/"masks", relativeTo=SOURCE_FOLDER)

    datafiles += masks
    size += masks_size

    dataset = reader.create_dataset()
    dataset_id = scicat_client.upload_raw_dataset(dataset)
    reader.dataset_id = dataset_id
    thumbnail = reader.create_attachment(png_files[0])
    scicat_client.upload_attachment(thumbnail)

    data_block = reader.create_data_block(datafiles, size)
    scicat_client.upload_datablock(data_block)
    return dataset_id, issues


def loadImageCorrIsNone(filepath, dark_pedestal=100):
    """This is a version of the SST1RSoXSLoader loadSingleImage when corr='none', however it does not return
    attributes attached to the data"""
    img = Image.open(filepath)
    corr = 1
    image_data = (np.array(img)-dark_pedestal)/corr
    return xr.DataArray(image_data, dims=['pix_y','pix_x'])


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


def get_owner_jul2023(project_name: str):
    if project_name in ["kwon_fouling", "calibration", "landsman_isoporous", "landsman_isoporus", "landsman_microplastics", "landsman_fouling"]:
        return "Matt Landsman", "mrlandsman@lbl.gov"
    if project_name == "mckay_fouling":
        return "Cameron McKay", "cameron.keith.mckay@utexas.edu"
    raise Exception(f"Unknown project_name: `{project_name}`")


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
