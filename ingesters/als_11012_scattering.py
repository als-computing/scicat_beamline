from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, OrderedDict

import numpy as np
from astropy.io import fits
from astropy.io.fits.header import _HeaderCommentaryCards
from PIL import Image, ImageOps
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (Attachment, Dataset, DatasetType, OrigDatablock,
                            Ownable, RawDataset)

from common_ingester_utils import (Issue, add_to_sci_metadata_from_bad_headers,
                                   create_data_files_list, get_file_mod_time,
                                   glob_non_hidden_in_folder)

ingest_spec = "als_11012_scattering"


class Scattering11012Reader:
    """A DatasetReader for reading 11012 scattering datasets.
    Reader exepects a folder that contains the labview (AI) text file as
    well as all fits files and some png files. Png files will be ingested
    as attachments/thumbnails. Fits files will be ingested as DataFiles.

    Scientific Metadata is built as a dictionary where each child is an array build from
    headers of each fits file.
    """

    dataset_id: Optional[str] = None
    _issues = []

    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable

    # def create_data_files(self) -> Tuple[List[DataFile], int]:
    #     "Collects all fits files"
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

    def create_data_block(self, datafiles, size) -> OrigDatablock:
        "Creates a datablock of fits files"

        assert (
            self.dataset_id is not None
        ), "dataset_id must be set before creating data block"

        return OrigDatablock(
            datasetId=self.dataset_id,
            instrumentGroup="instrument-default",
            size=size,
            dataFileList=datafiles,
            **self._ownable.model_dump(),
        )

    def create_dataset(self) -> Dataset:
        "Creates a dataset object"
        sample_name = self._folder.name

        ai_file_path = next(glob_non_hidden_in_folder(self._folder, "*.txt"))
        creationTime = get_file_mod_time(ai_file_path)
        ai_file_name = ai_file_path.name[:-7]
        description = ai_file_name.replace("_", " ")
        description = description.replace("-", " ")
        sample_name = sample_name.replace("_", " ").replace("-", " ")
        description = sample_name + " " + description
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
            sourceFolder=self._folder.as_posix(),
            scientificMetadata=self.create_scientific_metadata(),
            sampleId=sample_name,
            isPublished=False,
            description=description,
            keywords=["scattering", "RSoXS", "ALS", "11.0.1.2", "11.0.1.2 RSoXS"],
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

    def create_scientific_metadata(self) -> Dict:
        """Generate a json dict of scientific metadata
        by reading each fits file header

        Args:
            folder (Path):  folder in which to scan fits files
        """
        fits_files = glob_non_hidden_in_folder(self._folder, "*.fits")
        fits_files = sorted(fits_files)
        metadata = {}
        # Headers from AI file
        ai_file_name = next(glob_non_hidden_in_folder(self._folder, "*.txt"))
        add_to_sci_metadata_from_bad_headers(
            metadata, ai_file_name, when_to_stop=lambda line: line.startswith("Time")
        )
        for fits_file in fits_files:
            with fits.open(fits_file) as hdulist:
                metadata_header = hdulist[0].header
                for key in metadata_header.keys():
                    value = metadata_header[key]
                    if type(value) == _HeaderCommentaryCards:
                        continue
                    if not metadata.get(key):
                        metadata[key] = OrderedDict()
                    metadata[key][fits_file.stem] = metadata_header[key]
        return metadata


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
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
    reader = Scattering11012Reader(file_path, ownable)

    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))
    if len(list(png_files)) == 0:
        fits_filenames = sorted(glob_non_hidden_in_folder(file_path, "*.fits"))
        fits_filename = fits_filenames[len(fits_filenames) // 2]
        image_data = fits.getdata(fits_filename, ext=2)
        build_thumbnail(image_data, fits_filename.name[:-5], file_path)
    png_files = list(glob_non_hidden_in_folder(file_path, "*.png"))

    datafile_array, size = create_data_files_list(file_path, lambda x: x.name == "dat")

    dataset = reader.create_dataset()
    dataset_id = scicat_client.datasets_create(dataset)
    reader.dataset_id = dataset_id

    thumbnail = reader.create_attachment(png_files[0])
    scicat_client.datasets_attachment_create(thumbnail)

    data_block = reader.create_data_block(datafile_array, size)
    scicat_client.datasets_origdatablock_create(dataset_id, data_block)
    return dataset_id


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
