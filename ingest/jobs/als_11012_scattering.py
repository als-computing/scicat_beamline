from curses.ascii import isspace
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, OrderedDict, Tuple
import numpy as np
from PIL import Image, ImageOps
from astropy.io import fits
from astropy.io.fits.header import _HeaderCommentaryCards
import pandas

from jobs.dataset_reader import DatasetReader

from ingestor import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    DerivedDataset,
    Issue,
    Ownable,
    ScicatIngestor,
    encode_thumbnail,
    get_file_mod_time,
    get_file_size)


class Scattering11012Reader(DatasetReader):
    """A DatasetReader for reading 11012 scattering datasets.
    Reader exepects a folder that contains the labview (AI) text file as
    well as all fits files and some png files. Png files will be ingested 
    as attachments/thumbnails. Fits files will be ingested as Datablock.DataFiles.

    Scientific Metadata is built as a dictionary where each child is an array build from
    headers of each fits file.
    """
    dataset_id: str = None
    _issues = []


    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable

    def create_data_files(self) -> List[DataFile]:
        "Collects all fits files"
        datafiles = []
        for file in self._folder.iterdir():
            # We exclude directories within this, directories within will probably be folders of corresponding dat files.
            if file.name == 'dat':
                continue
            datafile = DataFile(
                path = file.name,
                size = get_file_size(file),
                time= get_file_mod_time(file),
                type="RawDatasets"
            )
            datafiles.append(datafile)
        return datafiles

    def create_data_block(self) -> Datablock:
        "Creates a datablock of fits files"
        datafiles = self.create_data_files()
        
        return Datablock(
            datasetId = self.dataset_id,
            size = get_file_size(self._folder),
            dataFileList = datafiles,
            **self._ownable.dict()
        )


    def create_dataset(self) -> Dataset:
        "Creates a dataset object"
        #Excludes size of dat folder
        folder_size = get_file_size(self._folder) - get_file_size(Path(f"{self._folder}/dat"))
        sample_name = self._folder.name

        ai_file_name = next(self._folder.glob("*.txt")).name[:-7]

        description = ai_file_name.replace("_", " ")
        description = description.replace('-', ' ')
        appended_keywords = description.split()
        dataset = Dataset(
            owner="test",
            contactEmail="cbabay1993@gmail.com",
            creationLocation="ALS11021",
            datasetName=sample_name,
            type=DatasetType.raw,
            instrumentId="11012",
            proposalId="unknown",
            dataFormat="BCS",
            principalInvestigator="Lynn Katz",
            sourceFolder=self._folder.as_posix(),
            size=folder_size,
            scientificMetadata=self.create_scientific_metadata(),
            sampleId=sample_name,
            isPublished=False,
            description=description,
            keywords=["scattering", "rsoxs", "11.0.1.2", "ccd"] + appended_keywords,
            creationTime=get_file_mod_time(self._folder),
            **self._ownable.dict())
        return dataset

    def create_attachment(self, file: Path) -> Attachment:
        "Creates a thumbnail png"
        return Attachment(
            datasetId = self.dataset_id,
            thumbnail = encode_thumbnail(file),
            caption="scattering image",
            **self._ownable.dict()
        )

    def create_scientific_metadata(self) -> Dict:

        """Generate a json dict of scientific metadata
        by reading each fits file header

        Args:
            folder (Path):  folder in which to scan fits files
        """
        fits_files = self._folder.glob("*.fits")
        metadata = {}
        # Headers from AI file
        ai_file_name = next(self._folder.glob("*.txt"))
        with open(ai_file_name) as ai_file:
            metadata['headers'] = []
            for line in ai_file:
                if line.startswith('Time'):
                    break
                metadata["headers"].append(line.rstrip())
        for fits_file in fits_files:
            with fits.open(fits_file) as hdulist:
                metadata_header = hdulist[0].header
                for key in metadata_header.keys():
                    if not metadata.get(key):
                        metadata[key] = []
                    value = metadata_header[key]
                    if type(value) == _HeaderCommentaryCards:
                        continue
                    metadata.get(key).append(metadata_header[key])
        return metadata


def ingest(folder: Path) -> Tuple[str, List[Issue]]:
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
            accessGroups=["MWET", "ingestor"])
    reader = Scattering11012Reader(folder, ownable)
    issues:List[Issue] = []
    ingestor = ScicatIngestor(issues)

    dataset = reader.create_dataset()
    dataset_id = ingestor.upload_raw_dataset(dataset)
    reader.dataset_id = dataset_id
    png_files = list(folder.glob("*.png"))
    if len(list(png_files)) > 0:
        thumbnail = reader.create_attachment(png_files[0])
        ingestor.upload_attachment(thumbnail)

    data_block = reader.create_data_block()   
    ingestor.upload_datablock(data_block)
    return dataset_id, issues


def build_thumbnail(fits_data, name, directory):
        log_image = fits_data
        log_image = log_image - np.min(log_image) + 1.001
        log_image = np.log(log_image)
        log_image = 205*log_image/(np.max(log_image))
        auto_contrast_image = Image.fromarray(log_image.astype('uint8'))
        auto_contrast_image = ImageOps.autocontrast(
                                auto_contrast_image, cutoff=0.1)
        dir = Path(directory)
        filename = name + ".png"
        # file = io.BytesIO()
        file = dir / Path(filename)
        auto_contrast_image.save(file, format='PNG')
        return file

if __name__ == "__main__":
    from pprint import pprint
    folder = Path('/home/dylan/data/beamlines/11012/restructured/scattering')
    for path in folder.iterdir():
        print(path)
        if not path.is_dir():
            continue
        try:
            png_files = list(path.glob('*.png'))
            if len(png_files) == 0:
                fits_filenames = sorted(path.glob('*.fits'))
                fits_filename = fits_filenames[len(fits_filenames)//2]
                image_data = fits.getdata(fits_filename, ext=2)
                build_thumbnail(image_data, fits_filename.name[:-5], path)
            
            dataset_id, issues = ingest(path)
            print(f"Ingested {path} as {dataset_id}. Issues:")
            pprint(issues)
        except Exception as e:
            print('ERROR:')
            print(e)
            print(f"Error ingesting {path} with {e}")
            raise e

    