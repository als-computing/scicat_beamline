from curses.ascii import isspace
from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, OrderedDict, Tuple
from unittest import skip
import numpy as np
from PIL import Image, ImageOps
from astropy.io import fits
from astropy.io.fits.header import _HeaderCommentaryCards
import pandas
import os



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
from jobs.dataset_reader import DatasetReader

class IgorAnalysisReader(DatasetReader):
    """A DatasetReader for Igor generated dat files
    Reader exepects a folder that contains dat files as
    well as a jpeg files of graphs. Jpeg files will be ingested 
    as attachments/thumbnails.

    Scientific Metadata is built as a dictionary of energy value keys each with a dict of the
    associated dat headers
    """
    dataset_id: str = None
    _issues = []


    def __init__(self, folder: Path, ownable: Ownable) -> None:
        self._folder = folder
        self._ownable = ownable

    def create_data_files(self) -> List[DataFile]:
        "Collects all files"
        datafiles = []
        for file in self._folder.iterdir():
            datafile = DataFile(
                path = file.name,
                size = get_file_size(file),
                time= get_file_mod_time(file),
                type="DerivedDatasets"
            )
            datafiles.append(datafile)
        return datafiles

    def create_data_block(self) -> Datablock:
        "Creates a datablock of all files"
        datafiles = self.create_data_files()
        
        return Datablock(
            datasetId = self.dataset_id,
            size = get_file_size(self._folder),
            dataFileList = datafiles,
            **self._ownable.dict()
        )


    def create_dataset(self) -> Dataset:
        "Creates a dataset object"
        folder_size = get_file_size(self._folder)
        datasetName = self._folder.parent.name + "_IGOR_ANALYSIS"
        inputDatasetName = self._folder.parent.name
        temp_ingestor = ScicatIngestor()
        a = temp_ingestor.get_datasets({"datasetName": inputDatasetName})
        print(inputDatasetName)
        

        print(self._folder)
        ai_file_name = next(self._folder.parent.glob("*.txt")).name[:-7]

        description = ai_file_name.replace("_", " ")
        description = description.replace('-', ' ')
        appended_keywords = description.split()
        dataset = DerivedDataset(
            investigator="Cameron McKay",
            inputDatasets=[a[0]['pid']],
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
            sourceFolder=self._folder.as_posix(),
            size=folder_size,
            scientificMetadata=self.create_scientific_metadata(),
            sampleId=datasetName,
            isPublished=False,
            description=description,
            keywords=["scattering", "rsoxs", "11.0.1.2", "ccd", "igor", "analysis"] + appended_keywords,
            creationTime=get_file_mod_time(self._folder),
            **self._ownable.dict())
        return dataset

    def create_attachment(self, file: Path) -> Attachment:
        "Creates a thumbnail jpg"
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
        dat_filenames = self._folder.glob("*.dat")
        sci_metadata = {}
        column_converters = {0:lambda string : string.strip('#').strip()}
        for dat_filename in dat_filenames:
            headers = None
            counter = 0
            with open(dat_filename) as dat_file:
                #Count the number of lines we will parse
                for line in dat_file:
                    if ( not line.isspace() and line.strip()[0] != '#'):
                        break
                    counter+=1
                dat_file.seek(0)
                headers = pandas.read_csv(dat_file, index_col=[0], squeeze=True, sep='=', nrows=counter, header=None, converters=column_converters, skip_blank_lines=True).dropna()

            # Re order headers in a new dict
            ordered_headers_dict = OrderedDict()

            # We are essentially swapping the top and bottom section of the headers
            atBottomHeaders = False
            for key, value in headers.items():
                if key == "Processed on":
                    atBottomHeaders = True
                if atBottomHeaders == True:
                    ordered_headers_dict[key] = value
            
            for key, value in headers.items():
                if key == "Processed on":
                   break
                ordered_headers_dict[key] = value

            energy = ordered_headers_dict['Nika_XrayEnergy']

            energy = energy.replace('.', "_")
            
            if energy not in sci_metadata:
                sci_metadata[energy] = ordered_headers_dict
            else:
                i = 1
                while True:
                    if f"{energy} ({i})" not in sci_metadata:
                        sci_metadata[f"{energy} ({i})"] = ordered_headers_dict
                    i+=1
        sci_metadata = OrderedDict(sorted(sci_metadata.items(), key=lambda t: t[0]))
        return sci_metadata


def ingest(folder: Path) -> Tuple[str, List[Issue]]:
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
            accessGroups=["MWET", "ingestor"])
    reader = IgorAnalysisReader(folder, ownable)
    issues:List[Issue] = []
    ingestor = ScicatIngestor(issues)

    dataset = reader.create_dataset()
    print(dataset)
    dataset_id = ingestor.upload_derived_dataset(dataset)
    reader.dataset_id = dataset_id
    #TODO: ensure that all jpg files are uploaded as attachments
    # And maybe pngs
    jpg_files = list(folder.glob("*.jpg"))
    if len(list(jpg_files)) > 0:
        thumbnail = reader.create_attachment(jpg_files[0])
        ingestor.upload_attachment(thumbnail, datasetType="DerivedDatasets")

    data_block = reader.create_data_block()   
    ingestor.upload_datablock(data_block, datasetType="DerivedDatasets")
    return dataset_id, issues

if __name__ == "__main__":
    from pprint import pprint
    folder = Path('/home/j/programming/work/Oct_2021_scattering/CCD')
    for path_obj in folder.iterdir():
        print(f"Folder: {path_obj}")
        if not path_obj.is_dir():
            continue
        dat_folder = path_obj / "dat"
        if dat_folder.exists() and dat_folder.is_dir() and len(os.listdir(dat_folder)) > 0:
            dataset_id, issues = ingest(dat_folder)
            print(f"Ingested {path_obj} as {dataset_id}. Issues:")
            pprint(issues)

    