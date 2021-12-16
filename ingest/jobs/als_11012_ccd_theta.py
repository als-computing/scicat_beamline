from datetime import datetime
import os
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from .dataset_reader import DatasetReader

from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size
)
from pyscicat.model import (
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Ownable
  )

load_dotenv('.env')

SCICAT_BASEURL = os.getenv('SCICAT_BASEURL')
SCICAT_INGEST_USER = os.getenv('SCICAT_INGEST_USER')
SCICAT_INGEST_PASSWORD = os.getenv('SCICAT_INGEST_PASSWORD')


class CCDTheta11012Reader(DatasetReader):
    """A DatasetReader for reading 11012 theta scan datasets
    Reader exepects a single file that contains theta scan data

    Scientific Metadata is built as a dictionary where each child is an array build from
    headers of each fits file.
    """
    dataset_id: str = None

    def __init__(self, file: Path, ownable: Ownable) -> None:
        self._file = file
        self._ownable = ownable

    def create_data_files(self) -> List[DataFile]:
        "Collects the one AI  txt file"
        return [self._file]

    def create_data_block(self) -> Datablock:
        "Creates a datablock of fits files"
        datafiles = self.create_data_files()
        return Datablock(
            datasetId=self.dataset_id,
            size=get_file_size(self._file),
            dataFileList=datafiles,
            **self._ownable.dict()
        )

    def create_dataset(self) -> Dataset:
        "Creates a dataset object"
        folder_size = get_file_size(self._file)
        sample_name = self._file.name
        dataset = Dataset(
            owner="test",
            contactEmail="cbabay1993@gmail.com",
            creationLocation="ALS11021",
            datasetName=sample_name,
            type=DatasetType.raw,
            instrumentId="11012",
            proposalId="unknown",
            dataFormat="BCS",
            principalInvestigator="Lynne Katz",
            sourceFolder=self._file.as_posix(),
            size=folder_size,
            scientificMetadata={},
            sampleId=sample_name,
            isPublished=False,
            description="",
            keywords=["ccd", "theta", "rsoxs", "11.0.1.2"],
            creationTime=get_file_mod_time(self._file),
            **self._ownable.dict())
        return dataset

    def create_scientific_metadata(self) -> Dict:
        pass


def ingest(file: Path) -> Tuple[str]:
    "Ingest a folder of 11012 CCD Theta scan files"
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
    reader = CCDTheta11012Reader(file, ownable)
    client = ScicatClient(base_url=SCICAT_BASEURL, username=SCICAT_INGEST_USER, password=SCICAT_INGEST_PASSWORD)

    dataset = reader.create_dataset()
    dataset_id = client.upload_raw_dataset(dataset)
    reader.dataset_id = dataset_id
    return dataset_id


if __name__ == "__main__":
    folder = Path('/home/dylan/data/beamlines/11012/restructured/ccd_theta')
    for path in folder.iterdir():
        try:
            dataset_id = ingest(path)
            print(f"Ingested {path} as {dataset_id}.")
        except Exception as e:
            print(f"Error ingesting {path} with {e}")
            raise e
