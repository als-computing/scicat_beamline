from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, Tuple

from astropy.io import fits
from astropy.io.fits.header import _HeaderCommentaryCards

from .dataset_reader import DatasetReader

from ..ingestor import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Issue,
    Ownable,
    ScicatIngestor,
    encode_thumbnail,
    get_file_mod_time,
    get_file_size,
)


class CCDTheta11012Reader(DatasetReader):
    """A DatasetReader for reading 11012 theta scan datasets
    Reader exepects a single file that contains theta scan data

    Scientific Metadata is built as a dictionary where each child is an array build from
    headers of each fits file.
    """

    dataset_id: str = None
    _issues = []

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
            **self._ownable.dict(),
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
            **self._ownable.dict(),
        )
        return dataset

    def create_scientific_metadata(self) -> Dict:
        pass


def ingest(file: Path) -> Tuple[str, List[Issue]]:
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
        accessGroups=["MWET", "ingestor"],
    )
    reader = CCDTheta11012Reader(file, ownable)
    issues: List[Issue] = []
    ingestor = ScicatIngestor(issues)

    dataset = reader.create_dataset()
    dataset_id = ingestor.upload_raw_dataset(dataset)
    reader.dataset_id = dataset_id
    return dataset_id, issues


if __name__ == "__main__":
    from pprint import pprint

    folder = Path("/home/dylan/data/beamlines/11012/restructured/ccd_theta")
    for path in folder.iterdir():
        try:
            dataset_id, issues = ingest(path)
            print(f"Ingested {path} as {dataset_id}. Issues:")
            pprint(issues)
        except Exception as e:
            print(f"Error ingesting {path} with {e}")
            raise e
