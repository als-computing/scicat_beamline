from datetime import datetime
from pathlib import Path
import sys
from typing import Dict, List, Tuple

from astropy.io import fits
from astropy.io.fits.header import _HeaderCommentaryCards


from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    Attachment,
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Issue,
    Ownable,
)

ingest_spec = "als_11012_ccd_theta"

def create_data_files(self) -> List[DataFile]:
    "Collects the one AI  txt file"
    return [self._file]


def create_data_block(dataset_id: str, file: Path, ownable: Ownable) -> Datablock:
    "Creates a datablock of fits files"
    datafiles = create_data_files([str(file)])

    return Datablock(
        datasetId=dataset_id,
        size=get_file_size(file),
        dataFileList=datafiles,
        **ownable.dict(),
    )


def create_dataset(file, ownable: Ownable) -> Dataset:
    "Creates a dataset object"
    folder_size = get_file_size(file)
    sample_name = file.name
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
        sourceFolder=file.as_posix(),
        size=folder_size,
        scientificMetadata={},
        sampleId=sample_name,
        isPublished=False,
        description="",
        keywords=["ccd", "theta", "rsoxs", "11.0.1.2"],
        creationTime=get_file_mod_time(file),
        **ownable.dict(),
    )
    return dataset


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: str,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
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

    dataset = create_dataset()
    dataset_id = scicat_client.upload_raw_dataset(dataset)

    datablock = create_data_block(dataset_id, file_path, ownable)
    scicat_client.upload_datablock(datablock)

    return dataset_id, issues
