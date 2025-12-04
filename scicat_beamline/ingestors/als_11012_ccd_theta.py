from datetime import datetime
from pathlib import Path
from typing import List


from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    OrigDatablock,
    DataFile,
    Dataset,
    DatasetType,
    Ownable,
)

from scicat_beamline.utils import Issue

ingest_spec = "als_11012_ccd_theta"


def create_data_files(self) -> List[DataFile]:
    "Collects the one AI  txt file"
    return [self._file]


def create_data_block(dataset_id: str, file: Path, ownable: Ownable) -> OrigDatablock:
    "Creates a datablock of fits files"
    datafiles = create_data_files([str(file)])

    return OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=get_file_size(file),
        dataFileList=datafiles,
        **ownable.model_dump(),
    )


def create_dataset(file, ownable: Ownable) -> Dataset:
    "Creates a dataset object"
    sample_name = file.name
    dataset = Dataset(
        owner="test",
        contactEmail="cbabay1993@gmail.com",
        creationLocation="ALS 11.0.1.2",
        datasetName=sample_name,
        type=DatasetType.raw,
        instrumentId="11.0.1.2",
        proposalId="unknown",
        dataFormat="BCS",
        principalInvestigator="Lynne Katz",
        sourceFolder=file.parent.as_posix(),
        scientificMetadata={},
        sampleId=sample_name,
        isPublished=False,
        description="",
        keywords=["ccd", "theta", "rsoxs", "als", "11.0.1.2"],
        creationTime=get_file_mod_time(file),
        **ownable.model_dump(),
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
    dataset_id = scicat_client.datasets_create(dataset)

    datablock = create_data_block(dataset_id, file_path, ownable)
    scicat_client.datasets_origdatablock_create(dataset_id, datablock)

    return dataset_id
