from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import OrderedDict

import fabio
from pyscicat.client import ScicatClient
from pyscicat.model import (
    Attachment,
    OrigDatablock,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
)
from scicat_beamline.ingestors.common_ingestor_code import add_to_sci_metadata_from_bad_headers

from scicat_beamline.scicat_utils import (
    build_search_terms,
    build_thumbnail,
    encode_image_2_thumbnail,
)
from scicat_beamline.utils import Issue

ingest_spec = "als733_saxs"

logger = logging.getLogger("scicat_ingest.733_SAXS")


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:

    scientific_metadata = OrderedDict()
    edf_file = edf_from_txt(file_path)
    if edf_file:
        with fabio.open(edf_file) as fabio_obj:
            image_data = fabio_obj.data
            scientific_metadata["edf headers"] = fabio_obj.header
    add_to_sci_metadata_from_bad_headers(scientific_metadata, file_path)

    scicat_metadata = {
        "owner": "Matt Landsman",
        "email": "mrlandsman@lbl.gov",
        "instrument_name": "ALS 7.3.3",
        "proposal": "UNKNOWN",
        "pi": "Greg Su",
    }

    # temporary access controls setup
    ownable = Ownable(
        ownerGroup="MWET",
        accessGroups=["ingestor", "MWET"],
    )

    dataset_id = upload_raw_dataset(
        scicat_client,
        file_path,
        scicat_metadata,
        scientific_metadata,
        ownable,
    )
    upload_data_block(scicat_client, file_path, dataset_id, ownable)
    if edf_file:
        thumbnail_file = build_thumbnail(image_data, thumbnail_dir)
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)

    return dataset_id


def edf_from_txt(txt_file_path: Path):
    return Path(txt_file_path.parent, txt_file_path.stem + ".edf")


def upload_raw_dataset(
    scicat_client: ScicatClient,
    file_path: Path,
    scicat_metadata: Dict,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    file_mod_time = get_file_mod_time(file_path)
    file_name = file_path.stem
    description = build_search_terms(file_path.parent.name + "_" + file_name)
    appended_keywords = description.split()
    dataset = RawDataset(
        owner=scicat_metadata.get("owner"),
        contactEmail=scicat_metadata.get("email"),
        creationLocation=scicat_metadata.get("instrument_name"),
        datasetName=file_name,
        type=DatasetType.raw,
        instrumentId=scicat_metadata.get("instrument_name"),
        proposalId=scicat_metadata.get("proposal"),
        dataFormat="733",
        principalInvestigator=scicat_metadata.get("pi"),
        sourceFolder=str(file_path.parent),
        scientificMetadata=scientific_metadata,
        sampleId=description,
        isPublished=False,
        description=description,
        keywords=appended_keywords + ["WAXS", "ALS", "7.3.3", "scattering", "7.3.3 WAXS"],  # TODO: change according to whether it is waxs or saxs,
        creationTime=file_mod_time,
        **ownable.dict(),
    )
    dataset_id = scicat_client.upload_raw_dataset(dataset)
    return dataset_id


def collect_files(txt_file_path: Path) -> List[Path]:
    return [txt_file_path, edf_from_txt(txt_file_path)]


def create_data_files(txt_file_path: Path) -> Tuple[int, List[DataFile]]:
    "Collects all txt and edf files"
    data_files = []
    total_size = 0
    files = collect_files(txt_file_path)
    for file in files:
        file_size = get_file_size(file)
        datafile = DataFile(
            path=file.name,
            size=file_size,
            time=get_file_mod_time(file),
            type="RawDatasets",
        )
        total_size += file_size
        data_files.append(datafile)
    return total_size, data_files


def upload_data_block(
    scicat_client: ScicatClient, txt_file_path: Path, dataset_id: str, ownable: Ownable
) -> OrigDatablock:
    "Creates a OrigDatablock of files, txt file plus fits files"
    total_size, datafiles = create_data_files(txt_file_path)

    datablock = OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=total_size,
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(datablock)


def upload_attachment(
    scicat_client: ScicatClient,
    encoded_thumnbnail: str,
    dataset_id: str,
    ownable: Ownable,
) -> Attachment:
    "Creates a thumbnail png"
    attachment = Attachment(
        datasetId=dataset_id,
        thumbnail=encoded_thumnbnail,
        caption="scattering image",
        **ownable.dict(),
    )
    scicat_client.upload_attachment(attachment)


def get_file_size(file_path: Path) -> int:
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    return str(datetime.fromtimestamp(file_path.lstat().st_mtime))


def _get_dataset_value(data_set):
    logger.debug(f"{data_set}  {data_set.dtype}")
    try:
        if "S" in data_set.dtype.str:
            if data_set.shape == (1,):
                return data_set.asstr()[0]
            elif data_set.shape == ():
                return data_set[()].decode("utf-8")
            else:
                return list(data_set.asstr())
        else:
            if data_set.maxshape == (1,):
                logger.debug(f"{data_set}  {data_set[()][0]}")
                return data_set[()][0]
            else:
                logger.debug(f"{data_set}  {data_set[()]}")
                return data_set[()]
    except Exception:
        logger.exception("Exception extracting dataset value")
        return None
