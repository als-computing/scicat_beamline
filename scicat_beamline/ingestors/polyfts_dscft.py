import logging
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import fabio

from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (Attachment, DataFile, DatasetType, DerivedDataset,
                            OrigDatablock, Ownable, RawDataset)
from scicat_beamline.common_ingestor_code import (
    Issue, add_to_sci_metadata_from_bad_headers, create_data_files_list)
from scicat_beamline.scicat_utils import (build_search_terms,
                                          encode_image_2_thumbnail)

ingest_spec = "polyfts_dscft"

logger = logging.getLogger("scicat_ingest.733_SAXS")


global_keywords = [
    "PolyFTS",
    "DSCFT",
]  # TODO: before ingestion change


def ingest(
    scicat_client: ScicatClient,
    username: str,
    folder: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:

    scientific_metadata = OrderedDict()

    # TODO: change this before ingestion
    basic_scientific_md = OrderedDict()
    basic_scientific_md["institution"] = "ucsb"

    # TODO: change based on project name before ingestion
    basic_scientific_md["project_name"] = "cooper_diblock_nips"

    # raise Exception("MUST SPECIFY GEOMETRY")

    scientific_metadata.update(basic_scientific_md)

    # TODO: change PI before ingestion
    scicat_metadata = {
        "owner": "Tony Cooper",
        "email": "acooper@ucsb.edu",
        "instrument_name": "PolyFTS DSCFT",
        "proposal": "UNKNOWN",
        "pi": "Glenn Fredrickson",
    }

    # temporary access controls setup
    ownable = Ownable(
        ownerGroup="MWET",
        accessGroups=["ingestor", "MWET"],
    )

    dataset_id = upload_raw_dataset(
        scicat_client,
        folder,
        scicat_metadata,
        scientific_metadata,
        ownable,
    )
    upload_data_block(scicat_client, folder, dataset_id, ownable)

    thumbnail_file = folder / "fields_rho.gif"
    encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file, imType="gif")
    upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)

    return dataset_id


def upload_raw_dataset(
    scicat_client: ScicatClient,
    folder: Path,
    scicat_metadata: Dict,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    sci_md_keywords = [
        scientific_metadata["project_name"],
        scientific_metadata["institution"],
    ]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]

    params_file = folder / "params.in"

    file_mod_time = get_file_mod_time(params_file)
    folder_name = folder.name

    # sampleId = get_sample_id_oct_2022(file_name)

    description = build_search_terms(folder_name)
    # sample_keywords = find_sample_keywords_oct_2022(folder.name)
    dataset = RawDataset(
        owner=scicat_metadata.get("owner"),
        contactEmail=scicat_metadata.get("email"),
        creationLocation="PERLMUTTER",
        datasetName=folder_name,
        type=DatasetType.raw,
        instrumentId=scicat_metadata.get("instrument_name"),
        proposalId=scicat_metadata.get("proposal"),
        dataFormat="",
        principalInvestigator=scicat_metadata.get("pi"),
        sourceFolder=str(folder),
        scientificMetadata=scientific_metadata,
        sampleId="",
        isPublished=False,
        description=description,
        keywords=global_keywords + sci_md_keywords + [sample_id],
        creationTime=file_mod_time,
        **ownable.model_dump(),
    )
    dataset_id = scicat_client.datasets_create(dataset)
    return dataset_id


def upload_data_block(
    scicat_client: ScicatClient, folder: Path, dataset_id: str, ownable: Ownable
) -> OrigDatablock:
    "Creates a OrigDatablock of files"
    file_params = folder / "params.in"
    total_size = get_file_size(file_params)

    file_rho_initial = folder / "fields_rho_initial.in"
    file_w_initial = folder / "fields_w_initial.in"

    total_size += get_file_size(file_rho_initial)
    total_size += get_file_size(file_w_initial)

    datafiles = [
        DataFile(
            path=file_params.name,
            size=get_file_size(file_params),
            time=get_file_mod_time(file_params),
            type="RawDatasets",
        ),
        DataFile(
            path=file_w_initial.name,
            size=get_file_size(file_w_initial),
            time=get_file_mod_time(file_w_initial),
            type="RawDatasets",
        ),
        DataFile(
            path=file_rho_initial.name,
            size=get_file_size(file_rho_initial),
            time=get_file_mod_time(file_rho_initial),
            type="RawDatasets",
        ),
    ]

    datablock = OrigDatablock(
        datasetId=dataset_id,
        instrumentGroup="instrument-default",
        size=total_size,
        dataFileList=datafiles,
        **ownable.model_dump(),
    )
    scicat_client.datasets_origdatablock_create(dataset_id, datablock)


# TODO: Replace with a generalized version in common_ingestor_code.py
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
        caption="simulation gif",
        **ownable.model_dump(),
    )
    scicat_client.datasets_attachment_create(attachment)


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


# def find_sample_keywords_oct_2022(datasetName):
#     # TODO: write a new method for finding sample keywords depending on how the sample is represented in the dataset name
#     keywords = []
#     if "kapton" in datasetName:
#         keywords.append("kapton")
#     if "B19" in datasetName or "B13" in datasetName:
#         keywords.append("membranes")
#     if "disp" in datasetName:
#         keywords.append("disp")
#     if "agnp" in datasetName:
#         keywords.append("agnp")
#     if "cal" in datasetName:
#         keywords.append("cal")
#     if "agb" in datasetName:
#         keywords.append("agb")
#     return keywords


# def get_sample_id_oct_2022(datasetName: str):
#     # TODO: write a new method for finding the sampleId depending on how it is represented in the name
#     sampleId = None
#     if "agb" in datasetName:
#         sampleId = datasetName[datasetName.find("agb"):].split('_')
#         sampleId = sampleId[0]
#     elif "kapton" in datasetName and "cal" not in datasetName:
#         sampleId = datasetName[datasetName.find("kapton"):].split("_")
#         if sampleId[1] == 'ctrl':
#             sampleId = sampleId[0] + "_" + sampleId[1]
#         else:
#             sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
#     elif "B19" in datasetName:
#         sampleId = datasetName[datasetName.find("B19"):].split('_')
#         sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
#     elif "B13" in datasetName:
#         sampleId = datasetName[datasetName.find("B13"):].split('_')
#         sampleId = sampleId[0]+"_"+sampleId[1]+"_"+sampleId[2]
#     elif "disp" in datasetName:
#         sampleId = datasetName[datasetName.find("disp"):].split('_')
#         sampleId = sampleId[0]
#     elif "agnp" in datasetName:
#         sampleId = datasetName[datasetName.find("agnp"):].split('_')
#         sampleId = sampleId[0]
#         posIndex = sampleId.find("pos")
#         if posIndex != -1:
#             sampleId = sampleId[:sampleId.find("pos")]
#     return sampleId
