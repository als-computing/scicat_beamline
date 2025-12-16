import logging
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import fabio
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (
    Attachment,
    CreateDatasetOrigDatablockDto,
    DataFile,
    DatasetType,
    DerivedDataset,
    OrigDatablock,
    Ownable,
    RawDataset,
)

from scicat_beamline.common_ingester_utils import (
    Issue,
    add_to_sci_metadata_from_bad_headers,
    build_search_terms,
    create_data_files_list,
)
from scicat_beamline.thumbnail_utils import (
    build_waxs_saxs_thumb_733,
    encode_image_2_thumbnail,
)

ingest_spec = "als733_saxs"

logger = logging.getLogger("scicat_ingest")


global_keywords = [
    "SAXS",
    "ALS",
    "7.3.3",
    "scattering",
    "7.3.3 SAXS",
]  # TODO: before ingestion change according to SAXS/WAXS


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

    # TODO: change this before ingestion depending on how the institution is marked. Sometimes it's in the name and sometimes it's not.
    basic_scientific_md = OrderedDict()
    if "cal" in file_path.name:
        basic_scientific_md["institution"] = "lbnl"
    else:
        basic_scientific_md["institution"] = "texas"

    # TODO: change based on project name before ingestion
    basic_scientific_md["project_name"] = "SNIPS membranes"

    # TODO: change to transmission or grazing before ingestion
    basic_scientific_md["geometry"] = "transmission"
    # raise Exception("MUST SPECIFY GEOMETRY")

    scientific_metadata.update(basic_scientific_md)

    # TODO: change PI before ingestion
    scicat_metadata = {
        "owner": "Garrett Birkel",
        "email": "gwbirkel@lbl.gov",
        "instrument_name": "ALS 7.3.3",
        "proposal": "UNKNOWN",
        "pi": "Garrett Birkel",
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
        thumbnail_file = build_waxs_saxs_thumb_733(
            image_data, thumbnail_dir, edf_file.name
        )
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(
            scicat_client,
            encoded_thumbnail,
            dataset_id=dataset_id,
            caption="scattering image",
            ownable=ownable,
        )

    create_derived(
        scicat_client, edf_file, dataset_id, basic_scientific_md, scicat_metadata
    )

    return dataset_id


def create_derived(
    scicat_client: ScicatClient,
    raw_file_path: Path,
    raw_dataset_id: str,
    basic_scientific_md: OrderedDict,
    scicat_metadata: dict,
):
    # TODO: change depending on analysis type
    ANALYSIS = "radial integration"
    # TODO: change job parameters depending on the parameters given to the script which creates the derived data
    jobParams = {"method": "pyFAI integrate1d", "npt": 2000}

    now_str = datetime.isoformat(datetime.now(datetime.timezone.utc)) + "Z"
    ownable = Ownable(
        createdBy="dylan",
        updatedBy="dylan",
        updatedAt=now_str,
        createdAt=now_str,
        ownerGroup="MWET",
        accessGroups=["MWET", "ingestor"],
    )
    # TODO: before ingestion change how the derived_name is generated, could be different depending on the script
    # for generating the derived data

    raw_fname = raw_file_path.name
    derived_name = raw_fname[: raw_fname.find("_2m")]
    derived_name = derived_name[derived_name.find("texas_") + 6 :]
    if "disp" in derived_name:
        derived_name = derived_name[derived_name.find("disp") :]

    derived_parent_folder = raw_file_path.parent / "analysis"

    derived_files, total_size = create_data_files_list(
        derived_parent_folder, excludeCheck=lambda path: derived_name not in path.name
    )
    if len(derived_files) == 0:
        return

    datasetName = derived_name + "_" + ANALYSIS.upper().replace(" ", "_")
    description = datasetName.replace("_", " ")

    creationTime = get_file_mod_time(
        Path(derived_parent_folder / derived_files[0].path)
    )

    sci_md_keywords = [
        basic_scientific_md["project_name"],
        basic_scientific_md["institution"],
        basic_scientific_md["geometry"],
    ]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]
    basic_scientific_md["analysis"] = ANALYSIS

    sample_keywords = find_sample_keywords_oct_2022(derived_name)

    dataset = DerivedDataset(
        investigator=scicat_metadata.get("pi"),
        inputDatasets=[raw_dataset_id],
        usedSoftware=["jupyter notebook", "python", "matplotlib", "pyFAI"],
        owner=scicat_metadata.get("owner"),
        contactEmail=scicat_metadata.get("email"),
        datasetName=datasetName,
        type=DatasetType.derived,
        instrumentId="7.3.3",
        sourceFolder=derived_parent_folder.as_posix(),
        scientificMetadata=basic_scientific_md,
        jobParameters=jobParams,
        isPublished=False,
        description=description,
        keywords=global_keywords
        + sci_md_keywords
        + sample_keywords
        + [
            "analysis",
            "reduced",
            ANALYSIS,
        ],  # TODO: before ingestion change keywords depending on type of analysis
        creationTime=creationTime,
        **ownable.model_dump(),
    )

    derived_id = scicat_client.datasets_create(dataset)
    logger.info(f"Created derived dataset with id {derived_id} for file {raw_fname}")

    # TODO: decide which thumbnail to use before ingestion
    thumbPath = None
    for file in derived_files:
        if "radint" in Path(file.path).name and Path(file.path).suffix == ".png":
            thumbPath = derived_parent_folder / file.path

    encoded_thumbnail = encode_thumbnail(thumbPath)

    upload_attachment(
        scicat_client,
        encoded_thumbnail=encoded_thumbnail,
        dataset_id=derived_id,
        caption="radial integration graph",
        ownable=ownable,
        dataset_type="DerivedDatasets",
    )

    data_block = OrigDatablock(
        datasetId=derived_id,
        instrumentGroup="instrument-default",
        size=total_size,
        dataFileList=derived_files,
        **ownable.model_dump(),
    )

    scicat_client.datasets_origdatablock_create(derived_id, data_block)


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
    sci_md_keywords = [
        scientific_metadata["project_name"],
        scientific_metadata["institution"],
        scientific_metadata["geometry"],
    ]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]

    file_mod_time = get_file_mod_time(file_path)
    file_name = file_path.stem

    sampleId = get_sample_id_oct_2022(file_name)

    description = build_search_terms(file_path.parent.name + "_" + file_name)
    sample_keywords = find_sample_keywords_oct_2022(file_path.name)
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
        sampleId=sampleId,
        isPublished=False,
        description=description,
        keywords=global_keywords + sci_md_keywords + sample_keywords,
        creationTime=file_mod_time,
        **ownable.model_dump(),
    )
    dataset_id = scicat_client.datasets_create(dataset)
    logger.info(f"Created dataset with id {dataset_id} for file {file_path.name}")
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
        )
        total_size += file_size
        data_files.append(datafile)
    logger.info(f"Found {len(data_files)} data files")
    return total_size, data_files


def upload_data_block(
    scicat_client: ScicatClient, txt_file_path: Path, dataset_id: str, ownable: Ownable
) -> OrigDatablock:
    "Creates a OrigDatablock of files"
    total_size, datafiles = create_data_files(txt_file_path)

    datablock = CreateDatasetOrigDatablockDto(
        size=total_size,
        dataFileList=datafiles,
    )
    result = scicat_client.datasets_origdatablock_create(dataset_id, datablock)
    logger.info(
        f"Created datablock for dataset id {dataset_id} for file {txt_file_path.name}"
    )
    return result


# TODO: Move to common_ingester_code.py and use as a generalized function
def upload_attachment(
    scicat_client: ScicatClient,
    encoded_thumbnail: str,
    dataset_id: str,
    caption: str,
    ownable: Ownable,
    dataset_type: str = "Datasets",
) -> Attachment:
    "Creates an attachment thumbnail"
    attachment = Attachment(
        datasetId=dataset_id,
        thumbnail=encoded_thumbnail,
        caption=caption,
        **ownable.model_dump(),
    )
    result = scicat_client.datasets_attachment_create(
        attachment, datasetType=dataset_type
    )
    logger.info(f'Created attachment for dataset {dataset_id} with caption "{caption}"')
    return result


def get_file_size(file_path: Path) -> int:
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    return datetime.fromtimestamp(file_path.lstat().st_mtime).isoformat() + "Z"


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


def find_sample_keywords_oct_2022(datasetName):
    # TODO: write a new method for finding sample keywords depending on how the sample is represented in the dataset name
    keywords = []
    if "kapton" in datasetName:
        keywords.append("kapton")
    if "B19" in datasetName or "B13" in datasetName:
        keywords.append("membranes")
    if "disp" in datasetName:
        keywords.append("disp")
    if "agnp" in datasetName:
        keywords.append("agnp")
    if "cal" in datasetName:
        keywords.append("cal")
    if "agb" in datasetName:
        keywords.append("agb")
    return keywords


def get_sample_id_oct_2022(datasetName: str):
    # TODO: write a new method for finding the sampleId depending on how it is represented in the name
    sampleId = None
    if "agb" in datasetName:
        sampleId = datasetName[datasetName.find("agb") :].split("_")
        sampleId = sampleId[0]
    elif "kapton" in datasetName and "cal" not in datasetName:
        sampleId = datasetName[datasetName.find("kapton") :].split("_")
        if sampleId[1] == "ctrl":
            sampleId = sampleId[0] + "_" + sampleId[1]
        else:
            sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "B19" in datasetName:
        sampleId = datasetName[datasetName.find("B19") :].split("_")
        sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "B13" in datasetName:
        sampleId = datasetName[datasetName.find("B13") :].split("_")
        sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "disp" in datasetName:
        sampleId = datasetName[datasetName.find("disp") :].split("_")
        sampleId = sampleId[0]
    elif "agnp" in datasetName:
        sampleId = datasetName[datasetName.find("agnp") :].split("_")
        sampleId = sampleId[0]
        posIndex = sampleId.find("pos")
        if posIndex != -1:
            sampleId = sampleId[: sampleId.find("pos")]
    return sampleId
