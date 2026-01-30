import logging
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import fabio
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (Attachment, CreateDatasetOrigDatablockDto,
                            DataFile, DatasetType, DerivedDataset,
                            OrigDatablock, Ownable, RawDataset)
from dataset_metadata_schemas.dataset_metadata import Als, SciCat, FileManifest, Container as DatasetMetadataContainer
from dataset_metadata_schemas.utilities import get_nested
from dataset_tracker_client.client import DatasettrackerClient

from scicat_beamline.thumbnails import (build_waxs_saxs_thumb_733,
                                        encode_image_2_thumbnail)
from scicat_beamline.utils import (Issue, add_to_sci_metadata_from_key_value_text,
                                   search_terms_from_name, create_data_files_list, file_manifest_from_folder, file_manifest_from_files,
                                   get_file_mod_time, get_file_size)

ingest_spec = "als733_saxs"

logger = logging.getLogger("scicat_operation")


global_keywords = [
    "SAXS",
    "ALS",
    "7.3.3",
    "scattering",
    "7.3.3 SAXS",
]  # TODO: before ingestion change according to SAXS/WAXS


def ingest(
    scicat_client: ScicatClient,
    dataset_path: Path,
    file_manifest: FileManifest,
    temp_dir: Path,
    als_dataset_metadata: Optional[DatasetMetadataContainer] = None,
    owner_username: Optional[str] = None,
) -> DatasetMetadataContainer:

    if dataset_path is None:
        raise ValueError("Must provide a dataset_path for this ingester")
    # If we got no list of files, we grab a listing from dataset_path
    if dataset_files is None:
        file_manifest = file_manifest_from_folder(dataset_path, recursive=True)
    else:
        file_manifest = file_manifest_from_files(dataset_path, dataset_files)

    # Easier to work directly with the full Path objects in the code below.
    dataset_files = [Path(dataset_path, f.path) for f in file_manifest.files]

    # We expect to encounter one .txt file.
    # If we don't find exactly one, we raise an error.
    txt_files: List[Path] = []
    for file_path in dataset_files:
        if file_path.suffix.lower() == ".txt":
            txt_files.append(file_path)    
    if len(txt_files) != 1:
        raise ValueError(f"Expected one .txt file, found {len(txt_files)}")
    txt_file = txt_files[0]

    # We look through dataset_files for an .edf file with the same base name as the .txt file
    edf_file = None
    for f in dataset_files:
        if f.suffix.lower() == ".edf" and f.stem == txt_file.stem:
            edf_file = f
            break

    scientific_metadata = OrderedDict()
    if edf_file:
        with fabio.open(edf_file) as fabio_obj:
            image_data = fabio_obj.data
            scientific_metadata["edf headers"] = fabio_obj.header
    add_to_sci_metadata_from_key_value_text(scientific_metadata, txt_file)

    basic_scientific_md = OrderedDict()

    # This used to account for another institution, "texas", but now we assume LBNL.
    basic_scientific_md["institution"] = "lbnl"

    # TODO: This is very sus.  We need a better way to get a project name.
    basic_scientific_md["project_name"] = "SNIPS membranes"

    # TODO: Change to transmission or grazing before ingestion?
    basic_scientific_md["geometry"] = "transmission"
    # raise Exception("MUST SPECIFY GEOMETRY")

    scientific_metadata.update(basic_scientific_md)

    # TODO: Very sus. Just matching lines in the text file.
    proposal_name = scientific_metadata.get("ALS Proposal #", "UNKNOWN")
    principal_investigator = scientific_metadata.get("PI", "UNKNOWN")

    scicat_metadata = {
        "owner": "Garrett Birkel",      # TODO: Definitely not correct!
        "email": "gwbirkel@lbl.gov",    # TODO: Definitely not correct!
        "instrument_name": "7.3.3",     # TODO: Is this correct?
        "proposal": proposal_name,
        "pi": principal_investigator,
    }

    # temporary access controls setup
    ownable = Ownable(
        ownerGroup = proposal_name.replace(" ", "_"), # TODO: Also hella sus.
        accessGroups=["ingestor", proposal_name.replace(" ", "_")],
    )

    sci_md_keywords = [
        scientific_metadata["project_name"],
        scientific_metadata["institution"],
        scientific_metadata["geometry"],
    ]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]

    file_mod_time = get_file_mod_time(txt_file)
    file_name = txt_file.stem

    sampleId = get_sample_id_oct_2022(file_name)

    description = search_terms_from_name(txt_file.parent.name + "_" + file_name)
    sample_keywords = find_sample_keywords_oct_2022(txt_file.name)
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
        sourceFolder=str(txt_file.parent),
        scientificMetadata=scientific_metadata,
        sampleId=sampleId,
        isPublished=False,
        description=description,
        keywords=global_keywords + sci_md_keywords + sample_keywords,
        creationTime=file_mod_time,
        **ownable.model_dump(),
    )
    scicat_dataset_id = scicat_client.datasets_create(dataset)
    logger.info(f"Created dataset with id {scicat_dataset_id} for file {txt_file.name}")
    
    datafiles = data_file_dtos_from_manifest(file_manifest)

    datablock = CreateDatasetOrigDatablockDto(
        size=file_manifest.total_size_bytes,
        dataFileList=datafiles,
    )
    _ = scicat_client.datasets_origdatablock_create(scicat_dataset_id, datablock)
    logger.info(
        f"Created datablock for dataset id {scicat_dataset_id} for file {txt_file.name}"
    )

    if edf_file:
        thumbnail_file = build_waxs_saxs_thumb_733(
            image_data, temp_dir, edf_file.name
        )
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(
            scicat_client,
            encoded_thumbnail,
            dataset_id=scicat_dataset_id,
            caption="scattering image",
            ownable=ownable,
        )

    #
    # TODO: This presents problems, but luckily we aren't expecting derived data.
    #
    #create_derived(
    #    scicat_client, edf_file, scicat_dataset_id, basic_scientific_md, scicat_metadata
    #)

    # In the SciCat object, the only thing we'll set in here is the scicat_dataset_id.
    # The rest is set by the main ingester function we return to.

    if not als_dataset_metadata:
        als_dataset_metadata = DatasetMetadataContainer(als=Als())

    als_dataset_metadata.als.beamline_id = "7.3.3"
    als_dataset_metadata.als.proposal_id = proposal_name
    als_dataset_metadata.als.principal_investigator = principal_investigator
    als_dataset_metadata.als.file_manifest = file_manifest

    if get_nested(als_dataset_metadata, "als.scicat") is None:
        als_dataset_metadata.als.scicat = SciCat()

    als_dataset_metadata.als.scicat.scicat_dataset_id = scicat_dataset_id
    return als_dataset_metadata


# Not used
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


def data_file_dtos_from_manifest(file_manifest: FileManifest) -> List[DataFile]:
    "Collects all txt and edf files"
    data_files = []
    for file in file_manifest.files:
        datafile = DataFile(
            path=file.path,
            size=file.size_bytes,
            time=file.date_last_modified,
            type="RawDatasets",
        )
        data_files.append(datafile)
    logger.info(f"Allocated {len(data_files)} data files")
    return data_files


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
