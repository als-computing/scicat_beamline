from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import OrderedDict

import fabio
from pyscicat.client import ScicatClient, encode_thumbnail
from pyscicat.model import (
    Attachment,
    OrigDatablock,
    DerivedDataset,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
)
from scicat_beamline.ingestors.common_ingestor_code import add_to_sci_metadata_from_bad_headers, create_data_files_list

from scicat_beamline.scicat_utils import (
    build_search_terms,
    build_waxs_saxs_thumb_733,
    encode_image_2_thumbnail,
)
from scicat_beamline.utils import Issue
from dateutil import parser

ingest_spec = "als733_saxs"

logger = logging.getLogger("scicat_ingest.733_SAXS")

# TODO: before ingestion change according to whether it is SAXS or WAXS
global_keywords = ["SAXS", "ALS", "7.3.3", "scattering", "7.3.3 SAXS"]  


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

    basic_scientific_md = OrderedDict()

    # TODO: change based on project name before ingestion
    basic_scientific_md["project_name"] = get_project_name_mar17_2023(file_path.name)

    # TODO: change this before ingestion depending on how the institution is marked.
    if (basic_scientific_md["project_name"] == "calibration"):
        basic_scientific_md["institution"] = "lbnl"
    else:
        basic_scientific_md["institution"] = "texas"

    # TODO: change to transmission or grazing before ingestion
    basic_scientific_md["geometry"] = "transmission"
    # raise Exception("MUST SPECIFY GEOMETRY")

    basic_scientific_md.update(scientific_metadata)
    scientific_metadata = basic_scientific_md

    # TODO: change PI before ingestion
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
        thumbnail_file = build_waxs_saxs_thumb_733(image_data, thumbnail_dir, edf_file.name)
        encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
        upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)
    
    create_derived(scicat_client, edf_file, dataset_id, basic_scientific_md, scicat_metadata)

    return dataset_id


def create_derived(scicat_client: ScicatClient, raw_file_path: Path, raw_dataset_id: str, basic_scientific_md: OrderedDict, scicat_metadata: dict):
    # TODO: change depending on analysis type
    ANALYSIS = "radial integration"
    # TODO: change job parameters depending on the parameters given to the script which creates the derived data
    jobParams = {
        "method": "pyFAI integrate1d",
        "npt": 2000,
        "azimuth_range": [-180, 180]
    }

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

    derived_name = get_analysis_dataset_name_mar17_2023(raw_file_path.name)

    derived_parent_folder = raw_file_path.parent/"analysis"

    derived_files, total_size = create_data_files_list(derived_parent_folder, excludeCheck=lambda path: derived_name not in path.name)
    if len(derived_files) == 0:
        return

    datasetName = derived_name + "_"+ANALYSIS.upper().replace(" ", "_")
    description = datasetName.replace("_", " ")

    creationTime = get_file_mod_time(Path(derived_parent_folder/derived_files[0].path))

    sci_md_keywords = [basic_scientific_md["project_name"], basic_scientific_md["institution"], basic_scientific_md["geometry"]]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]
    basic_scientific_md["analysis"] = ANALYSIS

    sample_keywords = find_sample_keywords_mar17_2023(derived_name)

    dataset = DerivedDataset(
        investigator=scicat_metadata.get("pi"),
        inputDatasets=[raw_dataset_id],
        usedSoftware=["jupyter notebook", "python", "matplotlib", "pyFAI"],
        owner=scicat_metadata.get('owner'),
        contactEmail=scicat_metadata.get('email'),
        datasetName=datasetName,
        type=DatasetType.derived,
        instrumentId="7.3.3",
        sourceFolder=derived_parent_folder.as_posix(),
        scientificMetadata=basic_scientific_md,
        jobParameters=jobParams,
        isPublished=False,
        description=description,
        keywords=list(set(global_keywords+sci_md_keywords+sample_keywords+["analysis", "reduced", ANALYSIS])),  # TODO: before ingestion change keywords depending on type of analysis
        creationTime=creationTime,
        **ownable.dict(),
    )

    derived_id = scicat_client.datasets_create(dataset)["pid"]

    #TODO: decide which thumbnail to use before ingestion
    thumbPath = None
    for file in derived_files:
        if "radint" in Path(file.path).name and Path(file.path).suffix == ".png":
            thumbPath = derived_parent_folder / file.path

    thumbnail = Attachment(
            datasetId=derived_id,
            thumbnail=encode_thumbnail(thumbPath),
            caption="radial integration graph",
            **ownable.dict()
    )

    scicat_client.upload_attachment(thumbnail, datasetType="DerivedDatasets")

    data_block = OrigDatablock(
        datasetId=derived_id,
        instrumentGroup="instrument-default",
        size=total_size,
        dataFileList=derived_files,
        **ownable.dict(),
    )

    scicat_client.upload_datablock(data_block, datasetType="DerivedDatasets")


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
    sci_md_keywords = [scientific_metadata["project_name"], scientific_metadata["institution"], scientific_metadata["geometry"]]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]

    creationTime = str(parser.parse(scientific_metadata["edf headers"]["Date"]))
    file_name = file_path.stem

    sampleId = get_sample_id_mar17_2023(file_name)

    description = build_search_terms(file_path.parent.name + "_" + file_name)
    sample_keywords = find_sample_keywords_mar17_2023(file_path.name)
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
        keywords=list(set(global_keywords + sci_md_keywords+sample_keywords)),
        creationTime=creationTime,
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
    "Creates a OrigDatablock of files"
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


def get_project_name_mar17_2023(datasetName):
    # TODO: write a new method for finding project name depending on how it is represented in the dataset name
    if "cal" in datasetName or "empty" in datasetName:
        return "calibration"
    if "mostafa" in datasetName:
        return "nasser_fouling"
    if "cameron" in datasetName:
        return "mckay_fouling"
    return "landsman_isoporous"


def find_sample_keywords_mar17_2023(datasetName):
    # TODO: write a new method for finding sample keywords depending on how the sample is represented in the dataset name
    keywords = set()
    if "thinfilm" in datasetName:
        keywords.add("thinfilm")
    if "capillary" in datasetName:
        keywords.add("capillary")
    if "kapton" in datasetName:
        keywords.add("kapton")
    if "agb" in datasetName:
        keywords.add("agb")
    if "DMF" in datasetName:
        keywords.add("DMF")
    if "DOX" in datasetName:
        keywords.add("DOX")
    if "THF" in datasetName:
        keywords.add("THF")
    if "PSP4VP" in datasetName:
        keywords.add("PSP4VP")
    if "directbeam" in datasetName:
        keywords.add("directbeam")
    return list(keywords)


def get_sample_id_mar17_2023(datasetName: str):
    # TODO: write a new method for finding the sampleId depending on how it is represented in the name
    sampleId = None
    if "capillary" in datasetName:
        sampleId = datasetName[datasetName.find("capillary"):].split("_")
        if (sampleId[1] == 'empty' or sampleId[1] == 'water'
           or "agb" in sampleId[1] or "mostafa" in sampleId[1] 
           or "DOX" in sampleId[1] or "DHF" in sampleId[1] 
           or "THF" in sampleId[1]):
            sampleId = sampleId[0] + "_" + sampleId[1]
        else:
            sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "thinfilm" in datasetName:
        sampleId = datasetName[datasetName.find("thinfilm"):].split("_")
        sampleId = sampleId[0] + "_" + sampleId[1]
    elif "directbeam" in datasetName:
        sampleId = "directbeam"
    elif "kapton" in datasetName:
        sampleId = datasetName[datasetName.find("kapton"):].split("_")
        if sampleId[1] == 'empty':
            sampleId = sampleId[0] + "_" + sampleId[1]
        else:
            sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    if sampleId is None:
        raise Exception(f"No sampleId created for name `{datasetName}`")
    return sampleId


def get_analysis_dataset_name_mar17_2023(raw_fname):
    # TODO: before ingestion change how the derived_name is generated, could be different depending on the script 
    # for generating the derived data
    derived_name = raw_fname[:raw_fname.find('_2m')]
    return derived_name


def get_analysis_dataset_name_oct_2022(raw_fname):
    # TODO: before ingestion change how the derived_name is generated, could be different depending on the script 
    # for generating the derived data
    derived_name = raw_fname[:raw_fname.find('_2m')]
    derived_name = derived_name[derived_name.find('texas_') + 6:]
    if 'disp' in derived_name:
        derived_name = derived_name[derived_name.find('disp'):]


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
        sampleId = datasetName[datasetName.find("agb"):].split('_')
        sampleId = sampleId[0]
    elif "kapton" in datasetName and "cal" not in datasetName:
        sampleId = datasetName[datasetName.find("kapton"):].split("_")
        if sampleId[1] == 'ctrl':
            sampleId = sampleId[0] + "_" + sampleId[1]
        else:
            sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "B19" in datasetName:
        sampleId = datasetName[datasetName.find("B19"):].split('_')
        sampleId = sampleId[0] + "_" + sampleId[1] + "_" + sampleId[2]
    elif "B13" in datasetName:
        sampleId = datasetName[datasetName.find("B13"):].split('_')
        sampleId = sampleId[0]+"_"+sampleId[1]+"_"+sampleId[2]
    elif "disp" in datasetName:
        sampleId = datasetName[datasetName.find("disp"):].split('_')
        sampleId = sampleId[0]
    elif "agnp" in datasetName:
        sampleId = datasetName[datasetName.find("agnp"):].split('_')
        sampleId = sampleId[0]
        posIndex = sampleId.find("pos")
        if posIndex != -1:
            sampleId = sampleId[:sampleId.find("pos")]
    return sampleId

