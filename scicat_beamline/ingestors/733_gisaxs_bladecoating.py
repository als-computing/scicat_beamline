from datetime import datetime
import io
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import OrderedDict
from PIL import Image

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
    encode_image_2_thumbnail,
)
from scicat_beamline.utils import Issue
from dateutil import parser

ingest_spec = "733_gisaxs_bladecoating"

logger = logging.getLogger("scicat_ingest.733_SAXS")

# TODO: before ingestion change according to whether it is SAXS or WAXS, also add or change other relevant keywords e.g. "blade_coating"
global_keywords = ["SAXS", "ALS", "7.3.3", "scattering", "7.3.3 SAXS", "blade_coating"]
# TODO: change this before uploading. It is necessary for the description
PARENT_FOLDER = "2023-04_ALS733_gisaxs_bladecoating"


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    derived_folder: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    #TODO: change source folder, this is set like it is because the calibration folder is usually a few levels up
    # and the source folder must include it
    RAW_SOURCE_FOLDER = file_path.parent.parent
    #TODO: change name of calibration folder
    CALIBRATION_FOLDER = RAW_SOURCE_FOLDER/"gisaxs_calibrations"
    # TODO: change exclusion criteria
    if "_binned" in str(file_path.name):
        return

    scientific_metadata = OrderedDict()
    # Exclude patterns: https://stackoverflow.com/a/21502564
    # We want to exclude any txt files for beamstop testing
    # first ensure that "beamstop_test" isn't in the folder name
    assert "beamstop_test" not in file_path.name, "'beamstop_test' is in folder name, \
        can cause trouble if we have multiple txt files inside the folder, with some also having 'beamstop_test' in their name"

    txt_file = list(set(file_path.glob("*.txt")) - set(file_path.glob("*beamstop_test*")))
    assert len(txt_file) == 1
    txt_file = txt_file[0]

    add_to_sci_metadata_from_bad_headers(scientific_metadata, txt_file)

    basic_scientific_md = OrderedDict()

    # TODO: change based on project name before ingestion
    basic_scientific_md["project_name"] = "GAP C SNIPS"

    # TODO: change this before ingestion depending on how the institution is marked.
    if (basic_scientific_md["project_name"] == "calibration"):
        basic_scientific_md["institution"] = "lbnl"
    else:
        basic_scientific_md["institution"] = "texas"

    # TODO: change to transmission or grazing before ingestion
    basic_scientific_md["geometry"] = "grazing"
    # raise Exception("MUST SPECIFY GEOMETRY")

    basic_scientific_md.update(scientific_metadata)
    scientific_metadata = basic_scientific_md

    # TODO: change PI and owner before ingestion

    owner, email = get_owner_info_apr_2023(file_path)
    scicat_metadata = {
        "owner": owner,
        "email": email,
        "instrument_name": "ALS 7.3.3",
        "pi": "Greg Su",
        "proposal": "ALS-11839",
        # TODO: change techniques based on data
        "techniques": ["gisaxs", "blade_coating"],
        "derived_techniques": ["horizontal integration"],
    }

    # temporary access controls setup
    ownable = Ownable(
        ownerGroup="MWET",
        accessGroups=["ingestor", "MWET"],
        instrumentGroup="instrument-default"
    )

    dataset_id = upload_raw_dataset(
        scicat_client,
        file_path,
        scicat_metadata,
        scientific_metadata,
        ownable,
        RAW_SOURCE_FOLDER
    )
    upload_data_block(scicat_client, file_path, CALIBRATION_FOLDER, dataset_id, ownable, RAW_SOURCE_FOLDER)

    thumbnail_file = list(file_path.glob("*.gif"))
    assert len(thumbnail_file) == 1
    thumbnail_file = thumbnail_file[0]
    encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
    upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)

    create_derived(scicat_client, file_path, dataset_id, basic_scientific_md, scicat_metadata, thumbnail_dir, 
                   derived_folder)

    return dataset_id


def create_derived(scicat_client: ScicatClient, raw_file_path: Path, raw_dataset_id: str,
                   basic_scientific_md: OrderedDict, scicat_metadata: dict, thumbnail_dir: Path, derived_folder: Path):
    # TODO: change depending on analysis type
    ANALYSIS = "horizontal integration"
    # TODO: change job parameters depending on the parameters given to the script which creates the derived data
    jobParams = {
        "method_1": {
            "name": "np.nanmean"
        },
        "method_2": {
            "name": "scipy.interpolate.interp1d",
            "kind": "linear",
        }
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

    derived_name = raw_file_path.name
    # TODO: change where analysis path is
    derived_path = derived_folder/derived_name

    derived_files, total_size = create_data_files_list(derived_path)
    if len(derived_files) == 0:
        return

    datasetName = derived_name + "_"+ANALYSIS.upper().replace(" ", "_")
    description = build_search_terms(PARENT_FOLDER + " " + datasetName)

    creationTime = get_file_mod_time(derived_path)

    sci_md_keywords = [basic_scientific_md["project_name"], basic_scientific_md["institution"], basic_scientific_md["geometry"], scicat_metadata["proposal"]]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]
    basic_scientific_md["analysis"] = ANALYSIS

    sample_keywords = find_sample_keywords_apr_2023(derived_name)

    dataset = DerivedDataset(
        investigator=scicat_metadata.get("pi"),
        inputDatasets=[raw_dataset_id],
        usedSoftware=["jupyter notebook", "python", "matplotlib", "scipy.interpolate.interp1d"],
        #techniques=scicat_metadata.get('techniques')+scicat_metadata.get("derived_techniques"),
        owner=scicat_metadata.get('owner'),
        contactEmail=scicat_metadata.get('email'),
        datasetName=datasetName,
        type=DatasetType.derived,
        instrumentId="7.3.3",
        sourceFolder=derived_path.as_posix(),
        scientificMetadata=basic_scientific_md,
        jobParameters=jobParams,
        isPublished=False,
        description=description,
        keywords=list(set(global_keywords+sci_md_keywords+sample_keywords+["analysis", ANALYSIS])),  # TODO: before ingestion change keywords depending on type of analysis
        creationTime=creationTime,
        **ownable.dict(),
    )

    derived_id = scicat_client.datasets_create(dataset)

    thumbnail = Attachment(
            datasetId=derived_id,
            thumbnail=encode_thumbnail(derived_path/(derived_name + "_movie.gif"), imType="gif"),
            caption="horizontal linecut gif",
            **ownable.dict()
    )
    scicat_client.upload_attachment(thumbnail, datasetType="DerivedDatasets")
    data_block = OrigDatablock(
        datasetId=derived_id,
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
    source_folder
) -> str:
    "Creates a dataset object"
    sci_md_keywords = [scientific_metadata["project_name"], scientific_metadata["institution"], scientific_metadata["geometry"], scicat_metadata["proposal"]]
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]

    creationTime = str(parser.parse(fabio.open(next(file_path.glob("*.edf"))).header["Date"]))
    file_name = file_path.stem

    sampleId = get_sample_id_apr_2023(file_name)

    description = build_search_terms(PARENT_FOLDER + "_" + file_name)
    sample_keywords = find_sample_keywords_apr_2023(file_path.name)
    dataset = RawDataset(
        owner=scicat_metadata.get("owner"),
        contactEmail=scicat_metadata.get("email"),
        creationLocation=scicat_metadata.get("instrument_name"),
        #techniques=scicat_metadata.get("techniques"),
        datasetName=file_name,
        type=DatasetType.raw,
        instrumentId=scicat_metadata.get("instrument_name"),
        proposalId=scicat_metadata.get("proposal"),
        dataFormat="733",
        principalInvestigator=scicat_metadata.get("pi"),
        sourceFolder=str(source_folder),
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
    scicat_client: ScicatClient, file_path: Path, calibration_folder: Path,  dataset_id: str, ownable: Ownable, source_folder: Path
) -> OrigDatablock:
    "Creates a OrigDatablock of files"
    datafiles, data_size = create_data_files_list(file_path, recursive=True, relativeTo=source_folder)
    cal_files, cal_size = create_data_files_list(calibration_folder, recursive=True, relativeTo=source_folder)

    total_size = data_size + cal_size
    datafiles += cal_files

    datablock = OrigDatablock(
        datasetId=dataset_id,
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


def get_owner_info_apr_2023(file_path: Path):
    owner = "Noah Wamble"
    email = "noah.wamble@utexas.edu"

    if "19064k" in str(file_path):
        owner = "Matt Landsman"
        email = "mrlandsman@lbl.gov"
    return owner, email


def get_sample_id_apr_2023(file_name: str):
    return file_name.split("_")[1]


def find_sample_keywords_apr_2023(file_name: str):
    keywords = set()
    if "ps4vp" in file_name.lower() or "psp4vp" in file_name.lower():
        keywords.add("psp4vp")
    if "cu" in file_name.lower():
        keywords.add("copper acetate")
    if "BPIV54" in file_name.lower():
        keywords.add("PS-PI-PS-P4VP")
        keywords.add("BP-IV-54")
    if "19064k" in file_name.lower():
        keywords.add("polymer source")
    return list(keywords)


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

