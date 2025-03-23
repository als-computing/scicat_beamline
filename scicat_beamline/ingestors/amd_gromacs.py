from dateutil import parser
import yaml
import pdf2image
import logging
from pathlib import Path
from typing import Dict, List
from pyscicat.client import ScicatClient
from pyscicat.model import (
    Attachment,
    OrigDatablock,
    RawDataset,
    DatasetType,
    Ownable,
)
from scicat_beamline.ingestors.common_ingestor_code import create_data_files_list

from scicat_beamline.scicat_utils import (
    encode_image_2_thumbnail,
)
from scicat_beamline.utils import Issue, glob_non_hidden_in_folder

ingest_spec = "amd_gromacs"

logger = logging.getLogger("scicat_ingest.amd_gromacs")


global_keywords = ["GROMACS", "molecular dynamics", "simulation"]  # TODO: before ingestion change


def ingest(
    scicat_client: ScicatClient,
    username: str,
    folder: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    thumbnail_dir = Path(thumbnail_dir)

    yamls = sorted(glob_non_hidden_in_folder(folder, '*.yaml'))
    if len(yamls) > 1:
        raise Exception("More than 1 yaml file found.")
    with open(yamls[0]) as yaml_stream:
        yaml_config = yaml.safe_load(yaml_stream)

    scientific_metadata = yaml_config["dataset"]

    # temporary access controls setup
    ownable = Ownable(
        ownerGroup="MWET",
        accessGroups=["ingestor", "MWET"],
    )

    dataset_id = upload_raw_dataset(
        scicat_client,
        folder,
        scientific_metadata,
        ownable,
    )
    upload_data_block(scicat_client, folder, dataset_id, ownable)

    pdf_file = sorted(folder.glob("*.pdf"))[0]

    pdf2image.convert_from_path(pdf_file, output_folder=thumbnail_dir, output_file=pdf_file.stem, single_file=True, fmt='png')

    thumbnail_file = thumbnail_dir / (pdf_file.stem + ".png")
    encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file, imType="png")
    upload_attachment(scicat_client, encoded_thumbnail, dataset_id, ownable)

    return dataset_id


def upload_raw_dataset(
    scicat_client: ScicatClient,
    folder: Path,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    sci_md_keywords = [scientific_metadata["projectName"], scientific_metadata["institution"]]
    keywords = scientific_metadata.pop("keywords")
    sci_md_keywords = [x for x in sci_md_keywords if x is not None]
    sampleId = "__".join(scientific_metadata.pop("material")["components"])

    scicat_metadata = {
        "datasetName": scientific_metadata.pop("datasetName"),
        "owner": scientific_metadata.pop("owner"),
        "contactEmail": scientific_metadata.pop("contactEmail"),
        "creationLocation": scientific_metadata.pop("creationLocation"),
        "creationTime": str(parser.parse(scientific_metadata.pop("creationTime"))),
        "principalInvestigator": scientific_metadata.pop("principalInvestigator"),
        "instrumentId": scientific_metadata.pop("instrumentId"),
        "proposalId": scientific_metadata.pop("proposalId"),
        "description": scientific_metadata.pop("description"),
        "sampleId": sampleId,
    }

    dataset = RawDataset(
        **scicat_metadata,
        type=DatasetType.raw,
        dataFormat="",
        sourceFolder=str(folder),
        scientificMetadata=scientific_metadata,
        isPublished=False,
        keywords=global_keywords + sci_md_keywords + keywords,
        **ownable.dict(),
    )
    dataset_id = scicat_client.upload_raw_dataset(dataset)
    return dataset_id


def upload_data_block(
    scicat_client: ScicatClient, folder: Path, dataset_id: str, ownable: Ownable
) -> OrigDatablock:
    "Creates a OrigDatablock of files"
    datafiles, size = create_data_files_list(folder, recursive=True)

    datablock = OrigDatablock(
        datasetId=dataset_id,
        size=size,
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
        caption="graph png",
        **ownable.dict(),
    )
    scicat_client.upload_attachment(attachment)
