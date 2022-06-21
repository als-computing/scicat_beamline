from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import os
import sys
from turtle import st
from typing import Any, Dict, List
from numpy import append

from pymongo import MongoClient

from pyscicat.client import (
    from_token,
    get_file_mod_time,
    get_file_size,
    ScicatClient
)

from pyscicat.model import (
    Datablock,
    DataFile,
    Dataset,
    DatasetType,
    Ownable,
    RawDataset
)
from scicat_beamline.utils import Issue
from splash_ingest.ingestors.scicat_utils import (
    build_search_terms,
    build_thumbnail,
    calculate_access_controls,
)


@dataclass
class IngestionStatus():
    spot_id = None
    file: str = None
    error: Exception = None
    pid: str = None
    dataset_loaded: bool = False
    datablock_loaded: bool = False


ingest_spec = "als_733_spot"

def spot_raw_cursor(db, end_station):
    query = {"$and": [
                        {"fs.stage": "raw"},
                        {"fs.dataset": {"$not" : {"$regex": "heartbeat"}}},
                        {"fs.end_station": end_station}
                    ]}
    docs = db.fsmetadata.find(query)
    return docs


def build_scientific_metadata(app_metadata_doc: Dict, spot_fields: Dict) -> Dict:
    sci_meta = {}
    for spot_key, spot_value in spot_fields.items():
        if spot_value == "?":
            sci_meta[spot_key] = app_metadata_doc.get(spot_key)
        else:
            sci_meta[spot_value] = app_metadata_doc.get(spot_key)
    return OrderedDict(sorted(sci_meta.items()))


def ingest(
    scicat_client,
    spot_doc
) -> IngestionStatus:
    status = IngestionStatus(spot_doc.get("_id"))
    fs_doc = spot_doc.get('fs')
    if not fs_doc:
        status.error(KeyError(f'fs file for {spot_doc.get("_id")}'))
        return status

    if not fs_doc.get('phyloc'):
        status.error(KeyError(f'not file for {spot_doc.get("_id")}'))
        return status


    # now_str = datetime.isoformat(datetime.utcnow()) + "Z"
    access_controls = calculate_access_controls(
        username,
        "8.3.2",
        "",
    )

    ownable = Ownable(
        ownerGroup=access_controls["owner_group"],
        accessGroups=access_controls["access_groups"],
    )

    appmetadata_doc = spot_doc.get('appmetadata')
    scientific_metadata = {}
    if appmetadata_doc:
        scientific_metadata = appmetadata_doc

    try:
        pid = upload_raw_dataset(
            scicat_client,
            fs_doc,
            scientific_metadata,
            ownable,
        )
        status.dataset_loaded = True
        status.pid = pid
    except Exception as e:
        status.error = e
        return status

    if not fs_doc.get('phyloc'):
        return status

    try:
        upload_data_block(scicat_client, fs_doc, pid, ownable)
        status.datablock_loaded = True
    except Exception as e:
        status.error = e
    return status


def upload_data_block(
    scicat_client: ScicatClient,
    fs_doc: Dict,
    dataset_id: str,
    ownable: Ownable
) -> Datablock:
    "Creates a datablock of fits files"
    path = Path(fs_doc.get('phyloc')).name
    size = fs_doc.get('size') or 0
    datafiles = []
    datafile = DataFile(
        path=path,
        size=size,
        type="RawDatasets",
    )
    if fs_doc.get('date'):
        datafile.time = fs_doc.get('data')

    datafiles.append(datafile)

    datablock = Datablock(
        datasetId=dataset_id,
        size=size,
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(datablock)


def upload_raw_dataset(
    scicat_client: ScicatClient,
    fs_doc: Dict,
    scientific_metadata: Dict,
    ownable: Ownable,
) -> str:
    "Creates a dataset object"
    file = Path(fs_doc.get("phyloc"))
    file_size = 0
    file_mod_time = 0
    file_name = file.stem
    if file.exists():
        file_size = get_file_size(file)
        file_mod_time = get_file_mod_time(file)

    description = build_search_terms(file_name)
    appended_keywords = description.split()
    appended_keywords.append("spot")
    dataset = RawDataset(
        owner=fs_doc.get("owner") or "unknown",
        contactEmail=fs_doc.get("/measurement/sample/experimenter/email") or "unkown",
        creationLocation=fs_doc.get("facility") or "unkown",
        datasetName=file_name,
        type=DatasetType.raw,
        instrumentId=fs_doc.get("end_station") or "unkown",
        proposalId="unkown",
        dataFormat="spot",
        principalInvestigator=fs_doc.get("ownrer") or "unknown",
        sourceFolder=str(file.parent),
        size=file_size,
        scientificMetadata=scientific_metadata,
        sampleId=file.stem,
        isPublished=False,
        description=description,
        keywords=["spot", "7.3.3"],
        creationTime=file_mod_time,
        **ownable.dict(),
    )

    dataset_id = scicat_client.upload_raw_dataset(dataset)
    return dataset_id




if __name__ == "__main__":
    spot_url = sys.argv[1]
    token = sys.argv[2]
    username = sys.argv[3]
    scicat_url = sys.argv[4]

    scicat_client = from_token(scicat_url, token)

    db = MongoClient(spot_url).alsdata
    spot_docs = spot_raw_cursor(db, "bl832")
    for doc in spot_docs:
        try:
            status = ingest(scicat_client, doc)
            with open('ingested.csv', 'a') as ingested_csv:
                ingested_csv.write(f'{status.spot_id}\t {status.pid}\t {status.file}\t {status.dataset_loaded}\t {status.datablock_loaded}\t {status.error}\n')

        except Exception as e:
            with open('errors.csv', 'a') as errors_csv:
                 errors_csv.write(f'{doc.get("_id")}, {e}')
            print(f"NOT ingested {doc['_id']}    {str(e)}")
