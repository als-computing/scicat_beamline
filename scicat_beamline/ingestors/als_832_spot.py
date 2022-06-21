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


ingest_spec = "als_832_spot"

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
        scientific_metadata = build_scientific_metadata(appmetadata_doc, spot_fields)

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
        owner=fs_doc.get("/measurement/sample/experiment/pi") or "unknown",
        contactEmail=fs_doc.get("/measurement/sample/experimenter/email") or "unkown",
        creationLocation=fs_doc.get("/measurement/instrument/instrument_name") or "unkown",
        datasetName=file_name,
        type=DatasetType.raw,
        instrumentId=fs_doc.get("/measurement/instrument/instrument_name") or "unkown",
        proposalId=fs_doc.get("/measurement/sample/experiment/proposal") or "unkown",
        dataFormat="spot",
        principalInvestigator=fs_doc.get("/measurement/sample/experiment/pi") or "unkown",
        sourceFolder=str(file.parent),
        size=file_size,
        scientificMetadata=scientific_metadata,
        sampleId=file.stem,
        isPublished=False,
        description=description,
        keywords=["spot", "8.3.2"],
        creationTime=file_mod_time,
        **ownable.dict(),
    )

    dataset_id = scicat_client.upload_raw_dataset(dataset)
    return dataset_id






scientific_metadata_keys = {
    "/measurement/instrument/attenuator/setup/filter_y",
    "/measurement/instrument/camera_motor_stack/setup/tilt_motor",
    "/measurement/instrument/detection_system/objective/camera_objective",
    "/measurement/instrument/detection_system/scintillator/scintillator_type",
    "/measurement/instrument/detector/binning_x",
    "/measurement/instrument/detector/binning_y",
    "/measurement/instrument/detector/dark_field_value",
    "/measurement/instrument/detector/delay_time",
    "/measurement/instrument/detector/dimension_x",
    "/measurement/instrument/detector/dimension_y",
    "/measurement/instrument/detector/model",
    "/measurement/instrument/detector/pixel_size",
    "/measurement/instrument/detector/temperature",
    "/measurement/instrument/monochromator/setup/Z2",
    "/measurement/instrument/monochromator/setup/temperature_tc2",
    "/measurement/instrument/monochromator/setup/temperature_tc3",
    "/measurement/instrument/slits/setup/hslits_A_Door",
    "/measurement/instrument/slits/setup/hslits_A_Wall",
    "/measurement/instrument/slits/setup/hslits_center",
    "/measurement/instrument/slits/setup/hslits_size",
    "/measurement/instrument/slits/setup/vslits_Lead_Flag",
    "/measurement/instrument/source/source_name",
    "/process/acquisition/dark_fields/dark_num_avg_of",
    "/process/acquisition/dark_fields/num_dark_fields",
    "/process/acquisition/flat_fields/i0_move_x",
    "/process/acquisition/flat_fields/i0_move_y",
    "/process/acquisition/flat_fields/i0cycle",
    "/process/acquisition/flat_fields/num_flat_fields",
    "/process/acquisition/flat_fields/usebrightexpose",
    "/process/acquisition/mosaic/tile_xmovedist",
    "/process/acquisition/mosaic/tile_xnumimg",
    "/process/acquisition/mosaic/tile_xorig",
    "/process/acquisition/mosaic/tile_xoverlap",
    "/process/acquisition/mosaic/tile_ymovedist",
    "/process/acquisition/mosaic/tile_ynumimg",
    "/process/acquisition/mosaic/tile_yorig",
    "/process/acquisition/mosaic/tile_yoverlap",
    "/process/acquisition/name",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/multiRev",
    "/process/acquisition/rotation/nhalfCir",
    "/process/acquisition/rotation/num_angles",
    "/process/acquisition/rotation/range",
}

spot_fields = {
   "multiRev": "/process/acquisition/rotation/multiRev",
   "Horiz_Slit_A_Door": "/measurement/instrument/slits/setup/hslits_A_Door",
   "max_ring_size": "/process/tomo_rec/setup/algorithm/max_ring_size",
   "normalize_by_ROI": "/process/tomo_rec/setup/algorithm/normalize_by_ROI",
   "threshold": "/process/tomo_rec/setup/algorithm/threshold",
   "upp_ring_value": "/process/tomo_rec/setup/algorithm/upp_ring_value",
   "cooler_target": "/measurement/instrument/detector/temperature",
   "stepdeg": "?",
   "rfilter": "?",
   "exptime": "?",
   "cdmaterial": "?",
   "noisesig": "?",
   "rzsize": "?",
   "lens_name": "/measurement/instrument/detection_system/objective/camera_objective",
   "cammode": "?",
   "blur_limit": "/process/acquisition/rotation/blur_limit",
   "senergy": "/measurement/instrument/monochromator/energy",
   "rzdist": "?",
   "brightexptime": "/process/acquisition/flat_fields/flat_field_exposure",
   "obstime": "/measurement/instrument/detector/exposure_time",
   "num_dark_fields": "/process/acquisition/dark_fields/num_dark_fields",
   "rforder": "?",
   "normalization_ROI_right": "/process/tomo_rec/setup/algorithm/normalization_ROI_right",
   "xtechdir": "?",  # "c:ctstandards",
   "dark_num_avg_of": "/process/acquisition/dark_fields/dark_num_avg_of",
   "postImageDelay": "/measurement/instrument/detector/delay_time",
   "simple_ring_removal": "/process/tomo_rec/setup/algorithm/simple_ring_removal",
   "parentgroup": "?",
   "arange": "/process/acquisition/rotation/range",
   "Mono_Energy": "?", # "Inf",
   "dzfov": "?", # "3.481616",
   "diglev": "?", # "0",
   "Reconstruction_Type": "/process/tomo_rec/setup/algorithm/reconstruction_type",
   "Lead_Flag":"/measurement/instrument/slits/setup/vslits_Lead_Flag",
   "new_dirs_per_scan": "?",
   "Filter_Motor": "/measurement/instrument/attenuator/setup/filter_y",
   "dxdist":"?", #  "0.000000",
   "rot_angle":"?", #  "0.000000",
   "tile_xnumimg": "/process/acquisition/mosaic/tile_xnumimg",
   "ring_threshold":"/process/tomo_rec/setup/algorithm/ring_threshold",
   "i0hmove": "/process/acquisition/flat_fields/i0_move_x",
   "pfilegeom": "?", # "RADIOGRAPH",
   "radius": "/process/tomo_rec/setup/algorithm/radius",
   "evalystrt": "?", #"0",
   "rxsize": "?", # "0.000000",
   "Dark_Offset": "/measurement/instrument/detector/dark_field_value",
   "dzelements": "/measurement/instrument/detector/dimension_x", 
   "time_stamp": "/measurement/instrument/time_stamp",  
   "ybin":"/measurement/instrument/detector/binning_y",
   "Z2": "/measurement/instrument/monochromator/setup/Z2", # "232.380000",
   "output_scaling_max_value": "/process/tomo_rec/setup/algorithm/output_scaling_max_value", 
   "pzdist": "?", # "0.001303",
   "TC2": "?", # "0.000000",
   "rorder": "?", # "45",
   "TC3": "?", # "0.000000",
   "TC0": "?", # "0.000000",
   "TC1": "?", # "0.000000",
   "optics_type": "/measurement/instrument/detection_system/name",
   "low_ring_value":"/process/tomo_rec/setup/algorithm/low_ring_value", # "-100.000000",
   "scan_then_tile": "?", #"1",
   "xoffset": "?", #"1",
   "stage": "?", #"rc",
   "dxfov": "?", #"5.222424",
   "pycenter": "?", # "0.000000",
   "tile_xorig": "/process/acquisition/mosaic/tile_xorig",
   "pzcenter": "?", #  "0.000000",
   "normalization_ROI_bottom": "/process/tomo_rec/setup/algorithm/normalization_ROI_bottom",
   "axis4pos": "/measurement/instrument/sample_motor_stack/setup/sample_y",
   "phase_filt": "/process/tomo_rec/setup/algorithm/phase_filt", 
   "stype": "?", # "",
   "output_type": "/process/tomo_rec/setup/algorithm/output_type",
   "tile_yoverlap": "/process/acquisition/mosaic/tile_yoverlap",
   "stgsel": "?", # "0",
   "pydist": "?", # "0.001303",
   "ofactor": "?", # "1",
   "sdate":"?", #  "2018-01-31 14:40:15",
   "dxsize": "?", # "",
   "dtype": "?", # "",
   "bgeometry": "?", # "PARALLEL",
   "evalxstrt":"?", #  "0",
   "dataset": "/process/acquisition/name",
   "output_scaling_min_value": "/process/tomo_rec/setup/algorithm/output_scaling_min_value",
   "naverages": "?", #  "1",
   "dzdist": "?", #  "0.000000",
   "cddepth": "?", #  10.000000",
   "normalization_ROI_left": "/process/tomo_rec/setup/algorithm/normalization_ROI_left",
   "Beam_Current": "/measurement/instrument/source/current",
   "auto_eval_roi": "?", #  "1",
   "axis1pos": "/measurement/instrument/sample_motor_stack/setup/axis1pos",
   "Horiz_Slit_Size": "/measurement/instrument/slits/setup/hslits_size",
   "remove_outliers":  "/process/tomo_rec/setup/algorithm/remove_outliers",
   "archdir":  "?", #  "R:\fpanerai20180131_143930_tamdakht_compressionHT_s6_600C_0um",
   "nhalfCir":  "?", #  "1",
   "dzsize":  "?", #  "",
   "turret1":  "/measurement/instrument/monochromator/setup/turret1",
   "rydist":  "?", #  "0.000000",
   "turret2":  "/measurement/instrument/monochromator/setup/turret2",
   "pxsize":  "/measurement/instrument/detector/pixel_size",
   "Izero":  "/measurement/instrument/source/beam_intensity_incident",
   "nangles":  "/process/acquisition/rotation/num_angles",
   "yoffset": "?", # "1",
   "delta": "/process/tomo_rec/setup/algorithm/delta",
   "pysize": "?", # "0.001303",
   "cdxsize": "?", # "variable",
   "distance": "?", # "20.000000",
   "axis3pos": "/measurement/instrument/sample_motor_stack/setup/sample_x",
   "pgeometry": "?", # "PARALLEL",
   "tile_xmovedist": "/process/acquisition/mosaic/tile_xmovedist",
   "pxcenter": "?", # "0.000000",
   "rfile": "?", # "recobj",
   "ring_removal_method":"/process/tomo_rec/setup/algorithm/ring_removal_method",
   "exclude_selected_projections": "/process/tomo_rec/setup/algorithm/exclude_selected_projections",
   "nslices": "/measurement/instrument/detector/dimension_y",
   "rfcutoff": "?", # "0.500000",
   "cdzsize": "?", # "variable",
   "kernel_size": "?", # "0.000000",
   "tile_ynumimg": "?", # "0",
   "scanner": "/measurement/instrument/source/source_name",
   "axis5pos": "/measurement/instrument/sample_motor_stack/setup/axis5pos",
   "projection_mode": "/process/acquisition/name",
   "axis2pos": "?", # "0.000000",
   "rzelements":"?", #  "2160",
   "stage_date": "/process/acquisition/start_date",
   "tilt": "/measurement/instrument/camera_motor_stack/setup/tilt_motor",
   "rxelements": "?", #  "2560",
   "dxelements": "?", #  "2560",
   "time_elapsed": "?", #  "0.141425",
   "cdtype":  "?", # "Remote Control",
   "num_bright_field": "/process/acquisition/flat_fields/num_flat_fields",
   "evalxsize": "?", # "0",
   "scintillator_name": "/measurement/instrument/detection_system/scintillator/scintillator_type",
   "usebrightexpose": "/process/acquisition/flat_fields/usebrightexpose",
   "i0cycle": "/process/acquisition/flat_fields/i0cycle",
   "evalysize":  "?", #"0",
   "pfile":  "?", #"atrad",
   "max_arc_length": "/process/tomo_rec/setup/algorithm/max_arc_length",
   "Camera_Z_Support":  "/measurement/instrument/camera_motor_stack/setup/camera_distance",
   "object":  "?", #"20180131_143930_tamdakht_compressionHT_s6_600C_0um",
   "camera_used":  "/measurement/instrument/detector/model",
   "tile_ymovedist": "/process/acquisition/mosaic/tile_ymovedist",
   "xbin": "/measurement/instrument/detector/binning_x",
   "nrays": "/measurement/instrument/detector/dimension_x", 
   "beta": "?",
   "experimenter": "/measurement/sample/experimenter/email",  # do we capture name?
   "cooler_on": "?", # "1",
   "axis6pos": "?", # "9.859455",
   "sfactor": "?", # "0.000000",
   "Horiz_Slit_Pos": "/measurement/instrument/slits/setup/hslits_center",
   "Horiz_Slit_A_Wall": "/measurement/instrument/slits/setup/hslits_A_Wall",
   "pzsize": "?", # "0.001303",
   "ryelements": "?", # "2560",
   "tile_xoverlap": "/process/acquisition/mosaic/tile_xoverlap",
   "rysize": "?", # "0.000000",
   "scurrent": "/measurement/instrument/source/current", 
   "tile_yorig": "/process/acquisition/mosaic/tile_yorig",
   "frsum": "?", # "0",
   "normalization_ROI_top": "/process/tomo_rec/setup/algorithm/normalization_ROI_top",
   "i0vmove": "/process/acquisition/flat_fields/i0_move_y",
   "rxdist": "?", # "0.000000",
   "pxdist": "?", # "0.001303"
 }


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
