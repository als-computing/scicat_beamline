from collections import OrderedDict
from datetime import datetime
import glob
import os
from pathlib import Path
from typing import List
from dateutil import parser
from matplotlib import pyplot as plt
import matplotlib
import numpy
import pandas
from pyscicat.client import encode_thumbnail

from pyscicat.client import (
    ScicatClient,
    get_file_mod_time,
    get_file_size,
)

from pyscicat.model import (
    OrigDatablock,
    DataFile,
    RawDataset,
    DatasetType,
    Ownable,
    Attachment
)
import pytz
from scicat_beamline.ingestors.common_ingestor_code import (
    add_to_sci_metadata_from_bad_headers,
)

from scicat_beamline.utils import Issue

ingest_spec = "nsls2_nexafs_sst1"


def ingest(
    scicat_client: ScicatClient,
    username: str,
    file_path: Path,
    thumbnail_dir: Path,
    issues: List[Issue],
) -> str:
    "Ingest a folder of nsls-ii sst-1 nexafs files"
     
    if os.path.isdir(file_path):
        print(f'Skipping \"{file_path}\" because it is a folder.')
        return
    
    #TODO: change what files we skip
    if "test" in file_path.name:
        print(f'Skipping \"{file_path}\" because it is a test file.')
        return
    
    if "IrefHOPG" in file_path.name:
        print(f'Skipping \"{file_path}\" because it is a IrefHOPG file')
        return
    
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
        instrumentGroup="instrument-default"
    )

    issues: List[Issue] = []

    lines_to_skip = 0
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if line.startswith("----"):
                lines_to_skip = line_num
                break
    
    
    table = pandas.read_table(file_path, skiprows=lines_to_skip, delim_whitespace=True)
    metadata_table_filename = glob.glob(str(file_path.parent) + "/*.csv")[0]
    metadata_table = pandas.read_csv(metadata_table_filename)
    metadata_row = None

    metadata_table = metadata_table.replace({numpy.nan: None})
    for idx, entry in enumerate(metadata_table["sample_name"]):
        sample_name = entry.strip()
        if sample_name in file_path.name:
            metadata_row = metadata_table.iloc[idx]
            break

    if metadata_row is None:
        raise Exception(f"WARNING: {file_path} does not have an associated entry in the csv. Skipping...")

    # https://stackoverflow.com/a/54403705/
    table = table.replace({numpy.nan: None})

    scientific_metadata = OrderedDict()

    if metadata_row is not None:
        # TODO: before ingestion change the keys used for the keywords depending on how they are labelled in the csv file
        scientific_metadata["saf_id"] = str(metadata_row["saf_id"])
        scientific_metadata["project_name"] = metadata_row["project"]
        scientific_metadata["institution"] = metadata_row["institution"].lower()
        if scientific_metadata["institution"] == "ut":
            scientific_metadata["institution"] = "texas"

        if scientific_metadata["institution"] not in ["lbnl", "texas", "ucsb"]:
            raise Exception("Unknown institution")
        scientific_metadata["sample_name"] = metadata_row["sample_name"]
        scientific_metadata["sample_keywords"] = metadata_row["sample_keywords"]

        # scientific_metadata["incident_angle"] = str(metadata_row["incident_angle"])
        scientific_metadata["element_edge"] = file_path.name.split("_")[1].split("1")[0]
        scientific_metadata["measurement"] = metadata_row["measurement"]

        # scientific_metadata["scan_id"] = metadata_row["scan_id"]

        scientific_metadata["notes"] = metadata_row["notes"]
        # scientific_metadata["x_coordinate"] = metadata_row["x_coordinate"]
        # scientific_metadata["bar_location"] = metadata_row["bar_location"]
        # scientific_metadata["z_coordinate"] = metadata_row["z_coordinate"]

        def modifyKeyword(key, keyword):
            #TODO: change how we construct saf keyword depending on spreadsheet
            if (key == "saf_id") and "SAF" not in keyword:
                return "SAF " + keyword
            if (key == "institution"):
                return keyword.lower()
            return keyword
        # TODO: before ingestion change the keys used for the keywords depending on which are available in the csv file
        appended_keywords = [
            modifyKeyword(key, scientific_metadata[key])
            for key in [
                "saf_id",
                "institution",
                "project_name",
            ]
            if scientific_metadata[key] is not None
            and str(scientific_metadata[key]).strip() != ""
        ]

        measurements = metadata_row["measurement"].split(',')
        measurements = [x.strip() for x in measurements if x.strip() != ""]
        sample_keywords = metadata_row["sample_keywords"].split(',')
        sample_keywords = [x.strip() for x in sample_keywords if x.strip() != ""]
        appended_keywords += ([str(metadata_row["proposal_id"])] + measurements + sample_keywords)

        # Remove empty values that we got from the spreadsheet
        empty_keys = []
        for key, value in scientific_metadata.items():
            if str(value).strip() == "" or value == None:
                empty_keys.append(key)
        for key in empty_keys:
            scientific_metadata.pop(key)

    add_to_sci_metadata_from_bad_headers(
        scientific_metadata,
        file_path,
        when_to_stop=lambda line: line.startswith("----"),
    )
    scientific_metadata.update(table.to_dict(orient="list"))

    parent_folder = file_path.parent.absolute()
    log_file_path_strings = glob.glob(str(parent_folder) + "/*.log")

    files_size = 0

    for log_path_string in log_file_path_strings:
        files_size += get_file_size(Path(log_path_string))

    files_size += get_file_size(file_path)
    file_name = file_path.name

    

    # description = file_name.replace("_", " ")'
    dataset = None
    if metadata_row is not None:
        #TODO: change how we get values from the spreadsheet based on how they are labeled
        dataset_time = pytz.timezone('US/Eastern').localize(get_time_Jul2023(file_path))
        dataset_time = dataset_time.astimezone(pytz.utc)
        dataset = RawDataset(
            owner=metadata_row["owner"],
            contactEmail=metadata_row["owner_email"],
            creationLocation="NSLS-II SST-1 NEXAFS",
            datasetName=file_name,  # + "_" + metadata_row["element_edge"],
            type=DatasetType.raw,
            instrumentId="SST-1 NEXAFS",
            proposalId=str(metadata_row["proposal_id"]),
            dataFormat="NSLS-II",
            principalInvestigator=metadata_row["project_PI"],
            sourceFolder=file_path.parent.as_posix(),
            scientificMetadata=scientific_metadata,
            sampleId=metadata_row["sample_id"],
            isPublished=False,
            description=metadata_row["sample_name"]+": " + metadata_row[
                "sample_description"
            ],  # + ". " + metadata_row["sample_description.1"],
            keywords=["NEXAFS", "NSLS-II", "SST-1", "SST-1 NEXAFS"] + appended_keywords,
            creationTime=str(dataset_time),
            **ownable.dict(),
        )
    else:
        raise Exception("No metadata_row")

    dataset_id = scicat_client.upload_raw_dataset(dataset)

    log_datafiles = []
    for log_path_str in log_file_path_strings:
        log_path_obj = Path(log_path_str)
        log_datafiles.append(
            DataFile(
                path=str(log_path_obj.relative_to(log_path_obj.parent)),
                size=get_file_size(log_path_obj),
                time=get_file_mod_time(log_path_obj),
                type="RawDatasets",
            )
        )

    datafiles = [
        DataFile(
            path=str(file_path.relative_to(file_path.parent)),
            size=get_file_size(file_path),
            time=get_file_mod_time(file_path),
            type="RawDatasets",
        ),
        *log_datafiles,
    ]

    graph_txt_path = get_graph_file_Jul2023(file_path)

    if not graph_txt_path.exists():
        print(f"Warning: {file_path} does not have a corresponding graph file.")
    else:
        files_size += get_file_size(graph_txt_path)
        datafiles += [DataFile(
            path=str(graph_txt_path.relative_to(file_path.parent)),
            size=get_file_size(graph_txt_path),
            time=get_file_mod_time(graph_txt_path),
            type="RawDatasets",
        )]
        thumb_path = create_graph_thumb(file_path, graph_txt_path, thumbnail_dir)
        attachment = Attachment(
            datasetId=dataset_id,
            thumbnail=encode_thumbnail(thumb_path),
            caption="Nexafs plot",
            **ownable.dict(),
        )
        scicat_client.upload_attachment(attachment)

    data_block = OrigDatablock(
        datasetId=dataset_id,
        size=files_size,
        dataFileList=datafiles,
        **ownable.dict(),
    )
    scicat_client.upload_datablock(data_block)
    return dataset_id, issues


# if __name__ == "__main__":
#     from pprint import pprint

#     folder = Path("/home/j/programming/work/Oct_2021_scattering/Nexafs")
#     for path in folder.iterdir():
#         print(path)
#         if not path.is_file():
#             continue
#         try:
#             dataset_id, issues = ingest(path)
#             print(f"Ingested {path} as {dataset_id}. Issues:")
#             pprint(issues)
#         except Exception as e:
#             print("ERROR:")
#             print(e)
#             print(f"Error ingesting {path} with {e}")
#             raise e

def create_graph_thumb(raw_file_path, norm_file_path: Path, thumbnail_directory):
    matplotlib.use('agg')
    fig, ax1 = plt.subplots(figsize=(8,6))
    data = numpy.loadtxt(norm_file_path)
    xas_energy = data[:,0]
    xas_pey_norm = data[:,1]
    ax1.plot(xas_energy, xas_pey_norm, '-')
    ax1.set_xlabel('Energy (eV)')
    ax1.set_ylabel('Transmission Intensity (arbitrary units)')
    # sizing = get_plot_sizing_info_Jul2023(raw_file_path)
    plt.autoscale(enable=True, tight=True)
    save_path = Path(thumbnail_directory)/(norm_file_path.stem + '.png')
    fig.savefig(save_path, bbox_inches="tight", dpi=300)
    plt.close()
    return save_path

# def get_plot_sizing_info_Jul2023(file_path):
#     # name = file_path.name
#     # if "C150" in name:
#     #     return [280,300]
#     # if "O150" in name:
#     #     return [520,570]
#     # if "Si150" in name:
#     #     return [1830,1910]
#     return None

def get_graph_file_Jul2023(file_path):
    raw_name = file_path.name
    components = raw_name.split('_')
    graph_name = components[0] + "_" + components[1].split('1')[0] + ".txt"
    return file_path.parent/"analysis"/graph_name


def get_time_Jul2023(file_path):
    time_info = None
    with open(file_path) as nexafs_file:
        for line_num, line in enumerate(nexafs_file, 1):
            if "created on" in line:
                time_info = line
                break
    
    time_info = time_info.split("created on")
    time = time_info[1].split("on")[0].strip()
    return parser.parse(time)