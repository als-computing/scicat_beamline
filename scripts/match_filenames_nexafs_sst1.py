# This script is for matching nexafs SST-1 file names with the scan_id
# given in the spreadsheet. It creates a new column called file_name. Each entry contains
# all the file names (comma separated) that were matched with that scan id

import glob
from pathlib import Path

import pandas

metadata_table = pandas.read_csv(
    "/home/j/programming/work/datasets/SAF-309867-ML/M-WET Fall 2022 Beamtime.csv",
    dtype=str,
)

i = 0

path_set = set()

for path in glob.glob("/home/j/programming/work/datasets/SAF-309867-ML/*"):
    i += 1
    path_name = Path(path).name
    pos = path_name.find("150V")
    if pos > -1:
        name_in_spread = path_name[0:pos]
        pos2 = name_in_spread.rfind("_")
        name_in_spread = (
            name_in_spread[0 : pos2 + 1] + "PEY_" + name_in_spread[pos2 + 1 :]
        )
        # print(name_in_spread)
        match = False
        for idx, entry in enumerate(metadata_table["scan_id"]):
            if str(entry).strip() == name_in_spread:
                print(metadata_table.loc[idx, "file_name"])
                if str(metadata_table.loc[idx, "file_name"]) == "nan":
                    metadata_table.loc[idx, "file_name"] = f"{path_name}"
                else:
                    metadata_table.loc[idx, "file_name"] = (
                        str(metadata_table.loc[idx, "file_name"]) + f",{path_name}"
                    )
                match = True

        last_row_idx = len(metadata_table) - 1
        if match is False:
            if str(metadata_table.loc[last_row_idx, "file_name"]) == "nan":
                metadata_table.loc[last_row_idx, "file_name"] = f"{path_name}"
            else:
                metadata_table.loc[last_row_idx, "file_name"] = (
                    str(metadata_table.loc[idx, "file_name"]) + f",{path_name}"
                )
    elif Path(path).suffix not in [".txt", ".csv", ".xlsx", ".log"]:
        if str(metadata_table.loc[last_row_idx, "file_name"]) == "nan":
            metadata_table.loc[last_row_idx, "file_name"] = f"{path_name}"
        else:
            metadata_table.loc[last_row_idx, "file_name"] = (
                str(metadata_table.loc[idx, "file_name"]) + f",{path_name}"
            )

        # print(i)

metadata_table.to_csv(
    "/home/j/programming/work/datasets/SAF-309867-ML/M-WET Fall 2022 Beamtime.csv",
    index=False,
)
