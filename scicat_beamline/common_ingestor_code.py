import glob
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

from pyscicat.client import get_file_mod_time, get_file_size
from pyscicat.model import DataFile


class Severity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None


def glob_non_hidden_in_folder(folder: Path, pattern: str, recursive=False):
    """ "This code will return an iterator to all the non-hidden files in a folder according to
    the regex provided to argument `pattern.` if `recursive` is True it will allow for recursion in the glob pattern
    """
    return map(Path, glob.iglob(str(folder) + "/" + pattern, recursive=recursive))


def create_data_files_list(
    folder: Path, excludeCheck: Optional[Callable[[Path], bool]] = None, recursive=False
) -> Tuple[List[DataFile], int]:
    """Iterates over files in a directory and creates a list of files. It will exclude hidden files.
    It will also exclude directories.
    If the `excludeCheck` is passed in then it will check if it evaluates to true for each file in the directory
    and if it does, then that file will be excluded from the list. If `recursive` is set to true it will recursively iterate
    over all subdirectories, except hidden ones."""
    datafiles = []
    totalSize = 0

    for file in glob.iglob(str(folder) + "/**", recursive=recursive):
        file = Path(file)
        relativePathToFolder = file.relative_to(folder)
        if file.is_file() is False:
            continue
        if excludeCheck is None or excludeCheck(file) is False:
            datafile, datafileSize = create_data_file(
                file, relativePath=relativePathToFolder
            )
            datafiles.append(datafile)
            totalSize += datafileSize

    return datafiles, totalSize


def create_data_file(file: Path, relativePath=None) -> Tuple[DataFile, int]:
    """Creates datafile object and returns it along with the file size.
    `relativePath` is the path that will be passed to DataFile, and it should be relative to
    the root directory of the raw dataset. Default is `file.name`."""
    if relativePath is None:
        relativePath = file.name
    datafile = DataFile(
        path=str(relativePath),
        size=get_file_size(file),
        time=get_file_mod_time(file),
    )
    return datafile, get_file_size(file)


def add_to_sci_metadata_from_bad_headers(
    sci_md: dict, file_path: Path, when_to_stop: Optional[Callable[[str], bool]] = None
) -> None:
    """This function will scan through the lines within the file given by `file_path` and attempt to create key value pairs using : or = as a delimiter.
    If there are multiple of these in a single line or none of them then it will add the whole line as the value and create a key called unknown_field{count}.
    It will also do this if the text before the delimiter is empty.
    It will add these to the dict passed in through `sci_md`. Finally it will stop when the lambda `when_to_stop` returns True.
    If `when_to_stop` is None then it will go through the entire file."""
    unknown_cnt = 0
    with open(file_path) as txt_file:
        for line in txt_file.read().splitlines():
            if line.isspace() or line == "":
                continue
            if when_to_stop is not None and when_to_stop(line) is True:
                return
            parts = line.replace("=", ":").split(":")
            if len(parts) == 2 and parts[0] != "":
                value = sci_md.setdefault(parts[0], parts[1])
                if value != parts[1]:
                    if type(sci_md[parts[0]]) is list:
                        sci_md[parts[0]].append(parts[1])
                    sci_md[parts[0]] = [sci_md[parts[0]], parts[1]]
                continue
            sci_md[f"unknown_field{unknown_cnt}"] = line
            unknown_cnt += 1
