import glob
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import numpy as np

from pyscicat.model import DataFile


UNKNOWN_EMAIL = "unknown@example.com"


class Severity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None


logger = logging.getLogger("scicat_operation")


class NPArrayEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.ndarray):
            return [None if np.isnan(item) or np.isinf(item) else item for item in o]
        return json.JSONEncoder.default(self, o)


def get_file_size(file_path: Path) -> int:
    return file_path.lstat().st_size


def get_file_mod_time(file_path: Path) -> str:
    return datetime.fromtimestamp(file_path.lstat().st_mtime).isoformat() + "Z"


def calculate_access_controls(username, beamline, proposal) -> Dict:

    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if proposal and proposal != "None":
        owner_group = proposal

    # make an access group list that includes the name of the proposal and the name of the beamline
    access_groups = []
    if beamline:
        # No quotes, spaces, or commas at the beginning or end
        beamline = re.sub(r'^["\'\s,]+|["\'\s,]+$', "", beamline.lower())
        # This is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it.
        # Temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub.
        if beamline == "bl832":
            beamline = "8.3.2"

        access_groups.append(beamline)
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        if username != beamline:
            access_groups.append(username)

    return {"owner_group": owner_group, "access_groups": access_groups}


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


def add_to_sci_metadata_from_key_value_text(
    sci_md: dict, file_path: Path, when_to_stop: Optional[Callable[[str], bool]] = None
) -> None:
    """This function will scan through the lines within the file given by `file_path` and attempt to create key value pairs using : or = as a delimiter.
    If there are multiple of these in a single line or none of them then it will add the whole line as the value and create a key called unknown_field{count}.
    It will also do this if the text before the delimiter is empty.
    It will add these to the dict passed in through `sci_md`. Finally it will stop when the lambda `when_to_stop` returns True.
    If `when_to_stop` is None then it will go through the entire file."""

    def set_value(k, v):
        value = sci_md.setdefault(k, v)
        if value != v:
            if type(sci_md[k]) is list:
                sci_md[k].append(v)
            sci_md[k] = [sci_md[k], v]

    unknown_cnt = 0
    with open(file_path) as txt_file:
        for line in txt_file.read().splitlines():
            if line.isspace() or line == "":
                continue
            if when_to_stop is not None and when_to_stop(line) is True:
                return
            parts = line.split("=")
            if len(parts) == 2 and (not parts[0].isspace()) and parts[0] != "":
                set_value(parts[0], parts[1])
                continue
            parts = line.split(":")
            if len(parts) == 2 and (not parts[0].isspace()) and parts[0] != "":
                set_value(parts[0], parts[1])
                continue
            sci_md[f"unknown_field{unknown_cnt}"] = line
            unknown_cnt += 1


def build_search_terms(sample_name):
    """extract search terms from sample name to provide something pleasing to search on"""
    terms = re.split("[^a-zA-Z0-9]", sample_name)
    description = [term.lower() for term in terms if len(term) > 0]
    return " ".join(description)


def clean_email(email: Any) -> str:
    """
    Clean the provided email address.

    This function ensures that the input is a valid email address.
    It returns a default email if:
      - The input is not a string,
      - The input is empty after stripping,
      - The input equals "NONE" (case-insensitive), or
      - The input does not contain an "@" symbol.

    Parameters
    ----------
    email : any
        The raw email value extracted from metadata.

    Returns
    -------
    str
        A cleaned email address if valid, otherwise the default unknown email.

    Example
    -------
    >>> clean_email("  user@example.com  ")
    'user@example.com'
    >>> clean_email("garbage")
    'unknown@example.com'
    >>> clean_email(None)
    'unknown@example.com'
    """
    # Check that the email is a string
    if not isinstance(email, str):
        logger.info(f"Input email is not a string. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    # Remove surrounding whitespace
    cleaned = email.strip()

    # Remove leading/trailing quotes, commas, and whitespace
    cleaned = re.sub(r'^["\'\s,]+|["\'\s,]+$', "", email)

    # Fallback if the email is empty, equals "NONE", or lacks an "@" symbol
    if not cleaned or cleaned.upper() == "NONE" or "@" not in cleaned:
        logger.info(f"Invalid email address. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    # Optionally, remove spaces from inside the email (typically invalid in an email address)
    cleaned = cleaned.replace(" ", "")

    # Final verification: ensure that the cleaned email contains "@".
    if "@" not in cleaned:
        logger.info(f"Invalid email address: {cleaned}. Returning {UNKNOWN_EMAIL}")
        return UNKNOWN_EMAIL

    return cleaned
