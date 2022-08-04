from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Union
import glob


class Severity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None


def glob_non_hidden_in_folder(folder: Path, pattern: str, recursive=False):
    """"This code will return an iterator to all the non-hidden files in a folder according to
    the regex provided to argument `pattern.` if `recursive` is True it will allow for recursion in the glob pattern"""
    return map(Path, glob.iglob(str(folder) + "/" + pattern, recursive=recursive))
