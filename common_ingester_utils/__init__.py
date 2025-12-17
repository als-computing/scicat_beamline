"""
SciCat-related code for different beamlines
"""

from common_ingester_utils.common_ingester_utils import (
    Issue, NPArrayEncoder, Severity, add_to_sci_metadata_from_bad_headers,
    build_search_terms, calculate_access_controls, clean_email,
    create_data_file, create_data_files_list, get_file_mod_time, get_file_size,
    glob_non_hidden_in_folder)

__version__ = "0.1.0"

__all__ = [
    "Severity",
    "Issue",
    "NPArrayEncoder",
    "get_file_size",
    "get_file_mod_time",
    "calculate_access_controls",
    "glob_non_hidden_in_folder",
    "create_data_files_list",
    "create_data_file",
    "add_to_sci_metadata_from_bad_headers",
    "build_search_terms",
    "clean_email"
]