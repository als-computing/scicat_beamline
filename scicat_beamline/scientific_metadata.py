# coding: utf-8
# sciMeta: extract all metadata from a NeXus file and put it in a tree for upload as scientific metadata to SciCat
# author: Sofya Laskina, Brian R. Pauw
# date: 2022.01.10

import logging
from collections import abc
from pathlib import Path

# from unittest import skip # not sure where this import comes from
import h5py
# flake8: noqa: F401
import hdf5plugin  # ESRF's library that extends the read functionality of HDF5 files

from pyscicat.hdf5.h5tools import h5py_casting


def update_deep(dictionary: dict, path_update: dict) -> dict:
    """
    Update the main metadata dictionary with the new dictionary.
    """
    k = list(path_update.keys())[0]
    v = list(path_update.values())[0]
    if k not in dictionary.keys():
        dictionary[k] = v
    else:
        key_next = list(path_update[k].keys())[0]
        if key_next in dictionary[k].keys():
            dictionary[k] = update_deep(dictionary.get(k, {}), v)
        else:
            dictionary[k].update(v)
    return dictionary


def build_dictionary(levels, update_data: dict) -> dict:
    """
    Creates a json-like level based dictionary for the whole path starting from /entry1 or whatever the
    first child of the root in the datatree is.
    """
    for level in levels[::-1]:
        update_data = dict({level: update_data})
    return update_data


def unwind(
    h5f,
    parent_path,
    metadata,
    default="none",
    leaveAsArray=False,
    skipKeyList: list = [],
) -> dict:
    """
    Current_level is the operating level, that is one level higher that the collected data.
    """
    if isinstance(h5f.get(parent_path), abc.Mapping):
        new_keys = sorted(h5f.get(parent_path).keys())
        # remove unwanted items before we even get started on it
        keyList = [newKey for newKey in new_keys if newKey not in skipKeyList]

        for nk in keyList:
            unwind(h5f, "/".join([parent_path, nk]), metadata, skipKeyList=skipKeyList)
    else:
        try:
            val = h5f.get(parent_path)[()]
            val = h5py_casting(val, leaveAsArray)
        except (OSError, TypeError):
            logging.warning(
                f"file has no value at path {parent_path}, setting to default: {default}"
            )
            val = default

        attributes = {"value": val}
        try:
            attributes_add = h5f.get(parent_path).attrs
            a_key = attributes_add.keys()
            a_value = []
            for v in attributes_add.values():
                v = h5py_casting(v, leaveAsArray)
                a_value.append(v)
            attributes.update(dict(zip(a_key, a_value)))
        except (KeyError, AttributeError) as e:
            logging.warning(e)

        levels = parent_path.split("/")[1:]
        if list(attributes.keys()) == ["value"]:  # no attributes here
            nested_dict = val
        else:
            nested_dict = attributes.copy()
        if val != "":
            update_dict = build_dictionary(levels, nested_dict)
            metadata = update_deep(metadata, update_dict)


def scientific_metadata(
    filename, excludeRootEntry: bool = True, skipKeyList: list = []
) -> dict:
    """
    Goals:
    --
    Opens any HDF5 or nexus file and unwinds the structure to add up all the metadata and respective attributes.
    This adds the paths and structure as required for SciCat's "scientific metadata" upload, including units.

    Usage:
    --
    branches and keys to omit can be listed using the argument "skipKeyList". Example:
    scientificMetadata=scientific_metadata(Path('./my_file.h5'), skipKeyList=['sasdata1'])
    If the root branch is singular, it can be omitted from the output dictionary by setting
    excludeRootEntry to True

    """
    # ensure the filename argument is of class Path
    filename = Path(filename)
    assert (
        filename.exists()
    ), f"Input filename {filename.as_posix()} does not seem to exist."
    with h5py.File(filename, "r") as h5f:
        # let's see if we can do this simpler
        metadata = dict()  # .fromkeys(prior_keys)
        unwind(h5f, "/", metadata, skipKeyList=skipKeyList)

    # first metadata entry is empty, so enter one level deeper.
    if len(metadata.keys()) == 1 and list(metadata.keys())[0] == "":
        metadata = metadata[list(metadata.keys())[0]]

    if excludeRootEntry and (len(metadata.keys()) > 1):
        logging.warning(
            """root entry cannot be excluded when there are more than one in the HDF5 tree.
            excludeRootEntry flag will be ignored."""
        )
    if excludeRootEntry and (len(metadata.keys()) == 1):
        metadata = metadata[list(metadata.keys())[0]]

    return metadata
