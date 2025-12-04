# coding: utf-8
# h5tools: convenience functions for dealing with hdf5 and nexus files
# author: Sofya Laskina, Brian R. Pauw, I. Bressler
# date: 2022.01.10

import logging
from pathlib import Path

import h5py  # type: ignore
import numpy as np


def h5Get(filename, h5path: str, default="none", leaveAsArray=False):
    """
    Gets a single value or attribute from an HDF5 file, with added error checking and default handling.
    h5path is the string representation of the location in the hdf5 file, e.g. '/sasentry1/sasdata1/I'.
    If you want to extract an attribute, you can use the '@' symbol to split path and attribute name,
    e.g. '/sasentry1/sasdata1@timestamp' gets the timestamp attribute in sasdata1.
    """
    assert h5path is not None, "h5path must be specified"
    assert isinstance(h5path, str), "h5path must be a string"
    filename = Path(filename)
    assert filename.exists(), f"input filename {filename.as_posix()} cannot be found"
    attrKey = None
    if "@" in h5path:
        h5path, attrKey = h5path.split("@")

    with h5py.File(filename, "r") as h5f:
        try:
            if attrKey is None:
                val = h5f.get(h5path)[()]
            else:
                val = h5f.get(h5path).attrs[attrKey]

            val = h5py_casting(val)  # sofya added this line
            # logging.info('type val {} at key {}: {}'.format(val, h5path, type(val)))

        except TypeError:
            if attrKey is None:
                logging.warning(
                    f"cannot get value from file {filename.as_posix()} path {h5path}, setting to default"
                )
            else:
                logging.warning(
                    f"""cannot get value from file {filename.as_posix()} path {h5path},
                    attribute {attrKey} setting to default: {default}"""
                )

            val = default
    return val


def h5GetDict(filename, keyPaths: dict):
    """
    creates a dictionary with results extracted from an HDF5 file
    dictionary should have form:
    {h5path: default}
    """
    resultDict = {}
    for h5path, default in keyPaths.items():
        resultDict[h5path] = h5Get(filename, h5path, default=default)
    return resultDict


def h5py_casting(val, leaveAsArray=False):
    if isinstance(val, np.ndarray) and (not leaveAsArray):
        if val.size == 1:
            val = np.array([val.squeeze()])[0]
        else:
            if np.isnan(val).sum() + np.isinf(val).sum() == np.prod(
                [i for i in val.shape]
            ):
                # print('all elements are either nan or inf')
                val = "-"
            elif np.isnan(val.mean()) or np.isinf(val.mean()):
                # print('nan pixel at index', np.argwhere(np.isnan(val)))
                # print('inf pixel at index', np.argwhere(np.isinf(val)))
                val = np.mean(np.ma.masked_invalid(val))
            else:
                val = val.mean()
    """if isinstance( val, np.ndarray) and leaveAsArray: seems to take a lot of time
        val = val.tolist()
        return json.dumps(val, separators=(',', ':'), sort_keys=True, indent=4)"""
    if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
        val = "-"
    if isinstance(val, np.bytes_) or isinstance(val, bytes):
        val = val.decode("UTF-8")
    if isinstance(val, np.generic):
        val = val.item()
    if isinstance(val, str):
        if val[:2] == "b'":
            val = val[2:-1]
    return val
