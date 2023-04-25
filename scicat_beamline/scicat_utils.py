import base64
import json
import logging
from pathlib import Path
import re
from typing import Dict
from uuid import uuid4

import numpy as np
import numpy.typing as npt
from PIL import Image, ImageOps
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
import os
import xarray as xr


logger = logging.getLogger("splash_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


class NPArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return [None if np.isnan(item) else item for item in obj]
        return json.JSONEncoder.default(self, obj)


def calculate_access_controls(username, beamline, proposal) -> Dict:
    # make an access group list that includes the name of the proposal and the name of the beamline
    access_groups = []
    # set owner_group to username so that at least someone has access in case no proposal number is found
    owner_group = username
    if beamline:
        access_groups.append(beamline)
        # username lets the user see the Dataset in order to ingest objects after the Dataset
        access_groups.append(username)
        # temporary mapping while beamline controls process request to match beamline name with what comes
        # from ALSHub
        if beamline == "bl832":
            access_groups.append("8.3.2")

    if proposal and proposal != "None":
        owner_group = proposal

    # this is a bit of a kludge. Add 8.3.2 into the access groups so that staff will be able to see it
    return {"owner_group": owner_group, "access_groups": access_groups}


def build_search_terms(sample_name):
    """exctract search terms from sample name to provide something pleasing to search on"""
    terms = re.split("[^a-zA-Z0-9]", sample_name)
    description = [term.lower() for term in terms if len(term) > 0]
    return " ".join(description)


def encode_image_2_thumbnail(filename, imType="jpg"):
    logging.info(f"Creating thumbnail for dataset: {filename}")
    header = "data:image/{imType};base64,".format(imType=imType)
    with open(filename, "rb") as f:
        data = f.read()
    dataBytes = base64.b64encode(data)
    dataStr = dataBytes.decode("UTF-8")
    return header + dataStr


def build_thumbnail(image_array: npt.ArrayLike, thumbnail_dir: Path):
    image_array = image_array - np.min(image_array) + 1.001
    image_array = np.log(image_array)
    image_array = 205 * image_array / (np.max(image_array))
    auto_contrast_image = Image.fromarray(image_array.astype("uint8"))
    auto_contrast_image = ImageOps.autocontrast(auto_contrast_image, cutoff=0.1)
    filename = str(uuid4()) + ".png"
    # file = io.BytesIO()
    file = thumbnail_dir / Path(filename)
    auto_contrast_image.save(file, format="PNG")
    return file


def build_waxs_saxs_thumb_733(array: npt.ArrayLike, thumbnail_dir: Path, edf_name: str):
    # Taken from a Jupyter notebook by Matt Landsman
    plt.use('Agg')
    array[array < 1] = 1
    fig, ax = plt.subplots()
    im = ax.imshow(
        array,
        norm=LogNorm(vmin=np.percentile(array, 25), vmax=np.percentile(array, 99.9)),
    )
    ax.set_title(edf_name)
    ax.axis("off")
    ax_divider = make_axes_locatable(ax)
    cax = ax_divider.append_axes("bottom", size="3%", pad="2%")
    plt.colorbar(im, cax=cax, orientation="horizontal")
    cax.set_xlabel("Scattering intensity (arbitrary units)", size=8)
    cax.xaxis.set_label_position("bottom")
    cax.xaxis.tick_bottom()
    cax.xaxis.set_tick_params(labelsize=8)
    fig.tight_layout()

    filename = str(uuid4()) + ".png"

    file = thumbnail_dir / Path(filename)
    fig.savefig(file, bbox_inches="tight", dpi=300)
    plt.close()
    return file


def build_RSoXS_thumb_SST1(
    array: npt.ArrayLike, filename: str, thumbnail_dir: Path, scan_id
):
    matplotlib.use('agg')

    # Taken from a jupyter notebook by matt landsman
    fig, ax = plt.subplots()
    data_en = array.copy()
    data_en = xr.where(data_en < 1, 1, data_en)
    data_en.plot(norm=LogNorm(1, float(np.max(array))), cmap="viridis")
    if array.attrs.get("energy"):
        en_float = np.float64(array.energy)
        plt.title("{}\n{}_{}eV_{}".format(filename, scan_id, en_float, array.rsoxs_config))
        fname_plot = "{}".format(filename)
    else:
        plt.title("{}".format(filename))
        fname_plot = "{}".format(filename)
    
    save_plot = os.path.join(thumbnail_dir, fname_plot + ".png")
    fig.savefig(save_plot, bbox_inches="tight", dpi=300)
    plt.close()
