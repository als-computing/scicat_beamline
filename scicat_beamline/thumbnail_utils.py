import base64
import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict
from uuid import uuid4

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import numpy.typing as npt
import xarray as xr
from matplotlib.colors import LogNorm
from mpl_toolkits.axes_grid1.axes_divider import make_axes_locatable
from PIL import Image, ImageOps

logger = logging.getLogger("scicat_ingest")
can_debug = logger.isEnabledFor(logging.DEBUG)


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


def encode_filebuffer_image_2_thumbnail(filebuffer, imType="jpg"):
    logging.info("Creating thumbnail for dataset")
    header = "data:image/{imType};base64,".format(imType=imType)
    dataBytes = base64.b64encode(filebuffer.read())
    dataStr = dataBytes.decode("UTF-8")
    return header + dataStr


def build_thumbnail_as_filebuffer(image_array: npt.ArrayLike):
    image_array = image_array - np.min(image_array) + 1.001
    image_array = np.log(image_array)
    image_array = 205 * image_array / (np.max(image_array))
    auto_contrast_image = Image.fromarray(image_array.astype("uint8"))
    auto_contrast_image = ImageOps.autocontrast(auto_contrast_image, cutoff=0.1)
    # filename = str(uuid4()) + ".png"
    file = io.BytesIO()
    # file = thumbnail_dir / Path(filename)
    auto_contrast_image.save(file, format="png")
    file.seek(0)
    return file


def build_waxs_saxs_thumb_733(array: npt.ArrayLike, thumbnail_dir: Path, edf_name: str):
    # Taken from a Jupyter notebook by Matt Landsman
    matplotlib.use("Agg")
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
    matplotlib.use("agg")

    # Taken from a jupyter notebook by matt landsman
    fig, ax = plt.subplots()
    data_en = array.copy()
    data_en = xr.where(data_en < 1, 1, data_en)
    data_en.plot(norm=LogNorm(1, float(np.max(array))), cmap="viridis")
    if array.attrs.get("energy"):
        en_float = np.float64(array.energy)
        plt.title(
            "{}\n{}_{}eV_{}".format(filename, scan_id, en_float, array.rsoxs_config)
        )
        fname_plot = "{}".format(filename)
    else:
        plt.title("{}".format(filename))
        fname_plot = "{}".format(filename)

    save_plot = os.path.join(thumbnail_dir, fname_plot + ".png")
    fig.savefig(save_plot, bbox_inches="tight", dpi=300)
    plt.close()
