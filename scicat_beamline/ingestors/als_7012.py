from pyscicat.client import (
    ScicatClient, encode_thumbnail
)

from pyscicat.model import (
    Ownable,
    RawDataset,
    Dataset,
    DerivedDataset,
    DataFile,
    Attachment,
    CreateDatasetOrigDatablockDto
)

from scicat_beamline.scicat_utils import create_smiley_face
import datetime
import json
import os
import h5py
import numpy as np
import tempfile
from PIL import Image




def ingest(scicat_client: ScicatClient):
    #  what types of data are we ingesting?

    #  ------ | raw | processed | reconstructed
    #  stxm   | ?   |    yes    |     N/A
    #  ptycho | ?   |    yes    |     yes

    pass

def create_thumbnail(file_path : str):
    pass

def upload_thumbnail(client : ScicatClient, dataset_id : str):
    pass

def get_file_size(file_path : str):
    pass



if __name__ == "__main__":
    # client invoke doens't actually go in this this file but put this here during dev for testing
    SCICAT_ADDRESS = os.getenv('SCICAT_ADDRESS')
    SCICAT_USER = os.getenv('SCICAT_USER')
    SCICAT_PASSWORD = os.getenv('SCICAT_PASSWORD')

    # Create a SciCat client object.
    client = ScicatClient(
        base_url=SCICAT_ADDRESS,
        username=SCICAT_USER,
        password=SCICAT_PASSWORD,   
    )   