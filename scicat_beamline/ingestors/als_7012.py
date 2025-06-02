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

from scicat_utils import process_image
import datetime
import json
import os
import h5py
import numpy as np
import tempfile
from PIL import Image
from dotenv import load_dotenv
load_dotenv()

SCICAT_PASSWORD = os.getenv('SCICAT_PASSWORD')

# Create a SciCat client object.
client = ScicatClient(
    base_url="https://dataportal-staging.als.lbl.gov/api/v3/",
    username="admin",
    password=SCICAT_PASSWORD,
)


######## dummy dataset start
dummy =  RawDataset(datasetName='test name',  # errors without this, but from the API this doesn't seem like a required field
                    creationLocation='ALS 7.0.1.2', 
                    principalInvestigator='kate',
                    contactEmail='kate@kate.com',
                    creationTime=datetime.datetime.now().isoformat(),
                    owner='kate',
                    sourceFolder='source/folder/test',
                    ownerGroup='kate group')

if 0:
    try:
        response = client.datasets_create(dataset=dummy)
        print(response)
    except Exception as e:
        print(f'so sad. something went wrong \n{e}')

######## dummy dataset end


######## ptychography dataset start

data_path = '/Users/kkamdin/Data/cosmic/2024/06/240605'
file_name = 'NS_240605048_ccdframes_0_0.cxi'

ownable = Ownable(ownerGroup="kate", accessGroups=["kate_friends"])

dataset_required_fields = Dataset(type='derived',
                                  datasetName='240605048_0_0',
                                    contactEmail='kate@kate.com',
                                    creationTime=datetime.datetime.now().isoformat(),
                                    owner='kate',
                                    **ownable.model_dump(),
                                    sourceFolder=data_path)

new_dataset = DerivedDataset(investigator="kate",
                             inputDatasets=['240605048_raw_0', '240605048_raw_1', '240605048_raw_2'],
                             usedSoftware=['cdtools'],
                             jobParameters={'useful': 'metadata', 'about' : 'software params'},
                             jobLogData='wow you can put a whole job log file here',
                             **dataset_required_fields.model_dump()
                             )


if 0: # how to prevent dupliciate entries?
    dataset_id = None
    try:
        dataset_id = client.datasets_create(dataset=new_dataset) ## should return dataset_id if no error
        print(dataset_id)
    except Exception as e:
        print(f'so sad. something went wrong \n{e}')


dataset_id = 'als/d3edc03d-4f80-4dfd-82e4-f254cc824c16'  # copy/pasta from terminal output
if 0:  # hmm you can upload the same file twice as well
    if dataset_id is not None:
        try:
            # Create Datablock with DataFiles 
            # what is the deal with Datablock, OrigDatablock, CreateDatasetOrigDatablockDto? don't understand how these work together
            data_file = DataFile(path=os.path.join(data_path, file_name),
                                time=datetime.datetime.now().isoformat(), # error on upload without the time but doens't seem to be required in model
                                size=290000)  
            data_block_dto = CreateDatasetOrigDatablockDto(dataFileList=[data_file], size=290000)
            client.datasets_origdatablock_create(dataset_id, data_block_dto)
        except Exception as e:
            print(f'que triste. something went wrong \n{e}')


image = None
if os.path.exists(os.path.join(data_path, file_name)):
    f = h5py.File(os.path.join(data_path, file_name),'r')
    # from https://github.com/als-computing/pystxm-core/blob/main/src/pystxm_core/io/readCXI.py#L99-L123
    entryList = [str(e) for e in list(f['entry_1'])]
    currentImageNumber = str(len([e for e in list(f['entry_1']) if 'image' in e and 'latest' not in e]))
    if currentImageNumber == '0':
        print("Could not locate ptychography image data.")
        image = None
    else:
        print("Found %s images" %(int(currentImageNumber)))
        image = []
        for i in range(1,int(currentImageNumber) + 1):
            print("loading image: %s" %(i))
            try:
                #self.image.append(f['entry_1/image_' + str(i) + '/data_' + str(i) + '/data'][()])
                image.append(f['entry_1/image_'+str(i)+'/data'][()])
            except:
                pass

    if image is not None and image != []:
        print(f'found {len(image)} images')
        # get the last image because of this line
        # https://github.com/als-computing/pystxm-core/blob/main/src/pystxm_core/image.py#L75
        image = np.abs(image[-1]**2)  # image is complex so take the magnitude


thumbnail = None
if image is not None:
    n = 256  ## how are we picking this number??
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize=(10,10))
    ax = fig.add_subplot(111)
    ax.imshow(image[n:-n, n:-n])
    fig.show()

    thumbnail = image[n:-n, n:-n].copy()
    vmin, vmax = np.percentile(thumbnail, [2, 98])
    np.clip(thumbnail, vmin, vmax)

else:
    def create_smiley_face(size=256):
        # Create a zero array
        face = np.zeros((size, size))

        # Calculate center and scale factors
        center = size // 2
        eye_radius = size // 16
        eye_y_pos = center - size // 8
        left_eye_x = center - size // 4
        right_eye_x = center + size // 4

        # Create eyes
        y, x = np.ogrid[:size, :size]
        # Left eye
        left_eye_mask = (x - left_eye_x)**2 + (y - eye_y_pos)**2 <= eye_radius**2
        face[left_eye_mask] = 1
        # Right eye
        right_eye_mask = (x - right_eye_x)**2 + (y - eye_y_pos)**2 <= eye_radius**2
        face[right_eye_mask] = 1

        # Create smile
        smile_center_y = center + size // 6
        smile_radius = size // 3
        # Create an arc for the smile
        for i in range(size):
            for j in range(size):
                # Check if point is on the smile arc
                dist = np.sqrt((i - smile_center_y)**2 + (j - center)**2)
                if abs(dist - smile_radius) < 2:  # Thickness of smile line
                    # Only keep lower half of circle and within certain x-range
                    if i > smile_center_y and abs(j - center) < smile_radius:
                        face[i, j] = 1

        return face
    
    thumbnail = create_smiley_face(256)


if thumbnail is not None:
    #with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = '.'
    temp_path = os.path.join(tmpdir, 'temp_thumbnail.jpg')

    # Save numpy array as jpg
    Image.fromarray(thumbnail.astype(np.uint8)).convert('L').save(temp_path, 'JPEG')

    process_image(temp_path, os.path.join(tmpdir, 'temp_thumbnail_processed.jpg'))
    thumbnail_encoded = encode_thumbnail(os.path.join(tmpdir, 'temp_thumbnail_processed.jpg'))

    # # Load back with Pillow - do this before the temp dir is cleaned up
    # loaded_pil = Image.open(temp_path)
    # # Convert to RGB mode if needed and load into memory
    # loaded_pil = loaded_pil.convert('L')  # 'L' for grayscale
    # loaded_pil.load()  # Ensure the image is loaded into memory

# Now loaded_pil is available outside the temp directory scope
# You can convert it back to numpy if needed:
# loaded_arr = np.array(loaded_pil)

import base64
import copy
import io
thumbnail_encoded_copy = copy.copy(thumbnail_encoded)
# Remove header if present
if "base64," in thumbnail_encoded_copy:
    base64_str = thumbnail_encoded_copy.split("base64,")[1]

# Decode base64 to bytes
img_bytes = base64.b64decode(base64_str)

# Convert bytes to image
img = Image.open(io.BytesIO(img_bytes))

import matplotlib.pyplot as plt
fig = plt.figure(figsize=(10,10))
ax = fig.add_subplot(111)
ax.imshow(img)
fig.show()

#thumbnail_encoded = encode_thumbnail(thumb_path)

# # Create Attachment
# attachment = Attachment(
#     datasetId=dataset_id,
#     thumbnail=thumbnail_encoded,
#     caption="ptychography reconstruction",
#     **ownable.model_dump(),
# )
# client.upload_attachment(attachment)

######## ptychography dataset end

datasets = client.datasets_find()
print(f'found {len(datasets)} datasets:')
for d in datasets:
    print()
    temp = {}
    temp['_id'] = d.get('_id', '')
    temp['datasetName'] = d.get('datasetName', '')
    print(json.dumps(temp, indent=2))


print('RET to exit')
input()