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
    ax.imshow(image[n:-n, n:-n], cmap='gray')
    ax.set_title('Original float 32 Image')
    fig.show()
    fig.savefig('original_float32.png')


    thumbnail = image[n:-n, n:-n].copy()
    vmin, vmax = np.percentile(thumbnail, [2, 98])
    np.clip(thumbnail, vmin, vmax)

else:
    # just for testing
    thumbnail = create_smiley_face(256)

print(f'thumbnail type: {type(thumbnail)}, {thumbnail.dtype}')

if thumbnail is not None:
    with tempfile.TemporaryDirectory() as tmpdir:
        fig = plt.figure(figsize=(10,10))
        # Add an axes that fills the entire figure
        ax = fig.add_axes([0, 0, 1, 1])
        # Turn off the axes
        ax.set_axis_off()
        ax.imshow(thumbnail, cmap='gray')
        fig.show()
        fig.savefig(os.path.join(tmpdir, 'thumbnail.png'))

        thumbnail_encoded = encode_thumbnail(os.path.join(tmpdir, 'thumbnail.png'))

        # Create Attachment
        attachment = Attachment(
            datasetId=dataset_id,
            thumbnail=thumbnail_encoded,
            caption="ptychography reconstruction",
            **ownable.model_dump(),
        )
        client.upload_attachment(attachment)

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