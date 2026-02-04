import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyscicat.client import ScicatClient
from pyscicat.model import (Attachment, DataFile, Ownable)
from dataset_metadata_schemas.dataset_metadata import Als, SciCat, FileManifest, Container as DatasetMetadataContainer
from dataset_metadata_schemas.utilities import get_nested


class SciCatIngesterBase:
    """A base class for a SciCat ingester."""

    _scicat_client: ScicatClient
    _temp_dir: Path
    _logger: logging.Logger
    _als_dataset_metadata: Optional[DatasetMetadataContainer] = None


    def __init__(
            self,
            scicat_client: ScicatClient,
            temp_dir: Path,
            logger: logging.Logger = logging.getLogger("scicat_operation")) -> None:
        self._scicat_client = scicat_client
        self._temp_dir = temp_dir
        self._logger = logger


    def ingest(
            self,
            dataset_path: Path,
            file_manifest: FileManifest,
            als_dataset_metadata: Optional[DatasetMetadataContainer] = None,
            owner_username: Optional[str] = None
        ) -> DatasetMetadataContainer:
        "Ingest a dataset located at the given path. Override this method in subclasses."

        # Do this in your subclass
        self.use_or_create_als_metadata(als_dataset_metadata)

        self._logger.warning(f"This is the SciCat ingester base class. It will do nothing!")
        return self._als_dataset_metadata


    def use_or_create_als_metadata(
            self,
            als_dataset_metadata: Optional[DatasetMetadataContainer] = None
        ) -> DatasetMetadataContainer:
        "Uses the provided ALS metadata or creates a new one, and ensures it has a basic structure."

        if not als_dataset_metadata:
            als_dataset_metadata = DatasetMetadataContainer(als=Als())

        # We created it above for a new one, but it may not exist in the provided one.
        if get_nested(als_dataset_metadata, "als") is None:
            als_dataset_metadata.als = Als()

        if get_nested(als_dataset_metadata, "als.scicat") is None:
            als_dataset_metadata.als.scicat = SciCat()

        self._als_dataset_metadata = als_dataset_metadata
        return self._als_dataset_metadata


    #
    # Various helper methods
    #

    def data_file_dtos_from_manifest(self, file_manifest: FileManifest) -> List[DataFile]:
        "Makes a DataFile object suitable for sending to SciCat out of each file in the given manifest."
        data_files = []
        for file in file_manifest.files:
            datafile = DataFile(
                path=file.path,
                size=file.size_bytes,
                time=file.date_last_modified.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                type="RawDatasets",
            )
            data_files.append(datafile)
        return data_files


    def upload_thumbnail_attachment(
        self,
        encoded_thumbnail: str,
        dataset_id: str,
        caption: str,
        ownable: Ownable,
        dataset_type: str = "Datasets",
    ) -> Attachment:
        "Creates an attachment thumbnail"
        attachment = Attachment(
            datasetId=dataset_id,
            thumbnail=encoded_thumbnail,
            caption=caption,
            **ownable.model_dump(),
        )
        result = self._scicat_client.datasets_attachment_create(
            attachment, datasetType=dataset_type
        )
        self._logger.info(f'Created thumbnail attachment for dataset {dataset_id} with caption "{caption}"')
        return result


    def extract_h5_file_fields(self, file, keys) -> Dict[str, Any]:
        metadata = {}
        for md_key in keys:
            dataset = file.get(md_key)
            if not dataset:
                self._logger.warning(f"Metadata key not found: {md_key}")
                continue
            metadata[md_key] = self.get_h5_file_dataset_value(file[md_key])
        return metadata


    def get_h5_file_dataset_value(self, data_set):
        self._logger.debug(f"{data_set}  {data_set.dtype}")
        try:
            if "S" in data_set.dtype.str:
                if data_set.shape == (1,):
                    return data_set.asstr()[0]
                elif data_set.shape == ():
                    return data_set[()].decode("utf-8")
                else:
                    return list(data_set.asstr())
            else:
                if data_set.maxshape == (1,):
                    self._logger.debug(f"{data_set}  {data_set[()][0]}")
                    return data_set[()][0]
                else:
                    self._logger.debug(f"{data_set}  {data_set[()]}")
                    return data_set[()]
        except Exception:
            self._logger.exception("Exception extracting dataset value")
            return None


    def get_h5_file_data_sample(self, file, data_sample_keys, sample_size=10):
        data_sample = {}
        for key in data_sample_keys:
            data_array = file.get(key)
            if not data_array:
                continue
            step_size = int(len(data_array) / sample_size)
            if step_size == 0:
                step_size = 1
            sample = data_array[0::step_size]
            data_sample[key] = sample

        return data_sample