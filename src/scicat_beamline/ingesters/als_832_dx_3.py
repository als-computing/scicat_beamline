import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import h5py
from pyscicat.model import (DatasetType, OrigDatablock, Ownable, RawDataset)
from dataset_metadata_schemas.dataset_metadata import FileManifest, FileManifestEntry, Container as DatasetMetadataContainer

from ingester_base_class import SciCatIngesterBase
from scicat_beamline.thumbnails import (build_thumbnail,
                                        encode_image_2_thumbnail)
from scicat_beamline.utils import (NPArrayEncoder,
                                   search_terms_from_name,
                                   calculate_access_controls)

# Note: This spec should be considered obsolete. Use als_832_dx_4 instead.

ingest_spec = "als832_dx_3"


class Als_832_Dx_3_Ingester(SciCatIngesterBase):
    """SciCat ingester for ALS 8.3.2 DX 3 data."""

    def ingest(
            self,
            dataset_path: Path,
            file_manifest: FileManifest,
            als_dataset_metadata: Optional[DatasetMetadataContainer] = None,
            owner_username: Optional[str] = None
        ) -> DatasetMetadataContainer:

        # Ensure we have ALS metadata structure
        self.use_or_create_als_metadata(als_dataset_metadata)

        # We expect to encounter one .h5 file.
        # If we don't find exactly one, we raise an error.
        found_files: List[FileManifestEntry] = []
        for manifest_file in file_manifest.files:
            file_path = Path(dataset_path, manifest_file.path)
            if file_path.suffix.lower() == ".h5":
                found_files.append(manifest_file)    
        if len(found_files) != 1:
            raise ValueError(f"Expected one .h5 file, found {len(found_files)}")
        h5_manifest_file = found_files[0]
        h5_file = Path(dataset_path, h5_manifest_file.path)

        with h5py.File(h5_file, "r") as file:
            scicat_metadata = self.extract_h5_file_fields(file, scicat_metadata_keys)
            scientific_metadata = self.extract_h5_file_fields(file, scientific_metadata_keys)
            scientific_metadata["data_sample"] = self.get_h5_file_data_sample(file, data_sample_keys)
            encoded_scientific_metadata = json.loads(
                json.dumps(scientific_metadata, cls=NPArrayEncoder)
            )
            access_controls = calculate_access_controls(
                owner_username,
                scicat_metadata.get("/measurement/sample/experiment/beamline"),
                scicat_metadata.get("/measurement/sample/experiment/proposal"),
            )
            self._logger.info(
                f"Access controls for  {h5_file}  access_groups: {access_controls.get('access_groups')} "
                f"owner_group: {access_controls.get('owner_group')}"
            )

            ownable = Ownable(
                ownerGroup=access_controls["owner_group"],
                accessGroups=access_controls["access_groups"],
            )

            file_name = scicat_metadata.get("/measurement/sample/file_name")
            description = search_terms_from_name(file_name)
            appended_keywords = description.split()
            proposal_name = scicat_metadata.get("/measurement/sample/experiment/proposal") or "Unknown"
            principal_investigator = scicat_metadata.get("/measurement/sample/experiment/pi") or "Unknown"

            dataset = RawDataset(
                owner = scicat_metadata.get("/measurement/sample/experiment/pi") or "Unknown",
                contactEmail = scicat_metadata.get("/measurement/sample/experimenter/email") or "Unknown",
                creationLocation = scicat_metadata.get("/measurement/instrument/instrument_name") or "Unknown",
                datasetName = file_name,
                type = DatasetType.raw,
                instrumentId = scicat_metadata.get("/measurement/instrument/instrument_name") or "Unknown",
                proposalId = proposal_name,
                dataFormat = "DX",
                principalInvestigator = principal_investigator,
                sourceFolder = str(h5_file.parent),
                scientificMetadata = encoded_scientific_metadata,
                sampleId = description,
                isPublished = False,
                description = description,
                keywords = appended_keywords,
                creationTime = h5_manifest_file.date_last_modified,
                **ownable.model_dump(),
            )
            scicat_dataset_id = self._scicat_client.datasets_create(dataset)
            self._logger.info(f"Created dataset with id {scicat_dataset_id} for file {h5_file.name}")

            thumbnail_file = build_thumbnail(file["/exchange/data"][0], self._temp_dir)
            encoded_thumbnail = encode_image_2_thumbnail(thumbnail_file)
            self.upload_thumbnail_attachment(encoded_thumbnail, scicat_dataset_id, "", ownable)

            datafiles = self.data_file_dtos_from_manifest(file_manifest)

            datablock = OrigDatablock(
                datasetId = scicat_dataset_id,
                instrumentGroup = "instrument-default",
                size = file_manifest.total_size_bytes,
                dataFileList = datafiles,
                **ownable.model_dump(),
            )
            self._scicat_client.datasets_origdatablock_create(scicat_dataset_id, datablock)

            self._als_dataset_metadata.als.beamline_id = "7.3.3"
            self._als_dataset_metadata.als.proposal_id = proposal_name
            self._als_dataset_metadata.als.principal_investigator = principal_investigator
            self._als_dataset_metadata.als.file_manifest = file_manifest

            self._als_dataset_metadata.als.scicat.scicat_dataset_id = scicat_dataset_id

        return self._als_dataset_metadata


scicat_metadata_keys = [
    "/measurement/instrument/instrument_name",
    "/measurement/sample/experiment/beamline",
    "/measurement/sample/experiment/experiment_lead",
    "/measurement/sample/experiment/pi",
    "/measurement/sample/experiment/proposal",
    "/measurement/sample/experimenter/email",
    "/measurement/sample/experimenter/name",
    "/measurement/sample/file_name",
]

scientific_metadata_keys = [
    "/measurement/instrument/attenuator/setup/filter_y",
    "/measurement/instrument/camera_motor_stack/setup/tilt_motor",
    "/measurement/instrument/detection_system/objective/camera_objective",
    "/measurement/instrument/detection_system/scintillator/scintillator_type",
    "/measurement/instrument/detector/binning_x",
    "/measurement/instrument/detector/binning_y",
    "/measurement/instrument/detector/dark_field_value",
    "/measurement/instrument/detector/delay_time",
    "/measurement/instrument/detector/dimension_x",
    "/measurement/instrument/detector/dimension_y",
    "/measurement/instrument/detector/model",
    "/measurement/instrument/detector/pixel_size",
    "/measurement/instrument/detector/temperature",
    "/measurement/instrument/monochromator/setup/Z2",
    "/measurement/instrument/monochromator/setup/temperature_tc2",
    "/measurement/instrument/monochromator/setup/temperature_tc3",
    "/measurement/instrument/slits/setup/hslits_A_Door",
    "/measurement/instrument/slits/setup/hslits_A_Wall",
    "/measurement/instrument/slits/setup/hslits_center",
    "/measurement/instrument/slits/setup/hslits_size",
    "/measurement/instrument/slits/setup/vslits_Lead_Flag",
    "/measurement/instrument/source/source_name",
    "/process/acquisition/dark_fields/dark_num_avg_of",
    "/process/acquisition/dark_fields/num_dark_fields",
    "/process/acquisition/flat_fields/i0_move_x",
    "/process/acquisition/flat_fields/i0_move_y",
    "/process/acquisition/flat_fields/i0cycle",
    "/process/acquisition/flat_fields/num_flat_fields",
    "/process/acquisition/flat_fields/usebrightexpose",
    "/process/acquisition/mosaic/tile_xmovedist",
    "/process/acquisition/mosaic/tile_xnumimg",
    "/process/acquisition/mosaic/tile_xorig",
    "/process/acquisition/mosaic/tile_xoverlap",
    "/process/acquisition/mosaic/tile_ymovedist",
    "/process/acquisition/mosaic/tile_ynumimg",
    "/process/acquisition/mosaic/tile_yorig",
    "/process/acquisition/mosaic/tile_yoverlap",
    "/process/acquisition/name",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/blur_limit",
    "/process/acquisition/rotation/multiRev",
    "/process/acquisition/rotation/nhalfCir",
    "/process/acquisition/rotation/num_angles",
    "/process/acquisition/rotation/range",
]

data_sample_keys = [
    "/measurement/instrument/sample_motor_stack/setup/axis1pos",
    "/measurement/instrument/sample_motor_stack/setup/axis2pos",
    "/measurement/instrument/sample_motor_stack/setup/sample_x",
    "/measurement/instrument/sample_motor_stack/setup/axis5pos",
    "/measurement/instrument/camera_motor_stack/setup/camera_elevation",
    "/measurement/instrument/source/current",
    "/measurement/instrument/camera_motor_stack/setup/camera_distance",
    "/measurement/instrument/source/beam_intensity_incident",
    "/measurement/instrument/monochromator/energy",
    "/measurement/instrument/detector/exposure_time",
    "/measurement/instrument/time_stamp",
    "/measurement/instrument/monochromator/setup/turret2",
    "/measurement/instrument/monochromator/setup/turret1",
]
