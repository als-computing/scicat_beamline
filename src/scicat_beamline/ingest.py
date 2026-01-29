import glob
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from xmlrpc import client
import typer

from dataset_metadata_schemas.utilities import (read_als_metadata_file, write_als_metadata_file, get_nested)
from dataset_metadata_schemas.dataset_metadata import SciCat, DatasetTracker, Container as DatasetMetadataContainer
from dataset_tracker_client.client import DatasettrackerClient
from dataset_tracker_client.model import (DatasetCreateDto,
                                          DatasetInstanceCreateDto,
                                          BeamlineCreateDto,
                                          ProposalCreateDto,
                                          DatasetInstanceFile,
                                          DatasetInstanceFileCreateDto)

from pyscicat.client import from_credentials

from scicat_beamline.ingesters import (als_733_saxs_ingest,
                                       als_832_dx_4_ingest,
                                       #    als_11012_ccd_theta_ingest,
                                       #    als_11012_igor_ingest,
                                       als_11012_scattering_ingest,
                                       als_test_ingest, nexafs_ingest,
                                       nsls2_nexafs_sst1_ingest,
                                       nsls2_rsoxs_sst1_ingest,
                                       nsls2_TREXS_smi_ingest,
                                       polyfts_dscft_ingest)
from scicat_beamline.utils import Issue


def standard_iterator(pattern: str):
    return glob.iglob(pattern)


def ingest(
    dataset_path: Path = typer.Argument(
        ...,
        file_okay=False,
        dir_okay=True,
        help=(
            "Common sub-path of the file(s) to ingest. Logfiles and metadata files will be created here. "
            "Prepended with SCICAT_INGEST_INTERNAL_BASE_FOLDER, or SCICAT_INGEST_BASE_FOLDER, if set."
        ),
    ),
    dataset_files: list[Path] = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=False,
        help=(
            "Files to ingest, as paths relative to dataset_path. "
            "Everything listed here will be considered part of the Dataset."
        ),
    ),
    ingester_spec: str | None = typer.Option(
        None,
        help="Spec to ingest with"
    ),
    owner_username: str | None = typer.Option(
        None,
        help="User doing the ingesting. May be different from the user_name.",
    ),
    scicat_url: str | None = typer.Option(
        None,
        help="Scicat server base url. If not provided, will try localhost default",
    ),
    scicat_username: str | None = typer.Option(
        None,
        help="Scicat server username"
    ),
    scicat_password: str | None = typer.Option(
        None,
        help="Scicat server password"
    ),
    datasettracker_url: str | None = typer.Option(
        None,
        help="Dataset Tracker server base url. Using the Dataset Tracker is optional."
    ),
    datasettracker_username: str | None = typer.Option(
        None,
        help="Dataset Tracker server username. Using the Dataset Tracker is optional."
    ),
    datasettracker_password: str | None = typer.Option(
        None,
        help="Dataset Tracker server password. Using the Dataset Tracker is optional."
    ),
    datasettracker_share_identifier: str | None = typer.Option(
        None,
        help=("A slug for a Share Sublocation in the Dataset Tracker."
              "Indicates which storage location this instance of the ingester has direct access to on its storage path. Typically 'als-beegfs'."
        )
    ),
    logger: logging.Logger | None = typer.Option(
        None,
        help="Logger to use"
    ),
    prefect_flow_run_id: str | None = typer.Option(
        None,
        help="Prefect flow run identifier, if available."
    ),
):
    results: Dict[str, Any] = {}

    if logger is None:
        logger = logging.getLogger("scicat_operation")
        logger.setLevel("INFO")

    if not ingester_spec:
        ingester_spec = os.getenv("SCICAT_INGEST_SPEC", "")
        if not ingester_spec:
            logger.exception("Cannot resolve ingester spec.")
            return results

    if not scicat_url:
        scicat_url = os.getenv("SCICAT_INGEST_URL", "")
        if not scicat_url:
            scicat_url = "http://localhost:3000/api/v3"
            logger.warning(f"Using default SciCat URL {scicat_url}")

    if not scicat_username:
        scicat_username = os.getenv("SCICAT_INGEST_USERNAME", "")
        if not scicat_username:
            logger.exception("Cannot resolve SciCat username.")
            return results

    if not scicat_password:
        scicat_password = os.getenv("SCICAT_INGEST_PASSWORD", "")
        if not scicat_password:
            logger.exception("Cannot resolve SciCat password.")
            return results

    # (Must attempt to resolve scicat_username first above)
    if not owner_username:
        owner_username = os.getenv("SCICAT_INGEST_OWNER_USERNAME", "")
        if not owner_username:
            logger.info("Using SciCat username as owner username.")
            owner_username = scicat_username

    if not datasettracker_url:
        datasettracker_url = os.getenv("DATASETTRACKER_URL", "")
        if not datasettracker_url:
            logger.info("Dataset Tracker URL not set. Dataset Tracker will not be used.")

    if not datasettracker_username:
        datasettracker_username = os.getenv("DATASETTRACKER_USERNAME", "")
        if not datasettracker_username:
            logger.warning("Cannot resolve Dataset Tracker username.")

    if not datasettracker_password:
        datasettracker_password = os.getenv("DATASETTRACKER_PASSWORD", "")
        if not datasettracker_password:
            logger.warning("Cannot resolve Dataset Tracker password.")

    if not datasettracker_share_identifier:
        datasettracker_share_identifier = os.getenv("DATASETTRACKER_SHARE_IDENTIFIER", "als-beegfs")
        if not datasettracker_share_identifier:
            logger.info("Dataset Tracker share identifier not set. Using a default of 'als-beegfs'.")

    # Attempte to resolve the ingester spec, and fail immediately otherwise.

    ingestion_function = None
    if ingester_spec == "bltest":
        ingestion_function = als_test_ingest
    elif ingester_spec == "als_11012_igor":
        ingestion_function = als_733_saxs_ingest
    elif ingester_spec == "als_832_dx_4":
        ingestion_function = als_832_dx_4_ingest
    elif ingester_spec == "als_11012_scattering":
        ingestion_function = als_11012_scattering_ingest
    elif ingester_spec == "als_11012_nexafs":
        ingestion_function = nexafs_ingest
    elif ingester_spec == "nsls2_rsoxs_sst1":
        ingestion_function = nsls2_rsoxs_sst1_ingest
    elif ingester_spec == "nsls2_nexafs_sst1":
        ingestion_function = nsls2_nexafs_sst1_ingest
    elif ingester_spec == "als733_saxs":
        ingestion_function = als_733_saxs_ingest
    elif ingester_spec == "nsls2_trexs_smi":
        ingestion_function = nsls2_TREXS_smi_ingest
    elif ingester_spec == "polyfts_dscft":
        ingestion_function = polyfts_dscft_ingest
    else:
        logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
        return results
    logger.info(f"Using ingester spec {ingester_spec}")

    # Attempt to expand the dataset_path to a complete path.
    # If an internal base folder is set (because we're accessing a mounted volume inside a container),
    # use that.  Otherwise look for a general base folder.

    prepend_path = None
    internal_base_folder = os.getenv("SCICAT_INGEST_INTERNAL_BASE_FOLDER", "")
    base_folder = os.getenv("SCICAT_INGEST_BASE_FOLDER", "")
    if internal_base_folder:
        prepend_path = internal_base_folder
        logger.info(f"Using internal base folder: {prepend_path}")
    elif base_folder:
        prepend_path = base_folder
        logger.info(f"Using base folder: {prepend_path}")
    else:
        logger.info(f"No base folder set.")
    if prepend_path:
        full_dataset_path = Path(prepend_path, dataset_path).resolve()
    else:
        full_dataset_path = Path(dataset_path).resolve()

    # Now that we know the full path, we can start a logfile

    logger.info("Setting up ingester logfile.")
    logfile = Path(full_dataset_path, "scicat_ingest_log.log")
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
    )
    fileHandler = logging.FileHandler(
        logfile, mode="a", encoding=None, delay=False, errors=None
    )
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    # Validate that all the given files exist and are not symlinks.

    valid_files = []
    for one_file in dataset_files:
        file_path = Path(full_dataset_path, one_file)

        if not file_path.exists():
            logger.error(f"Given a file path that does not exist: {file_path}")
            continue
        if file_path.is_symlink():
            logger.error(f"Symlink detected. Skipping: {file_path}")
            continue
        if file_path.is_dir():
            logger.error(f"Given a file path that resolves to a folder: {file_path}")
            continue
        # Note this is still the original given partial path
        valid_files.append(one_file)
    if len(valid_files) == 0:
        logger.error("No valid files or folders to ingest.")
        return results

    # Since we have a folder, we'll check for an existing als-dataset-metadata.json file.

    als_dataset_metadata:Optional[DatasetMetadataContainer] = None
    try:
        als_dataset_metadata = read_als_metadata_file(file_path=Path(full_dataset_path, "als-dataset-metadata.json"))
    except Exception as e:
        # This only warrants an "info" because we don't require Datasets arriving from
        # beamline stations to have a metadata file already. ... We just encourage it. :D
        logger.info("Did not find a als-dataset-metadata.json file.")

    # If we did find a file, there are some things we need to validate.

    if als_dataset_metadata is not None:
        # Ensure that there is no SciCat dataset ID already present in the metadata.
        # In the future we may allow this to be overriden, to force a re-ingestion.
        if get_nested(als_dataset_metadata, "als.scicat.scicat_dataset_id") is not None:
            logger.error(
                "The als-dataset-metadata.json file already has a SciCat dataset ID. Stopping."
            )
            return results
        # If there is a file_manifest, ensure that it contains all the files in our given ingest list.
        existing_manifest_files = get_nested(als_dataset_metadata, "als.file_manifest.files")
        if existing_manifest_files is not None:
            seen_files = set()
            for given_file in valid_files:
                seen_files.add(given_file)
            for manifest_file in existing_manifest_files:
                if manifest_file.path in seen_files:
                    seen_files.remove(manifest_file.path)
            # It is conceivable that we would want to re-ingest a Dataset with files removed, or with files
            # ignored due to their being supplemental.  But we should not allow an ingest with added files
            # unless the manifest has been updated in advance to reflect the changes.
            # Otherwise we risk allowing an ingestion on top of an entirely different SciCat Dataset.
            if len(seen_files) > 0:
                logger.error(
                    "The given dataset_files list containes files that are not in the existing als-dataset-metadata file. Possible metadata mismatch. Stopping."
                )
                for missing_file in seen_files:
                    logger.error(f" - {missing_file}")
                return results

    # The following commented-out code was used to crawl a given folder and look for
    # the correct files for each ingester spec, with the assumption that one file
    # represented one Dataset, and we would create many.
    # It's kept here as a reference so we can refer to it as we adapt the ingesters
    # to deal with a common folder and a standard list of files.
    # The preferred approach now is to let the ingester choose which files are most
    # improtant, and that the whole set always constitutes only one Dataset.

    # ingest_files_iter = []
    # if ingester_spec == "bltest":
    #     temp_iter = standard_iterator(f"{ingestion_search_path}/*.txt")
    #     for file_str in temp_iter:
    #         ingest_files_iter.append(file_str)
    # elif ingester_spec == "als_11012_igor":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/CCD/*/dat/")
    # elif ingester_spec == "als_832_dx_4":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")
    # elif ingester_spec == "als_11012_scattering":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/CCD/*/")
    # elif ingester_spec == "als_11012_nexafs":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/Nexafs/*")
    # elif ingester_spec == "nsls2_rsoxs_sst1":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")
    # elif ingester_spec == "nsls2_nexafs_sst1":
    #     temp_iter = standard_iterator(f"{ingestion_search_path}/*")
    #     for file_str in temp_iter:
    #         if (
    #             file_str.endswith(".log")
    #             or file_str.endswith(".csv")
    #             or file_str.endswith(".txt")
    #         ):
    #             continue
    #         ingest_files_iter.append(file_str)
    # elif ingester_spec == "als733_saxs":
    #     temp_iter = standard_iterator(f"{ingestion_search_path}/*.txt")
    #     for file_str in temp_iter:
    #         ingest_files_iter.append(file_str)
    # elif ingester_spec == "nsls2_trexs_smi":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")
    # elif ingester_spec == "polyfts_dscft":
    #     ingest_files_iter = standard_iterator(f"{ingestion_search_path}/*/")
    # else:
    #     logger.exception(f"Cannot resolve ingester spec {ingester_spec}")
    #     return results

    # Each ingester is given a connection to SciCat, and optionally to the Dataset Tracker.
    # They are also given any existing ALS Dataset metadata file.

    try:
        pyscicat_client = from_credentials(scicat_url, scicat_username, scicat_password)
    except Exception:
        logger.exception(f"Error logging in to SciCat. Cannot proceed.")
        return results

    # When the specific ingester's work is done we use the content of the ALS Dataset metadata
    # file it returns to create the relevant Dataset Tracker records.
    # It is not generally expected that the ingesters will need the Dataset Tracker directly,
    # but we pass it anyway for now.

    datasettracker_client = None
    if datasettracker_username and datasettracker_password and datasettracker_url:
        try:
            datasettracker_client = DatasettrackerClient(
                base_url=datasettracker_url,
                username=datasettracker_username,
                password=datasettracker_password,
            )
        except Exception as e:
            logger.exception(
                f"Credentials were given, but cannot connect to Dataset Tracker client. Error: {e}"
            )
            return results

    issues: List[Issue] = []
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            als_dataset_metadata = ingestion_function(
                scicat_client=pyscicat_client,
                temp_dir=temp_path,
                datasettracker_client=datasettracker_client,
                als_dataset_metadata=als_dataset_metadata,
                owner_username=owner_username,
                dataset_path=full_dataset_path,
                dataset_files=valid_files,
                issues=issues,
            )

    except Exception as e:
        logger.exception(f"Error running ingester function. Partial import may have occurred: {e}")
        return results

    if len(issues) > 0:
        logger.info("The following issues were encountered during ingestion:")
        for issue in issues:
            if issue.severity == "error":
                logger.error(f"{issue.msg}")
            else:
                logger.warning(f"{issue.msg}")

    if als_dataset_metadata is None:
        logger.error("Ingestion did not return ALS Dataset metadata. Cannot proceed.")
        return results

    scicat_dataset_id = get_nested(als_dataset_metadata, "als.scicat.scicat_dataset_id")
    if scicat_dataset_id is None:
        logger.error("Ingestion did not return a SciCat dataset ID. Cannot proceed.")
        return results

    als_dataset_metadata.als.scicat = SciCat(
        scicat_dataset_id = scicat_dataset_id,
        scicat_instance = scicat_url,
        date_ingested = datetime.now(timezone.utc).isoformat(),
        ingester_used = ingester_spec,
        ingestion_logfile = str(logfile.relative_to(full_dataset_path))
    )

    if datasettracker_client is None:
        logger.info("Dataset Tracker client not available. Skipping Dataset Tracker records updates.")
    else:
        file_manifest = get_nested(als_dataset_metadata, "als.file_manifest.files")
        if file_manifest is None or len(file_manifest) == 0:
            logger.error("No file manifest present in ALS Dataset metadata. Cannot proceed with Dataset Tracker records updates.")
            return results

        # The metadata file should use an id as the User Office defines it, e.g. "bl8.3.2",
        # not a Dataset Tracker slug, e.g. "bl8-3-2".

        beamline_name = get_nested(als_dataset_metadata, "als.beamline_id")
        beamline_exists = datasettracker_client.beamline_get_many(
            filter_fields={"name": beamline_name}
        )
        # If it doesn't exist, we'll create it and use it as-is,
        # relying on an admin to make a downstream correction if needed.
        if len(beamline_exists) > 0:
            beamline_exists = beamline_exists[0]
        else:
            beamline_exists = datasettracker_client.beamline_create(
                BeamlineCreateDto(
                    name=beamline_name,
                    description=f"Auto-created while ingesting Dataset {scicat_dataset_id}"
                )
            )

        # Same deal with Proposal identifiers.
        # If we haven't seen it before, we assume it's valid and create it.

        proposal_name = get_nested(als_dataset_metadata, "als.proposal_id")
        proposal_exists = datasettracker_client.proposal_get_many(
            filter_fields={"name": proposal_name}
        )
        if len(proposal_exists) > 0:
            proposal_exists = proposal_exists[0]
        else:
            proposal_exists = datasettracker_client.proposal_create(
                ProposalCreateDto(
                    name=proposal_name,
                    description=f"Auto-created while ingesting Dataset {scicat_dataset_id}"
                )
            )

        # Make sure the share sublocation we intend to use exists.

        share_sublocation = datasettracker_client.sharesublocation_get_one(datasettracker_share_identifier)
        if share_sublocation is None:
            logger.error(
                f"Dataset Tracker share sublocation with slug identifier {datasettracker_share_identifier} does not exist. Cannot proceed with Dataset Tracker record creation."
            )
            return results

        # Now we finally have what we need to create a new Dataset.

        existing_dataset_id = get_nested(als_dataset_metadata, "als.dataset_tracker.dataset_tracker_id")
        if existing_dataset_id is not None:
            logger.info("Dataset Tracker ID already present in metadata. Using existing record.")
            dataset_record = datasettracker_client.dataset_get_one(existing_dataset_id)
            if dataset_record is None:
                logger.error(
                    f"Dataset Tracker ID {existing_dataset_id} present in metadata but record not found. Something odd is going on."
                )
                return results
            else:
                # Since we found an existing record, assume we're updating it.
                dataset_record.scicat_dataset_id = scicat_dataset_id
                dataset_record.scicat_date_ingested = get_nested(als_dataset_metadata, "als.scicat.date_ingested")
                dataset_record.scicat_ingestion_flow_run_id = prefect_flow_run_id
                dataset_record = datasettracker_client.dataset_update(dataset_record)
        else:
            dataset_record = datasettracker_client.dataset_create(
                DatasetCreateDto(
                    name=get_nested(als_dataset_metadata, "als.name"),
                    description=get_nested(als_dataset_metadata, "als.description"),
                    slug_beamline=get_nested(als_dataset_metadata, "als.beamline_id"),
                    slug_proposal=get_nested(als_dataset_metadata, "als.proposal_id"),
                    date_of_acquisition=get_nested(als_dataset_metadata, "als.date_of_acquisition"),
                    scicat_dataset_id=scicat_dataset_id,
                    scicat_date_ingested=get_nested(als_dataset_metadata, "als.scicat.date_ingested"),
                    scicat_ingestion_flow_run_id=prefect_flow_run_id
                )
            )

        # Now we have an existing Dataset record that's been either created or updated.
        # Time to look for a Dataset Instance record.

        # A re-ingestion where files differ is an interesting situation because it's not
        # a copy or move: Files aren't going anywhere.  So the situation doesn't warrant a new
        # Instance record.  If one exists, we're going to assume the instance was changed
        # in place deliberately by a scientist correcting something that now requires a re-ingestion.
        # If there are downstream copies made from the old files (not likely), their record
        # creation dates will at least provide some clue to the order of operations.

        instance_record = datasettracker_client.dataset_instance_get_many(
            filter_fields={
                "slug_dataset": dataset_record.slug,
                "slug_share_sublocation": share_sublocation.slug,
                # There should only ever be one of these, but if there are more,
                # we "solve" the problem by taking the latest.
                # (The default sort on this API call is date created descending.)
                "date_files_deleted__isnull": True,
                "path": str(dataset_path)
            }
        )
        if len(instance_record) > 0:
            instance_record = instance_record[0]
            logger.info("Dataset Instance record with this path already exists. Using existing.")
        else:
            instance_record = datasettracker_client.dataset_instance_create(
                DatasetInstanceCreateDto(
                    slug_dataset=dataset_record.slug,
                    slug_share_sublocation=share_sublocation.slug,
                    # The path _within_ the share sublocation
                    path=str(dataset_path),
                    # We technically don't know what run put these files here, so we'll use this.
                    prefect_flow_run_id=prefect_flow_run_id,
                    # We can assume a manifest exists at this point.
                    files_size_bytes=get_nested(als_dataset_metadata, "als.file_manifest.total_size_bytes"),
                )
            )

        # Remember that above, we compared the incoming list of files to the manifest of a
        # (potentially) pre-existing metadata file.
        # Now we're assuming that the incoming list and the metadata file have been reconciled.
        # Our concern now is any discrepancy between the manifest and the existing
        # Dataset Instance File records.

        manifest_files_by_path = {f.path: f for f in file_manifest}
        file_records:List[DatasetInstanceFile] = datasettracker_client.dataset_instance_files_get_many(
            filter_fields={"id_dataset_instance": instance_record.id}
        )
        record_files_by_path = {f.path: f for f in file_records}

        # How we handle files is a bit tricky, because if we're using an existing Dataset Instance
        # record then old File records may exist.  Should we delete them, or update them?
        # One possible problem is the old records will have 'local_path' values indicating they
        # were renamed.  Another is that files may have been deleted from the manifest prior to
        # re-running ingestion, but before syncing with the Dataset Tracker.

        not_in_manifest = []
        for path in record_files_by_path.keys():
            if path not in manifest_files_by_path:
                not_in_manifest.append(path)

        not_in_records = []
        in_records = []
        for path in manifest_files_by_path.keys():
            if path not in record_files_by_path:
                not_in_records.append(path)
            else:
                in_records.append(path)

        # The current solution is:  Delete anything not in the manifest,
        # create anything not in the records, and update anything that matches.

        # Delete what's missing
        for missing_file_path in not_in_manifest:
            datasettracker_client.dataset_instance_file_delete(
                record_files_by_path[missing_file_path].id
            )
        
        # Create what's new
        for new_file_path in not_in_records:
            manifest_file = manifest_files_by_path[new_file_path]
            datasettracker_client.dataset_instance_file_create(
                DatasetInstanceFileCreateDto(
                    id_dataset_instance=instance_record.id,
                    file_path=new_file_path,
                    file_size_bytes=manifest_file.size_bytes,
                    date_file_last_modified=manifest_file.date_last_modified,
                    is_supplemental=manifest_file.is_supplemental,
                )
            )

        # Update what's changed
        for existing_file_path in in_records:
            manifest_file = manifest_files_by_path[existing_file_path]
            record_file = record_files_by_path[existing_file_path]
            record_file.file_size_bytes = manifest_file.size_bytes
            record_file.date_file_last_modified = manifest_file.date_last_modified
            record_file.is_supplemental = manifest_file.is_supplemental
            datasettracker_client.dataset_instance_file_update(
                record_file
            )

        existing_comments = get_nested(als_dataset_metadata, "als.dataset_tracker.instance_comments")

        als_dataset_metadata.als.dataset_tracker = DatasetTracker(
            dataset_tracker_id=dataset_record.slug,
            dataset_tracker_instance=datasettracker_url,
            instance_record_id=instance_record.id,
            instance_comments=existing_comments or []
        )

    # Write back the ALS dataset metadata file with any updates.

    # TODO: Add a reference to the metadata file to the manifest inside itself?

    # TODO: Remember if an old version of the file existed without a slug, and delete it.

    metadata_file_name = "als-dataset-metadata.json"
    slug = get_nested(als_dataset_metadata, "als.dataset_tracker.dataset_tracker_id")
    if slug is not None:
        metadata_file_name = f"als-dataset-metadata-{slug}.json"

    try:
        write_als_metadata_file(
            metadata=als_dataset_metadata,
            file_path=Path(full_dataset_path, metadata_file_name),
        )
        logger.info(
            "Wrote updated als-dataset-metadata.json file."
        )
    except Exception as e:
        logger.warning(
            f"Could not write updated als-dataset-metadata.json file. Error: {e}"
        )

    # Our successful result is the metadata file as JSON-compatible Python
    results = als_dataset_metadata.model_dump(mode="json")

    logger.info("Ingestion finished.")
    return results


if __name__ == "__main__":
    typer.run(ingest)
