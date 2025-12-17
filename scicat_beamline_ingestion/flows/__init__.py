"""
Prefect flows forSciCat Beamline ingestion and processing.
"""

from scicat_beamline_ingestion.flows.scicat_ingest_flow import \
    scicat_ingest_flow

__all__ = [
    "scicat_ingest_flow",
]
