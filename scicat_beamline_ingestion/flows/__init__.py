"""
Prefect flows forSciCat Beamline ingestion and processing.
"""

from scicat_beamline.flows.scicat_ingest_flow import scicat_ingest_flow

__all__ = [
    "scicat_ingest_flow",
]
