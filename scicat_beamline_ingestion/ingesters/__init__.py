"""
SciCat ingesters for different beamlines
"""

from ingesters.als_733_SAXS import ingest as als_733_saxs_ingest
from ingesters.als_832_dx_4 import ingest as als_832_dx_4_ingest
from ingesters.als_11012_ccd_theta import ingest as als_11012_ccd_theta_ingest
from ingesters.als_11012_igor import ingest as als_11012_igor_ingest
from ingesters.als_11012_scattering import \
    ingest as als_11012_scattering_ingest
from ingesters.nexafs import ingest as nexafs_ingest
from ingesters.nsls2_nexafs_sst1 import ingest as nsls2_nexafs_sst1_ingest
from ingesters.nsls2_RSoXS import ingest as nsls2_rsoxs_sst1_ingest
from ingesters.nsls2_TREXS_smi import ingest as nsls2_TREXS_smi_ingest
from ingesters.polyfts_dscft import ingest as polyfts_dscft_ingest
from ingesters.test_ingest import ingest as als_test_ingest

__all__ = [
    "als_test_ingest",
    "als_733_saxs_ingest",
    "als_832_dx_4_ingest",
    "als_11012_ccd_theta_ingest",
    "als_11012_igor_ingest",
    "als_11012_scattering_ingest",
    "nexafs_ingest",
    "nsls2_nexafs_sst1_ingest",
    "nsls2_rsoxs_sst1_ingest",
    "nsls2_TREXS_smi_ingest",
    "polyfts_dscft_ingest",
]
