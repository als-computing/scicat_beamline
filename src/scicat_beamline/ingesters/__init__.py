"""
SciCat ingesters for different beamlines
"""

from scicat_beamline.ingesters.ingester_base_class import SciCatIngesterBase
from scicat_beamline.ingesters.als_733_SAXS import Als_733_Saxs_Ingester
from scicat_beamline.ingesters.als_832_dx_4 import Als_832_Dx_4_Ingester
#from scicat_beamline.ingesters.als_11012_ccd_theta import \
#    ingest as als_11012_ccd_theta_ingest
#from scicat_beamline.ingesters.als_11012_igor import \
#    ingest as als_11012_igor_ingest
#from scicat_beamline.ingesters.als_11012_scattering import \
#    ingest as als_11012_scattering_ingest
#from scicat_beamline.ingesters.nexafs import ingest as nexafs_ingest
#from scicat_beamline.ingesters.nsls2_nexafs_sst1 import \
#    ingest as nsls2_nexafs_sst1_ingest
#from scicat_beamline.ingesters.nsls2_RSoXS import \
#    ingest as nsls2_rsoxs_sst1_ingest
#from scicat_beamline.ingesters.nsls2_TREXS_smi import \
#    ingest as nsls2_TREXS_smi_ingest
#from scicat_beamline.ingesters.polyfts_dscft import \
#    ingest as polyfts_dscft_ingest
#from scicat_beamline.ingesters.test_ingest import ingest as als_test_ingest

__all__ = [
    "SciCatIngesterBase",
    "Als_733_Saxs_Ingester",
    "Als_832_Dx_4_Ingester",
#    "als_11012_ccd_theta_ingest",
#    "als_11012_igor_ingest",
#    "als_11012_scattering_ingest",
#    "nexafs_ingest",
#    "nsls2_nexafs_sst1_ingest",
#    "nsls2_rsoxs_sst1_ingest",
#    "nsls2_TREXS_smi_ingest",
#    "polyfts_dscft_ingest",
]
