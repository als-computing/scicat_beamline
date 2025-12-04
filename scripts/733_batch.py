import sys
import tempfile
from pathlib import Path
from typing import List

from pyscicat.client import from_token
from scicat_beamline.common_ingester_code import Issue
from scicat_beamline.ingesters import als_733_SAXS

folder = Path(sys.argv[1])
token = sys.argv[2]
user = sys.argv[3]
scicat_url_base = sys.argv[4]
try:
    issues = []
    client = from_token(scicat_url_base, token)
    txt_files = folder.glob("**/*.txt")
    with tempfile.TemporaryDirectory() as thumbs_dir:
        for txt_file in list(txt_files):
            als_733_SAXS.ingest(client, user, txt_file, thumbs_dir, issues)
            print(f"Ingesting {txt_file}")
            if len(issues) > 0:
                print(f"    Issues found {[str(issue) for issue in issues]}")
except Exception as e:
    print(e)
