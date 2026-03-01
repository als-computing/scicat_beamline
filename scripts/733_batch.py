import sys
import tempfile
from pathlib import Path
from typing import List

from ingesters import als_733_SAXS
from pyscicat.client import from_token

folder = Path(sys.argv[1])
token = sys.argv[2]
user = sys.argv[3]
scicat_url_base = sys.argv[4]
try:
    client = from_token(scicat_url_base, token)
    txt_files = folder.glob("**/*.txt")
    with tempfile.TemporaryDirectory() as thumbs_dir:
        for txt_file in list(txt_files):
            als_733_SAXS.ingest(client, user, txt_file, thumbs_dir)
            print(f"Ingesting {txt_file}")
except Exception as e:
    print(e)
