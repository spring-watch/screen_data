"""Mirror s3://<bucket>/ta_screener/ (parquet + json) into ./ta_analysis, keeping the folder structure."""
import os
import shutil
from pathlib import Path

from common import BUCKET, get_logger, list_objects, s3

PREFIX = os.getenv("TA_PREFIX", "ta_screener/")
DEST = Path("ta_analysis")
log = get_logger("sync_ta")

client = s3()
keys = [o["Key"] for o in list_objects(client, PREFIX) if o["Key"].endswith((".parquet", ".json"))]
if not keys:
    raise SystemExit(f"no parquet/json files under s3://{BUCKET}/{PREFIX}")

shutil.rmtree(DEST, ignore_errors=True)  # exact mirror: drop files removed upstream
for key in keys:
    out = DEST / key[len(PREFIX):]
    out.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(BUCKET, key, str(out))
log.info("synced %d files from s3://%s/%s into %s/", len(keys), BUCKET, PREFIX, DEST)
