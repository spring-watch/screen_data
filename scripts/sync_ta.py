"""Mirror s3://<bucket>/ta_screener/ into ./ta_analysis, keeping the folder structure.

Syncs every .parquet file, plus .json files under GitHub's 100 MB file limit."""
import os
import shutil
from pathlib import Path

from common import BUCKET, get_logger, list_objects, s3

PREFIX = os.getenv("TA_PREFIX", "ta_screener/")
DEST = Path("ta_analysis")
MAX_BYTES = 95 * 1024**2  # GitHub rejects files > 100 MB
log = get_logger("sync_ta")

client = s3()
keys = []
for o in list_objects(client, PREFIX):
    if o["Key"].endswith(".parquet") or (o["Key"].endswith(".json") and o["Size"] <= MAX_BYTES):
        keys.append(o["Key"])
    elif o["Key"].endswith(".json"):
        log.warning("skip %s (%.0f MB > GitHub limit)", o["Key"], o["Size"] / 1024**2)
if not keys:
    raise SystemExit(f"no files to sync under s3://{BUCKET}/{PREFIX}")

shutil.rmtree(DEST, ignore_errors=True)  # exact mirror: drop files removed upstream
for key in keys:
    out = DEST / key[len(PREFIX):]
    out.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(BUCKET, key, str(out))
log.info("synced %d files from s3://%s/%s into %s/", len(keys), BUCKET, PREFIX, DEST)
