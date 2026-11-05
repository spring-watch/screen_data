"""Mirror s3://<bucket>/ta_screener/ into ./ta_analysis, keeping the folder structure.

Syncs every .parquet file, plus .json, .json.gz and .zst files under GitHub's 100 MB file limit, then writes
screen_meta.json (available run dates + industry list) at the repo root."""
import json
import os
import shutil
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from common import BUCKET, get_logger, list_objects, s3

PREFIX = os.getenv("TA_PREFIX", "ta_screener/")
DEST = Path("ta_analysis")
MAX_BYTES = 95 * 1024**2  # GitHub rejects files > 100 MB
META = Path("screen_meta.json")
ET = ZoneInfo("America/New_York")  # NYSE time (EST/EDT); tz data ships with pandas' tzdata dependency
log = get_logger("sync_ta")

client = s3()
keys = []
LIMITED_EXTS = (".json", ".json.gz", ".zst")
for o in list_objects(client, PREFIX):
    if o["Key"].endswith(".parquet") or (o["Key"].endswith(LIMITED_EXTS) and o["Size"] <= MAX_BYTES):
        keys.append(o["Key"])
    elif o["Key"].endswith(LIMITED_EXTS):
        log.warning("skip %s (%.0f MB > GitHub limit)", o["Key"], o["Size"] / 1024**2)
if not keys:
    raise SystemExit(f"no files to sync under s3://{BUCKET}/{PREFIX}")

shutil.rmtree(DEST, ignore_errors=True)  # exact mirror: drop files removed upstream
for key in keys:
    out = DEST / key[len(PREFIX):]
    out.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(BUCKET, key, str(out))
log.info("synced %d files from s3://%s/%s into %s/", len(keys), BUCKET, PREFIX, DEST)


def write_meta():
    run_dates, industries = {}, set()
    for f in sorted(DEST.glob("run_date=*/_metadata.json"), reverse=True):  # newest first
        m = json.loads(f.read_text())
        d = date.fromisoformat(m.get("run_date") or f.parent.name.split("=", 1)[1])
        phase = f" ({m['phase'].title()})" if m.get("phase") else ""
        run_dates[d.isoformat()] = f"{d:%b} {d.day}, {d.year}{phase}"  # e.g. "Nov 3, 2026 (Premarket)"
        industries |= set(m.get("industries", {}).values())
    META.write_text(json.dumps({
        "updated_at": datetime.now(ET).isoformat(timespec="seconds"),  # e.g. 2026-11-03T20:15:26-05:00
        "latest_run_date": next(iter(run_dates), None),
        "run_dates": run_dates,
        "industries": sorted(industries),
    }, indent=2) + "\n")
    log.info("wrote %s: %d run dates, %d industries", META, len(run_dates), len(industries))


write_meta()
