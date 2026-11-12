"""Mirror s3://<bucket>/ta_screener/ into ./ta_analysis, keeping the folder structure.

Syncs every .parquet file, plus .json, .json.gz and .zst files under GitHub's 100 MB file limit, then writes
screen_meta.json (available run dates + industry list) at the repo root."""
import json
import os
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from common import BUCKET, get_logger, list_objects, s3

PREFIX = os.getenv("TA_PREFIX", "ta_screener/")
DEST = Path("ta_analysis")
MANIFEST = DEST / ".sync_manifest.json"  # {relative path: {etag, size}} from the last sync
MAX_BYTES = 95 * 1024**2  # GitHub rejects files > 100 MB
META = Path("screen_meta.json")
ET = ZoneInfo("America/New_York")  # NYSE time (EST/EDT); tz data ships with pandas' tzdata dependency
log = get_logger("sync_ta")

client = s3()
objects = []
LIMITED_EXTS = (".json", ".json.gz", ".zst")
for o in list_objects(client, PREFIX):
    if o["Key"].endswith(".parquet") or (o["Key"].endswith(LIMITED_EXTS) and o["Size"] <= MAX_BYTES):
        objects.append(o)
    elif o["Key"].endswith(LIMITED_EXTS):
        log.warning("skip %s (%.0f MB > GitHub limit)", o["Key"], o["Size"] / 1024**2)
if not objects:
    raise SystemExit(f"no files to sync under s3://{BUCKET}/{PREFIX}")

# Incremental mirror: the previous sync is already checked out, so only fetch objects whose
# ETag/size differ from the committed manifest, then delete anything no longer upstream.
old = json.loads(MANIFEST.read_text()) if MANIFEST.is_file() else {}
new, fetched = {}, 0
for o in objects:
    rel = o["Key"][len(PREFIX):]
    sig = {"etag": o["ETag"].strip('"'), "size": o["Size"]}
    new[rel] = sig
    out = DEST / rel
    if old.get(rel) == sig and out.is_file() and out.stat().st_size == o["Size"]:
        continue  # unchanged since last sync
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".part")
    client.download_file(BUCKET, o["Key"], str(tmp))
    tmp.replace(out)  # atomic: never leave a half-written file in place
    fetched += 1

removed = 0
for p in sorted(DEST.rglob("*"), reverse=True):  # deepest first so emptied dirs can go too
    if p.is_file() and p != MANIFEST and p.relative_to(DEST).as_posix() not in new:
        p.unlink()
        removed += 1
    elif p.is_dir() and not any(p.iterdir()):
        p.rmdir()

MANIFEST.parent.mkdir(parents=True, exist_ok=True)
MANIFEST.write_text(json.dumps(new, indent=1, sort_keys=True) + "\n")
log.info("synced s3://%s/%s into %s/: %d files, %d fetched, %d removed",
         BUCKET, PREFIX, DEST, len(new), fetched, removed)


def write_meta():
    run_dates, industries = {}, set()
    for f in sorted(DEST.glob("run_date=*/metadata.json"), reverse=True):  # newest first
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
