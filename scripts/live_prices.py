"""Fetch current prices for every symbol in the latest ETF/stock screener snapshots and merge into live_prices.json.

Output: [{"AAPL": 338.32}, ..., {"updated_at": "2026-11-03T10:30:00Z"}]  (timestamp is always the last element)
"""
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from yfinance.data import YfData  # yfinance's session (handles Yahoo cookie/crumb)

from common import BUCKET, get_logger, list_objects, s3

PREFIXES = os.getenv("SCREENER_PREFIXES", "etf_screener_snapshots/,stock_screener_snapshots/").split(",")
BATCH = 200  # symbols per Yahoo quote request
QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
TS_KEY = "updated_at"
OUT = Path("live_prices.json")
log = get_logger("live_prices")


def latest_symbols(client, prefix):
    keys = [o["Key"] for o in list_objects(client, prefix) if o["Key"].endswith(".parquet")]
    if not keys:
        log.warning("no parquet under s3://%s/%s", BUCKET, prefix)
        return []
    folder = max(k.rsplit("/", 1)[0] for k in keys)  # snapshot_date=YYYY-MM-DD sorts chronologically
    df = pd.concat(pd.read_parquet(io.BytesIO(client.get_object(Bucket=BUCKET, Key=k)["Body"].read()))
                   for k in keys if k.rsplit("/", 1)[0] == folder)
    log.info("%s: %d symbols", folder, df["symbol"].nunique())
    return df["symbol"].dropna().astype(str).str.upper().tolist()


existing = {k: v for d in json.loads(OUT.read_text() or "[]") for k, v in d.items() if k != TS_KEY} if OUT.exists() else {}
client = s3()
symbols = list(dict.fromkeys([*existing, *(s for p in PREFIXES for s in latest_symbols(client, p.strip()))]))
yahoo = {s.replace(".", "-").replace("/", "-"): s for s in symbols}  # BRK.B -> BRK-B (Yahoo format)

prices, tickers, yf = {}, list(yahoo), YfData()
for i in range(0, len(tickers), BATCH):
    try:
        res = yf.get_raw_json(QUOTE_URL, params={"symbols": ",".join(tickers[i:i + BATCH]), "fields": "regularMarketPrice"})
    except Exception as e:  # one bad batch shouldn't drop the rest
        log.warning("batch %d failed: %s", i // BATCH, e)
        continue
    prices |= {yahoo[q["symbol"]]: round(q["regularMarketPrice"], 2)
               for q in res["quoteResponse"]["result"] if q.get("regularMarketPrice") is not None and q["symbol"] in yahoo}
log.info("fetched %d/%d prices", len(prices), len(symbols))

now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
rows = [{k: v} for k, v in {**existing, **prices}.items()] + [{TS_KEY: now}]
OUT.write_text(json.dumps(rows) + "\n")
