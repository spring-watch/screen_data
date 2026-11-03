"""Fetch current prices for tickers in the latest ETF/stock screener overview and merge into live_prices.json."""
import io
import json
import os
from pathlib import Path

import pandas as pd
import yfinance as yf

from common import BUCKET, get_logger, list_objects, s3

PREFIXES = os.getenv("SCREENER_PREFIXES", "etf_screener/,stock_screener/").split(",")
OUT = Path("live_prices.json")
log = get_logger("live_prices")


def latest_overview_tickers(client, prefix):
    objs = [o for o in list_objects(client, prefix)
            if "overview" in o["Key"] and o["Key"].endswith((".parquet", ".json", ".csv"))]
    if not objs:
        log.warning("no overview file under s3://%s/%s", BUCKET, prefix)
        return []
    key = max(objs, key=lambda o: (o["LastModified"], o["Key"]))["Key"]
    body = io.BytesIO(client.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    df = (pd.read_parquet if key.endswith(".parquet") else pd.read_json if key.endswith(".json") else pd.read_csv)(body)
    col = next(c for c in df.columns if c.lower() in ("ticker", "symbol"))
    log.info("%s: %d tickers", key, len(df))
    return df[col].dropna().astype(str).str.upper().tolist()


existing = {k: v for d in json.loads(OUT.read_text() or "[]") for k, v in d.items()} if OUT.exists() else {}
client = s3()
tickers = list(dict.fromkeys([*existing, *(t for p in PREFIXES for t in latest_overview_tickers(client, p.strip()))]))

closes = yf.download(tickers, period="5d", progress=False, auto_adjust=False, multi_level_index=True)["Close"]
prices = closes.ffill().iloc[-1].dropna().round(2).to_dict()
log.info("fetched %d/%d prices", len(prices), len(tickers))

merged = {**existing, **prices}
OUT.write_text(json.dumps([{k: v} for k, v in merged.items()]) + "\n")
