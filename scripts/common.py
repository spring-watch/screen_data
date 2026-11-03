import logging
import os

import boto3

BUCKET = os.getenv("S3_BUCKET", "market-data")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )


def list_objects(client, prefix):
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def get_logger(name):
    """Console logging, plus OTLP export when an endpoint is set and NO_OTEL isn't true."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(name)
    if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") and os.getenv("NO_OTEL", "").lower() not in ("1", "true"):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import Resource

        provider = LoggerProvider(resource=Resource.create({"service.name": name}))  # flushes on exit
        provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
        log.addHandler(LoggingHandler(logger_provider=provider))
    return log
