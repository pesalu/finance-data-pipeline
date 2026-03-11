import json
import boto3
import os
from datetime import datetime

s3 = boto3.client("s3")
events = boto3.client("events")

BUCKET = os.environ["BUCKET"]
EVENT_BUS = os.environ.get("EVENT_BUS", "default")

def lambda_handler(event, context):
    results = event["results"]

    if not results:
        raise Exception("No results received from Map state")

    symbol = results[0]["symbol"]
    ingestion_time = datetime.utcnow().isoformat()

    # Validate completeness
    missing = [r for r in results if r["records"] == 0]
    if missing:
        raise Exception(f"Some windows returned zero records: {missing}")

    # Build manifest
    manifest = {
        "symbol": symbol,
        "ingestedAt": ingestion_time,
        "windows": [
            {
                "from": r["from"],
                "to": r["to"],
                "records": r["records"],
                "s3Key": r["s3Key"]
            }
            for r in results
        ]
    }

    manifest_key = f"{symbol}/manifest-{ingestion_time}.json"

    # Write manifest to S3
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )

    print(f"Manifest written to {manifest_key}")

    # Emit ETL trigger event
    events.put_events(
        Entries=[
            {
                "Source": "data.ingestion",
                "DetailType": "IngestionCompleted",
                "Detail": json.dumps({
                    "symbol": symbol,
                    "manifestKey": manifest_key,
                    "windowCount": len(results),
                    "ingestedAt": ingestion_time
                }),
                "EventBusName": EVENT_BUS
            }
        ]
    )

    print("ETL trigger event emitted")

    return {
        "status": "success",
        "manifestKey": manifest_key,
        "windows": len(results)
    }
