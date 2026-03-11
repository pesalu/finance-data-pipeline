# import os
import json
import boto3
import os
import requests

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
secrets = boto3.client("secretsmanager")

BUCKET = os.environ["BUCKET"]
# BASE_URL = os.environ["BASE_URL"]
# API_KEY = os.environ["API_KEY"]

# Read base URL from SSM
base_url_param = os.environ["BASE_URL_PARAM"]
BASE_URL = ssm.get_parameter(Name=base_url_param)["Parameter"]["Value"]

# Read API key from Secrets Manager
secret_name = os.environ["FINANCE_SECRET"]
secret_value = secrets.get_secret_value(SecretId=secret_name)
secret_json = json.loads(secret_value["SecretString"])
API_KEY = secret_json["apiKey"]


def lambda_handler(event, context):
    symbol = event["symbol"]
    from_date = event["from"]
    to_date = event["to"]

    url = (
        f"{BASE_URL}/historical-price-eod/full"
        f"?symbol={symbol}&from={from_date}&to={to_date}&apikey={API_KEY}"
    )

    resp = requests.get(url)
    data = resp.json() or []

    key = f"{symbol}/{from_date}_{to_date}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved window {from_date} → {to_date} ({len(data)} records)")

    return {
        "symbol": symbol,
        "from": from_date,
        "to": to_date,
        "records": len(data),
        "s3Key": key
    }
