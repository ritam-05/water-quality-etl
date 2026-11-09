"""Scheduled retention cleanup for the Supabase raw-telemetry bucket.

Deploy this handler as a separate Lambda and invoke it on a daily EventBridge
Scheduler schedule. It intentionally does not run in the SQS processor Lambda.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BUCKET_NAME = "raw-telemetry"
RETENTION_DAYS = 5
PAGE_SIZE = 1_000
DELETE_BATCH_SIZE = 1_000
HTTP_TIMEOUT_SECONDS = 15


def _storage_request(url, headers, method, payload=None):
    request = Request(url, data=payload, headers=headers, method=method)
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def _parse_timestamp(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def lambda_handler(event, context):
    """Delete Supabase Storage objects created more than five days ago."""
    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    service_key = os.environ["SUPABASE_SERVICE_KEY"]
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    }
    list_url = f"{supabase_url}/storage/v1/object/list/{BUCKET_NAME}"
    delete_url = f"{supabase_url}/storage/v1/object/{BUCKET_NAME}"
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    old_paths = []
    offset = 0

    try:
        while True:
            payload = json.dumps(
                {"prefix": "", "limit": PAGE_SIZE, "offset": offset}
            ).encode("utf-8")
            objects = _storage_request(list_url, headers, "POST", payload)

            for obj in objects:
                created_at = _parse_timestamp(obj.get("created_at"))
                if created_at and created_at < cutoff:
                    old_paths.append(obj["name"])

            if len(objects) < PAGE_SIZE:
                break
            offset += len(objects)

        deleted_count = 0
        for start in range(0, len(old_paths), DELETE_BATCH_SIZE):
            paths = old_paths[start : start + DELETE_BATCH_SIZE]
            payload = json.dumps({"prefixes": paths}).encode("utf-8")
            _storage_request(delete_url, headers, "DELETE", payload)
            deleted_count += len(paths)

        result = {
            "statusCode": 200,
            "bucket": BUCKET_NAME,
            "retention_days": RETENTION_DAYS,
            "deleted_count": deleted_count,
        }
        print(json.dumps(result))
        return result
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError) as error:
        print(f"Storage cleanup failed: {error}")
        raise
