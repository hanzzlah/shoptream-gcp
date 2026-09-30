import csv
import io
import os
from datetime import datetime, timezone

import functions_framework # type: ignore
from google.cloud import bigquery, storage # type: ignore

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_status",
    "region",
    "created_at",
    "updated_at",
}

VALID_STATUSES = {
    "PLACED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
}


def _move_to_rejected(bucket, blob, object_name: str, reason: str):
    """Moves a malformed file from raw/uploads/ to raw/rejected/."""
    file_name = os.path.basename(object_name)
    destination_name = f"raw/rejected/{file_name}"

    bucket.copy_blob(blob, bucket, destination_name)
    blob.delete()
    print(f"REJECTED {object_name} -> {destination_name}. Reason: {reason}")


@functions_framework.cloud_event
def process_csv(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    if not object_name.startswith("raw/uploads/"):
        return "Ignored: outside raw/uploads/"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    # 1. Parse and validate file structure/data
    try:
        content = blob.download_as_text()
        reader = csv.DictReader(io.StringIO(content))

        if not reader.fieldnames:
            _move_to_rejected(bucket, blob, object_name, "CSV has no header")
            return "Rejected: CSV has no header"

        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            reason = f"Missing required columns: {sorted(missing)}"
            _move_to_rejected(bucket, blob, object_name, reason)
            return f"Rejected: {reason}"

        rows = []
        for line_number, row in enumerate(reader, start=2):
            quantity = int(row["quantity"])
            if quantity <= 0:
                reason = f"Invalid quantity at line {line_number}: {quantity}"
                _move_to_rejected(bucket, blob, object_name, reason)
                return f"Rejected: {reason}"

            status = row["order_status"]
            if status not in VALID_STATUSES:
                reason = f"Invalid status at line {line_number}: {status}"
                _move_to_rejected(bucket, blob, object_name, reason)
                return f"Rejected: {reason}"

            unit_price = float(row["unit_price"])
            order_total = round(quantity * unit_price, 2)

            created_at = datetime.fromisoformat(
                row["created_at"].replace("Z", "+00:00")
            )
            updated_at = datetime.fromisoformat(
                row["updated_at"].replace("Z", "+00:00")
            )

            rows.append(
                {
                    "order_id": row["order_id"],
                    "customer_id": row["customer_id"],
                    "product_id": row["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "order_total": order_total,
                    "order_status": status,
                    "region": row["region"],
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                    "order_date": created_at.date().isoformat(),
                    "source": "upload",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    except (ValueError, TypeError, KeyError) as e:
        # Catches parsing errors (e.g. invalid integer/float/ISO timestamp format)
        reason = f"Data parsing exception: {str(e)}"
        _move_to_rejected(bucket, blob, object_name, reason)
        return f"Rejected: {reason}"

    # 2. BigQuery Streaming Ingestion
    project_id = os.environ.get("PROJECT_ID")
    client = bigquery.Client(project=project_id)
    table_id = f"{client.project}.shopstream_staging.raw_orders_uploads"

    # Let BigQuery errors raise exceptions so Eventarc retries transient DB issues
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert failed: {errors}")

    return f"Inserted {len(rows)} rows from {object_name}"