import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam  # type: ignore
from apache_beam.io.gcp.bigquery import (  # type: ignore
    BigQueryDisposition,
    WriteToBigQuery,
)
from apache_beam.io.gcp.bigtableio import WriteToBigTable  # type: ignore
from apache_beam.io.gcp.pubsub import ReadFromPubSub  # type: ignore
from apache_beam.options.pipeline_options import (  # type: ignore
    PipelineOptions,
    StandardOptions,
)
from apache_beam.utils.timestamp import Timestamp  # type:ignore
from google.cloud import bigquery  # type: ignore
from google.cloud.bigtable.row import DirectRow  # type: ignore
from google.cloud.exceptions import GoogleCloudError  # type: ignore

logging.basicConfig(level=logging.INFO)

REQUIRED_FIELDS = {
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_status",
    "region",
    "event_timestamp",
}

VALID_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED"}

# Unified BigQuery Schema
VALID_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING"},
        {"name": "customer_id", "type": "STRING"},
        {"name": "product_id", "type": "STRING"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price", "type": "FLOAT"},
        {"name": "order_total", "type": "FLOAT"},
        {"name": "order_status", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "order_date", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "ingested_at", "type": "TIMESTAMP"},
    ]
}


def _now_bq():
    # Return Beam Timestamp object instead of datetime/string
    return Timestamp.now()


def ensure_bq_table_exists(table_spec: str, schema_dict: dict):
    """Pre-creates BigQuery table before pipeline run to prevent worker rate-limit errors."""
    try:
        client = bigquery.Client()
        schema_fields = [
            bigquery.SchemaField(field["name"], field["type"])
            for field in schema_dict["fields"]
        ]

        table_ref = table_spec.replace(":", ".")
        table = bigquery.Table(table_ref, schema=schema_fields)
        client.create_table(table, exists_ok=True)
        logging.info(
            f"[INIT OK] Verified BigQuery table existence for: {table_spec}"
        )

    except GoogleCloudError as e:
        logging.warning(
            f"[INIT NOTICE] Could not pre-create table '{table_spec}': {e.message}"
        )
    except Exception as e:
        logging.error(
            f"[INIT ERROR] Unexpected error during table check for '{table_spec}': {str(e)}"
        )


class ParseAndValidate(beam.DoFn):

    def process(self, element):
        raw_payload = element.decode("utf-8")
        logging.info(f"--> BEAM RECEIVED PAYLOAD: {raw_payload}")

        data = json.loads(raw_payload)

        # 1. Validate required fields presence
        missing = REQUIRED_FIELDS - data.keys()
        if missing:
            raise ValueError(
                f"Missing required fields: {missing} in payload: {raw_payload}"
            )

        quantity = int(data["quantity"])
        unit_price = float(data["unit_price"])

        # 2. Validate quantity and order status
        if quantity <= 0:
            raise ValueError(f"Invalid quantity ({quantity}). Must be > 0.")

        if data.get("order_status") not in VALID_STATUSES:
            raise ValueError(
                f"Invalid order_status '{data.get('order_status')}'. Must be one of {VALID_STATUSES}."
            )

        # 3. Parse timestamp into Beam Timestamp representation
        raw_ts = data["event_timestamp"]
        dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        beam_ts = Timestamp.from_utc_datetime(dt)

        data["quantity"] = quantity
        data["unit_price"] = unit_price
        data["order_total"] = round(quantity * unit_price, 2)
        
        # Pass Beam Timestamp objects for BigQuery TIMESTAMP fields
        data["created_at"] = beam_ts
        data["updated_at"] = beam_ts
        
        data["order_date"] = dt.date().isoformat()
        data["source"] = "streaming"

        yield data


class ToBigQueryRow(beam.DoFn):

    def process(self, data):
        yield {
            "order_id": data["order_id"],
            "customer_id": data["customer_id"],
            "product_id": data["product_id"],
            "quantity": data["quantity"],
            "unit_price": data["unit_price"],
            "order_total": data["order_total"],
            "order_status": data["order_status"],
            "region": data["region"],
            "created_at": data["created_at"],  # apache_beam.utils.timestamp.Timestamp
            "updated_at": data["updated_at"],  # apache_beam.utils.timestamp.Timestamp
            "order_date": data["order_date"],
            "source": data["source"],
            "ingested_at": _now_bq(),           # apache_beam.utils.timestamp.Timestamp
        }


class ToBigtableRow(beam.DoFn):

    def process(self, data):
        row_key = f"{data['customer_id']}#{data['order_date']}".encode("utf-8")
        row = DirectRow(row_key=row_key)

        row.set_cell("order", "order_id", data["order_id"].encode("utf-8"))
        row.set_cell("order", "product_id", data["product_id"].encode("utf-8"))
        row.set_cell(
            "order", "order_total", str(data["order_total"]).encode("utf-8")
        )
        row.set_cell("order", "status", data["order_status"].encode("utf-8"))
        row.set_cell("order", "region", data["region"].encode("utf-8"))
        yield row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--bigtable_project", default="")
    parser.add_argument("--bigtable_instance", default="")
    parser.add_argument("--bigtable_table", default="")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, save_main_session=True)
    options.view_as(StandardOptions).streaming = True

    # Verify table existence before job graph starts
    ensure_bq_table_exists(known_args.output_table, VALID_SCHEMA)

    with beam.Pipeline(options=options) as pipeline:
        valid_data = (
            pipeline
            | "ReadPubSub"
            >> ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidate())
        )

        # Write to BigQuery
        _ = (
            valid_data
            | "BQRows" >> beam.ParDo(ToBigQueryRow())
            | "WriteValidBigQuery"
            >> WriteToBigQuery(
                known_args.output_table,
                schema=VALID_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        )

        # Write to Bigtable (if arguments provided)
        if known_args.bigtable_instance and known_args.bigtable_table:
            _ = (
                valid_data
                | "FormatBigtable" >> beam.ParDo(ToBigtableRow())
                | "WriteBigtable"
                >> WriteToBigTable(
                    project_id=known_args.bigtable_project,
                    instance_id=known_args.bigtable_instance,
                    table_id=known_args.bigtable_table,
                )
            )


if __name__ == "__main__":
    run()