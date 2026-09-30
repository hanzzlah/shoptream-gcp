import argparse
from pyspark.sql import SparkSession  # type:ignore
from pyspark.sql import functions as F  # type:ignore
from pyspark.sql.window import Window  # type:ignore


def parse_args():
    parser = argparse.ArgumentParser(
        description="ShopStream Batch Orders Ingestion to BigQuery"
    )
    parser.add_argument("--jdbc-url", required=True, help="Supabase PostgreSQL JDBC URL")
    parser.add_argument("--jdbc-user", required=True, help="Database user")
    parser.add_argument("--jdbc-password", required=True, help="Database password")
    parser.add_argument("--target-project", required=True, help="GCP Project ID")
    parser.add_argument("--target-dataset", required=True, help="BigQuery Dataset ID")
    parser.add_argument("--target-table", required=True, help="BigQuery Table Name")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("ShopStreamBatchOrders")
        .getOrCreate()
    )

    # 1. Read raw orders from Supabase PostgreSQL via JDBC
    orders = (
        spark.read
        .format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", "shopstream.orders")
        .option("user", args.jdbc_user)
        .option("password", args.jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # 2. Window definition for deduplication by latest update timestamp
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

    # 3. Clean, deduplicate, validate, and enrich records
    clean = (
        orders
        # Validate data quality standards
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        # Deduplicate to keep latest order state per order_id
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        # Sanitize string fields
        .withColumn("region", F.trim(F.col("region")))
        # Compute total price rounded to 2 decimal places
        .withColumn(
            "order_total",
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        # Derive date field for BigQuery table partitioning
        .withColumn("order_date", F.to_date("created_at"))
        # Ingestion lineage metadata
        .withColumn("source", F.lit("batch"))
        .withColumn("ingested_at", F.current_timestamp())
        # Schema projection with fixed syntax (comma restored)
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_total",
            "order_status",
            "region",
            "created_at",
            "updated_at",
            "order_date",
            "source",
            "ingested_at",
        )
    )

    # 4. Write directly to BigQuery using BigQuery Storage Write API
    (
        clean.write
        .format("bigquery")
        .option(
            "table",
            f"{args.target_project}:{args.target_dataset}.{args.target_table}",
        )
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()