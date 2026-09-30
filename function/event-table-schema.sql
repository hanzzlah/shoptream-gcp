-- Create dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.shopstream_staging`
OPTIONS (
  location = "us-central1"
);

-- Create staging table matching process_csv output schema
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.shopstream_staging.raw_orders_uploads` (
  order_id STRING OPTIONS(description="Unique order ID"),
  customer_id STRING OPTIONS(description="Customer identifier"),
  product_id STRING OPTIONS(description="Product identifier"),
  quantity INT64 OPTIONS(description="Quantity ordered"),
  unit_price NUMERIC OPTIONS(description="Unit price"),
  order_total NUMERIC OPTIONS(description="Total order amount"),
  order_status STRING OPTIONS(description="Status: PLACED, SHIPPED, DELIVERED, CANCELLED"),
  region STRING OPTIONS(description="Order region"),
  created_at TIMESTAMP OPTIONS(description="Order creation timestamp"),
  updated_at TIMESTAMP OPTIONS(description="Order last updated timestamp"),
  order_date DATE OPTIONS(description="Date derived from created_at"),
  source STRING OPTIONS(description="Ingestion source"),
  ingested_at TIMESTAMP OPTIONS(description="Ingestion timestamp")
)
PARTITION BY order_date
CLUSTER BY region, order_status;