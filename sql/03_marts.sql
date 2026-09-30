CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_marts.dim_customer` AS
SELECT customer_id, first_order_date, last_order_date, order_count, lifetime_value
FROM `${PROJECT_ID}.shopstream_curated.customer_lifetime_value`;

CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_marts.dim_product` AS
WITH products AS (
  SELECT 'P001' product_id, 'Laptop Pro 14' product_name, 'Computers' category, 799.00 list_price UNION ALL
  SELECT 'P002', 'USB-C Hub', 'Accessories', 49.99 UNION ALL
  SELECT 'P003', 'Mechanical Keyboard', 'Accessories', 129.00 UNION ALL
  SELECT 'P004', 'Wireless Mouse', 'Accessories', 29.99 UNION ALL
  SELECT 'P005', 'Noise Cancelling Headphones', 'Audio', 199.00 UNION ALL
  SELECT 'P006', '1080p Webcam', 'Video', 89.00 UNION ALL
  SELECT 'P007', 'Desk Lamp', 'Office', 59.00 UNION ALL
  SELECT 'P008', 'Phone Stand', 'Accessories', 24.50 UNION ALL
  SELECT 'P009', 'Portable SSD', 'Storage', 149.00 UNION ALL
  SELECT 'P010', 'Wireless Charger', 'Accessories', 39.95
)
SELECT * FROM products;

CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_marts.fact_orders` AS
SELECT * FROM `${PROJECT_ID}.shopstream_curated.orders_enriched`;

CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_marts.agg_daily_sales` AS
SELECT
  order_date,
  COUNT(*) AS order_count,
  SUM(quantity) AS units,
  SUM(order_total) AS revenue
FROM `${PROJECT_ID}.shopstream_curated.orders_enriched`
WHERE order_status != 'CANCELLED'
GROUP BY order_date;