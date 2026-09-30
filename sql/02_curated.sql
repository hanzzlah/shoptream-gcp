-- Unified and deduplicated orders
CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_curated.orders_enriched` AS
SELECT * EXCEPT(rn)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY order_id
           ORDER BY ingested_at DESC
         ) AS rn
  FROM (
    SELECT * FROM `${PROJECT_ID}.shopstream_staging.raw_orders_batch`
    UNION ALL
    SELECT * FROM `${PROJECT_ID}.shopstream_staging.raw_orders_streaming`
    UNION ALL
    SELECT * FROM `${PROJECT_ID}.shopstream_staging.raw_orders_uploads`
  )
)
WHERE rn = 1;

-- Daily sales by region
CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_curated.daily_sales_by_region` AS
SELECT
  order_date,
  region,
  COUNT(*) AS order_count,
  SUM(quantity) AS units,
  SUM(order_total) AS revenue
FROM `${PROJECT_ID}.shopstream_curated.orders_enriched`
WHERE order_status != 'CANCELLED'
GROUP BY order_date, region;

-- Top products by revenue
CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_curated.top_products_by_revenue` AS
SELECT
  product_id,
  SUM(quantity) AS units,
  SUM(order_total) AS revenue
FROM `${PROJECT_ID}.shopstream_curated.orders_enriched`
WHERE order_status != 'CANCELLED'
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- Customer lifetime value
CREATE OR REPLACE TABLE `${PROJECT_ID}.shopstream_curated.customer_lifetime_value` AS
SELECT
  customer_id,
  COUNT(*) AS order_count,
  SUM(quantity) AS units,
  SUM(order_total) AS lifetime_value,
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date
FROM `${PROJECT_ID}.shopstream_curated.orders_enriched`
WHERE order_status != 'CANCELLED'
GROUP BY customer_id;