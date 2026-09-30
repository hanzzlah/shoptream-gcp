SELECT 'batch' AS source, COUNT(*) AS order_count FROM `${PROJECT_ID}.shopstream_staging.raw_orders_batch`
UNION ALL
SELECT 'streaming', COUNT(*) FROM `${PROJECT_ID}.shopstream_staging.raw_orders_streaming`
UNION ALL
SELECT 'uploads', COUNT(*) FROM `${PROJECT_ID}.shopstream_staging.raw_orders_uploads`