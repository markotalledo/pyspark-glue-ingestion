-- Query the clean events from Athena without a crawler: partition projection
-- derives partitions from the path, so new dates are visible as soon as they land.
-- Replace <clean-bucket> and the database name with the Terraform outputs.

CREATE EXTERNAL TABLE shop_events_dev_clean.events (
  event_id string,
  event_name string,
  occurred_at timestamp,
  sent_at timestamp,
  received_at timestamp,
  arrival_lag_hours double,
  anonymous_id string,
  customer_id string,
  session_id string,
  source string,
  product_id string,
  category string,
  price_cents bigint,
  quantity int,
  cart_value_cents bigint,
  order_id string,
  total_cents bigint,
  currency string,
  amount_cents bigint,
  payment_method string
)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://<clean-bucket>/events/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'projection.event_date.interval' = '1',
  'projection.event_date.interval.unit' = 'DAYS',
  'storage.location.template' = 's3://<clean-bucket>/events/event_date=${event_date}/'
);

-- Conversion funnel by source, last 7 days.
SELECT
  source,
  count_if(event_name = 'product_viewed') AS views,
  count_if(event_name = 'product_added_to_cart') AS adds,
  count_if(event_name = 'order_completed') AS orders
FROM shop_events_dev_clean.events
WHERE event_date >= current_date - interval '7' day
GROUP BY source
ORDER BY orders DESC;
