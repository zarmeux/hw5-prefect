CREATE DATABASE IF NOT EXISTS hw5;

CREATE EXTERNAL TABLE IF NOT EXISTS hw5.events_daily (
  events_count BIGINT,
  unique_users BIGINT,
  page_views BIGINT,
  signups BIGINT,
  purchases BIGINT,
  purchase_share DOUBLE
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse-external/hw5/events_daily'
TBLPROPERTIES ('external.table.purge'='false');

MSCK REPAIR TABLE hw5.events_daily SYNC PARTITIONS;
SHOW PARTITIONS hw5.events_daily;
DESCRIBE FORMATTED hw5.events_daily;
