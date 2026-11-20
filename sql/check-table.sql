SET hive.fetch.task.conversion=more;
SET hive.compute.query.using.stats=false;
SELECT event_date, events_count, unique_users, page_views, signups, purchases, purchase_share
FROM hw5.events_daily;
