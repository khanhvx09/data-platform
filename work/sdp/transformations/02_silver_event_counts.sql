CREATE MATERIALIZED VIEW sdp_silver_event_counts AS
SELECT event_type, COUNT(*) AS event_count
FROM sdp_bronze_events
GROUP BY event_type
