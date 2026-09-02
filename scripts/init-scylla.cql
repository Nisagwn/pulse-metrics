CREATE KEYSPACE IF NOT EXISTS pulse
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pulse;

CREATE TABLE IF NOT EXISTS metrics (
    service_name TEXT,
    metric_name TEXT,
    timestamp BIGINT,
    tags MAP<TEXT, TEXT>,
    value DOUBLE,
    PRIMARY KEY ((service_name, metric_name), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 2592000;
