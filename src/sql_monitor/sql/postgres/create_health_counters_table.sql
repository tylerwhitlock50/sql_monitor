CREATE TABLE IF NOT EXISTS sqlserver_health_counters_log (
    id BIGSERIAL PRIMARY KEY,
    capture_time TIMESTAMPTZ,
    object_name VARCHAR(128),
    counter_name VARCHAR(128),
    instance_name VARCHAR(128),
    cntr_type INT,
    cntr_value BIGINT
);
