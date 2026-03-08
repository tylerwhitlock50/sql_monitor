INSERT INTO sqlserver_health_counters_log (
    capture_time,
    object_name,
    counter_name,
    instance_name,
    cntr_type,
    cntr_value
) VALUES (
    %s, %s, %s, %s, %s, %s
)
