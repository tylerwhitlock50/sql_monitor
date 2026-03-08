SELECT
    SYSUTCDATETIME() AS capture_time,
    object_name,
    counter_name,
    instance_name,
    cntr_type,
    cntr_value
FROM sys.dm_os_performance_counters
WHERE counter_name IN (
    'Number of Deadlocks/sec',
    'Lock Timeouts/sec',
    'Lock Waits/sec'
)
ORDER BY counter_name, instance_name;
