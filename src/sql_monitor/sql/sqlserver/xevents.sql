WITH session_targets AS (
    SELECT CAST(st.target_data AS XML) AS target_data
    FROM sys.dm_xe_sessions s
    INNER JOIN sys.dm_xe_session_targets st
        ON s.address = st.event_session_address
    WHERE s.name = :session_name
      AND st.target_name = 'ring_buffer'
)
SELECT
    SYSUTCDATETIME() AS capture_time,
    evt.value('@timestamp', 'datetime2') AS event_time,
    evt.value('@name', 'nvarchar(128)') AS event_name,
    evt.value('(action[@name="session_id"]/value)[1]', 'int') AS session_id,
    evt.value('(action[@name="database_name"]/value)[1]', 'nvarchar(128)') AS database_name,
    evt.value('(action[@name="client_app_name"]/value)[1]', 'nvarchar(512)') AS client_app_name,
    evt.value('(action[@name="client_hostname"]/value)[1]', 'nvarchar(256)') AS client_hostname,
    evt.value('(action[@name="username"]/value)[1]', 'nvarchar(256)') AS username,
    evt.value('(data[@name="error_number"]/value)[1]', 'int') AS error_number,
    evt.value('(data[@name="severity"]/value)[1]', 'int') AS severity,
    evt.value('(data[@name="state"]/value)[1]', 'int') AS state,
    evt.value('(data[@name="duration"]/value)[1]', 'bigint') AS duration_ms,
    evt.value('(data[@name="message"]/value)[1]', 'nvarchar(4000)') AS message,
    evt.value('(action[@name="sql_text"]/value)[1]', 'nvarchar(4000)') AS sql_text,
    CAST(evt.query('.') AS NVARCHAR(MAX)) AS event_xml
FROM session_targets st
CROSS APPLY st.target_data.nodes('/RingBufferTarget/event') AS x(evt)
ORDER BY event_time DESC;
