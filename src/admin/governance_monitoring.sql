/* Governance & Monitoring — Stap 5
   Doel: governance-checks, metrics-view en performance-baseline.
   Regels: 2-delig, geen GO in proc-bodies, geen @@ROWCOUNT. */

------------------------------------------------------------
-- 1) Governance-checks: verboden patronen in proc-definities
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Governance_Checks]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Governance_Checks];
GO
CREATE PROCEDURE [dwh].[usp_Governance_Checks]
AS
BEGIN
  SET NOCOUNT ON;
  /* Checkt op: @@ROWCOUNT, SET ROWCOUNT, ' USE ' (batch switch). */
  SELECT 
    schema_name = OBJECT_SCHEMA_NAME(m.object_id),
    object_name = OBJECT_NAME(m.object_id),
    issue = CASE 
      WHEN m.definition LIKE '%@@ROWCOUNT%' THEN 'Forbidden: @@ROWCOUNT'
      WHEN m.definition LIKE '%SET ROWCOUNT%' THEN 'Forbidden: SET ROWCOUNT'
      WHEN m.definition LIKE '% USE %' THEN 'Forbidden: USE in module'
      ELSE 'OK'
    END,
    snippet = LEFT(m.definition, 4000)
  FROM sys.sql_modules AS m
  JOIN sys.objects o ON o.object_id = m.object_id AND o.type IN ('P','V','FN','TF','IF')
  WHERE m.definition LIKE '%@@ROWCOUNT%'
     OR m.definition LIKE '%SET ROWCOUNT%'
     OR m.definition LIKE '% USE %'
  ORDER BY schema_name, object_name;
END
GO

------------------------------------------------------------
-- 2) Metrics-view op JobRunLog (dag + proces)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[vJobRun_Metrics]', N'V') IS NOT NULL
  DROP VIEW [dwh].[vJobRun_Metrics];
GO
CREATE VIEW [dwh].[vJobRun_Metrics]
AS
SELECT
  RunDate      = CONVERT(date, StartTimeUtc),
  ProcessName,
  Runs         = COUNT_BIG(*),
  RowsRead     = SUM(COALESCE(RowsRead,0)),
  RowsInserted = SUM(COALESCE(RowsInserted,0)),
  RowsUpdated  = SUM(COALESCE(RowsUpdated,0)),
  RowsDeleted  = SUM(COALESCE(RowsDeleted,0))
FROM [dwh].[JobRunLog]
GROUP BY CONVERT(date, StartTimeUtc), ProcessName;
GO

------------------------------------------------------------
-- 3) Performance-baseline (waits, IO, top CPU queries)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Perf_Baseline]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Perf_Baseline];
GO
CREATE PROCEDURE [dwh].[usp_Perf_Baseline]
AS
BEGIN
  SET NOCOUNT ON;

  -- Waits top
  SELECT TOP (50)
    wait_type, waiting_tasks_count, wait_time_ms,
    pct = CAST(100.0*wait_time_ms / NULLIF(SUM(wait_time_ms) OVER(),0) AS decimal(5,2))
  FROM sys.dm_os_wait_stats
  WHERE wait_type NOT LIKE 'SLEEP%'
  ORDER BY wait_time_ms DESC;

  -- File IO
  SELECT DB_NAME(vfs.database_id) AS dbname, mf.name, vfs.num_of_reads, vfs.num_of_writes,
         vfs.io_stall_read_ms, vfs.io_stall_write_ms
  FROM sys.dm_io_virtual_file_stats(NULL,NULL) AS vfs
  JOIN sys.master_files AS mf ON mf.database_id=vfs.database_id AND mf.file_id=vfs.file_id
  ORDER BY (vfs.io_stall_read_ms+vfs.io_stall_write_ms) DESC;

  -- Top CPU queries
  SELECT TOP 20
    cpu_ms = qs.total_worker_time/1000.0,
    exec_count = qs.execution_count,
    sql_text = SUBSTRING(qt.text,1,4000)
  FROM sys.dm_exec_query_stats AS qs
  CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
  ORDER BY qs.total_worker_time DESC;
END
GO

------------------------------------------------------------
-- 4) Smoke: governance + metrics voorbeeldqueries
------------------------------------------------------------
-- Governance-run
EXEC [dwh].[usp_Governance_Checks];

-- Metrics laatste runs
SELECT TOP (20) *
FROM [dwh].[vJobRun_Metrics]
ORDER BY RunDate DESC, ProcessName;
