/* 00_smoke.sql — basiscontrole na deploy */
SET NOCOUNT ON;

-- DB-opties
SELECT 'RCSI' AS check_name, is_read_committed_snapshot_on FROM sys.databases WHERE database_id = DB_ID();
SELECT 'QStore' AS check_name, actual_state_desc FROM sys.database_query_store_options;

-- Kernobjecten
SELECT 'JobRunLog'     AS obj, OBJECT_ID(N'dwh.JobRunLog')        AS id
UNION ALL SELECT 'JobRunLogEvent', OBJECT_ID(N'dwh.JobRunLogEvent')
UNION ALL SELECT 'LoadConfig',     OBJECT_ID(N'dwh.LoadConfig')
UNION ALL SELECT 'Watermark',      OBJECT_ID(N'dwh.Watermark')
UNION ALL SELECT 'usp_JobRun_Start',       OBJECT_ID(N'dwh.usp_JobRun_Start')
UNION ALL SELECT 'usp_JobRun_End',         OBJECT_ID(N'dwh.usp_JobRun_End')
UNION ALL SELECT 'usp_Watermark_Get',      OBJECT_ID(N'dwh.usp_Watermark_Get')
UNION ALL SELECT 'usp_Watermark_Set',      OBJECT_ID(N'dwh.usp_Watermark_Set')
UNION ALL SELECT 'usp_Load_GenericUpsert', OBJECT_ID(N'dwh.usp_Load_GenericUpsert')
UNION ALL SELECT 'usp_Load_GenericIncrDate', OBJECT_ID(N'dwh.usp_Load_GenericIncrDate')
UNION ALL SELECT 'usp_Dispatch_Load',      OBJECT_ID(N'dwh.usp_Dispatch_Load');

-- Mini dry-run (faalt niet, logt wel)
DECLARE @jr UNIQUEIDENTIFIER;
EXEC dwh.usp_JobRun_Start N'smoke', @jr OUTPUT;
EXEC dwh.usp_JobRun_Event @jr, N'TEST', N'Smoke event';
EXEC dwh.usp_JobRun_End   @jr, N'Succeeded', 0,0,0,0, NULL;

SELECT TOP (5) * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;
