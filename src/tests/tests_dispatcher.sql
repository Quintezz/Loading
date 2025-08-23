/* tests_dispatcher.sql — End-to-end test voor dwh.usp_Dispatch_Load (FULL + INCR_DATE)
   Regels: 2-delig, geen @@ROWCOUNT. Alles in 1 transactie met ROLLBACK. */

SET NOCOUNT ON;
BEGIN TRAN;

-- 0) Vereisten
IF OBJECT_ID(N'[dwh].[usp_Dispatch_Load]', N'P') IS NULL
  THROW 51010, 'Dispatcher ontbreekt: dwh.usp_Dispatch_Load', 1;

-- 1) Testtabellen
IF OBJECT_ID(N'[silver].[DimProductV2_TEST]', N'U') IS NOT NULL DROP TABLE [silver].[DimProductV2_TEST];
CREATE TABLE [silver].[DimProductV2_TEST](
  [ProductBK] int NOT NULL,
  [Name]      nvarchar(200) NOT NULL
);

IF OBJECT_ID(N'[dwh].[DimProduct_TEST]', N'U') IS NOT NULL DROP TABLE [dwh].[DimProduct_TEST];
CREATE TABLE [dwh].[DimProduct_TEST](
  [ProductBK] int NOT NULL CONSTRAINT PK_dwh_DimProduct_TEST PRIMARY KEY,
  [Name]      nvarchar(200) NOT NULL
);

IF OBJECT_ID(N'[silver].[IncrDemo_TEST]', N'U') IS NOT NULL DROP TABLE [silver].[IncrDemo_TEST];
CREATE TABLE [silver].[IncrDemo_TEST](
  [BK]      int NOT NULL,
  [DateCol] datetime2(3) NOT NULL,
  [Val]     nvarchar(100) NOT NULL
);

IF OBJECT_ID(N'[dwh].[IncrDemo_TEST]', N'U') IS NOT NULL DROP TABLE [dwh].[IncrDemo_TEST];
CREATE TABLE [dwh].[IncrDemo_TEST](
  [BK]      int NOT NULL CONSTRAINT PK_dwh_IncrDemo_TEST PRIMARY KEY,
  [DateCol] datetime2(3) NOT NULL,
  [Val]     nvarchar(100) NOT NULL
);

-- 2) Seed brondata
INSERT INTO [silver].[DimProductV2_TEST]([ProductBK],[Name]) VALUES (1,N'A'),(2,N'B');
INSERT INTO [silver].[IncrDemo_TEST] ([BK],[DateCol],[Val]) VALUES
 (1,'2025-01-01T00:00:00.000',N'v1'),
 (2,'2025-01-02T00:00:00.000',N'v2'),
 (3,'2025-01-02T12:00:00.000',N'v3');

-- 3) LoadConfig regels voor dispatcher (Env=TST, Enabled=1)
MERGE [dwh].[LoadConfig] AS T
USING (VALUES
 (N'tests.Disp.Full_DimProduct_TEST', N'TST', N'FULL',      N'silver', N'DimProductV2_TEST', N'dwh', N'DimProduct_TEST', N'ProductBK', N'Name',    NULL,     NULL, 1, 1, 0, N'dispatch full test'),
 (N'tests.Disp.IncrDemo_TST',        N'TST', N'INCR_DATE', N'silver', N'IncrDemo_TEST',     N'dwh', N'IncrDemo_TEST',  N'BK',        N'Val',     N'DateCol', NULL, 1, 1, 0, N'dispatch incr test')
) AS S(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,OverlapDays,Comment)
ON (T.ProcessName=S.ProcessName)
WHEN MATCHED THEN UPDATE SET
  Env=S.Env, LoadType=S.LoadType, SourceSchema=S.SourceSchema, SourceObject=S.SourceObject,
  TargetSchema=S.TargetSchema, TargetTable=S.TargetTable, KeyColumns=S.KeyColumns, UpdateColumns=S.UpdateColumns,
  WatermarkColumn=S.WatermarkColumn, BatchSize=S.BatchSize, RequireUniqueKey=S.RequireUniqueKey, Enabled=S.Enabled, OverlapDays=S.OverlapDays, Comment=S.Comment
WHEN NOT MATCHED THEN
  INSERT(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,OverlapDays,Comment)
  VALUES(S.ProcessName,S.Env,S.LoadType,S.SourceSchema,S.SourceObject,S.TargetSchema,S.TargetTable,S.KeyColumns,S.UpdateColumns,S.WatermarkColumn,S.BatchSize,S.RequireUniqueKey,S.Enabled,S.OverlapDays,S.Comment);

-- Reset watermark voor INCR
EXEC [dwh].[usp_Watermark_Set] @ProcessName=N'tests.Disp.IncrDemo_TST', @ValueDateTime='1900-01-01';

-- 4) Dispatcher run 1 (verwacht: FULL 2 insert; INCR 3 insert)
EXEC [dwh].[usp_Dispatch_Load] @Env=N'TST';

DECLARE @ri_full bigint, @ru_full bigint, @ri_incr bigint, @ru_incr bigint;
SELECT TOP (1) @ri_full=RowsInserted, @ru_full=RowsUpdated
FROM [dwh].[JobRunLog] WHERE ProcessName=N'tests.Disp.Full_DimProduct_TEST' ORDER BY StartTimeUtc DESC;
SELECT TOP (1) @ri_incr=RowsInserted, @ru_incr=RowsUpdated
FROM [dwh].[JobRunLog] WHERE ProcessName=N'tests.Disp.IncrDemo_TST' ORDER BY StartTimeUtc DESC;

IF NOT (@ri_full=2 AND @ru_full=0) THROW 53001, 'Dispatcher: FULL run1 verwacht 2/0', 1;
IF NOT (@ri_incr=3 AND @ru_incr=0) THROW 53002, 'Dispatcher: INCR run1 verwacht 3/0', 1;

-- 5) Dispatcher run 2 (idempotent: verwacht 0/0 voor beide)
EXEC [dwh].[usp_Dispatch_Load] @Env=N'TST';
SELECT TOP (1) @ri_full=RowsInserted, @ru_full=RowsUpdated
FROM [dwh].[JobRunLog] WHERE ProcessName=N'tests.Disp.Full_DimProduct_TEST' ORDER BY StartTimeUtc DESC;
SELECT TOP (1) @ri_incr=RowsInserted, @ru_incr=RowsUpdated
FROM [dwh].[JobRunLog] WHERE ProcessName=N'tests.Disp.IncrDemo_TST' ORDER BY StartTimeUtc DESC;
IF NOT (@ri_full=0 AND @ru_full=0) THROW 53003, 'Dispatcher: FULL run2 moet 0/0 zijn', 1;
IF NOT (@ri_incr=0 AND @ru_incr=0) THROW 53004, 'Dispatcher: INCR run2 moet 0/0 zijn', 1;

-- 6) Nieuwe latere INCR-rij + Dispatcher run 3 (verwacht: INCR 1 insert)
INSERT INTO [silver].[IncrDemo_TEST] ([BK],[DateCol],[Val]) VALUES (4, DATEADD(day, 1, (SELECT MAX(DateCol) FROM [silver].[IncrDemo_TEST])), N'v4');
EXEC [dwh].[usp_Dispatch_Load] @Env=N'TST';
SELECT TOP (1) @ri_incr=RowsInserted, @ru_incr=RowsUpdated
FROM [dwh].[JobRunLog] WHERE ProcessName=N'tests.Disp.IncrDemo_TST' ORDER BY StartTimeUtc DESC;
IF NOT (@ri_incr=1 AND @ru_incr=0) THROW 53005, 'Dispatcher: INCR run3 verwacht 1/0', 1;

PRINT 'DISPATCHER TESTS OK';
ROLLBACK TRAN;
