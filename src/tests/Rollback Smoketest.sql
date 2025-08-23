/* tests_loaders.sql — End-to-end tests FULL & INCR_DATE
   Fix: geen CTE vóór IF. Gebruik variabelen. Alles in 1 transactie en ROLLBACK. */

SET NOCOUNT ON;
BEGIN TRAN;

------------------------------------------------------------
-- TEST 1: FULL — dwh.usp_Load_GenericUpsert
------------------------------------------------------------
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

INSERT INTO [silver].[DimProductV2_TEST]([ProductBK],[Name]) VALUES (1,N'A'),(2,N'B');

-- 1A. Eerste run verwacht 2 inserts, 0 updates
EXEC [dwh].[usp_Load_GenericUpsert]
  @ProcessName   = N'tests.FULL_DimProduct_TEST',
  @SourceSchema  = N'silver',
  @SourceObject  = N'DimProductV2_TEST',
  @TargetSchema  = N'dwh',
  @TargetTable   = N'DimProduct_TEST',
  @KeyColumns    = N'ProductBK',
  @UpdateColumns = N'Name';

DECLARE @ri bigint, @ru bigint;
SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.FULL_DimProduct_TEST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 2 AND @ru = 0)
  THROW 51001, 'FULL: eerste run verwacht RowsInserted=2, RowsUpdated=0', 1;

-- 1B. Idempotent: verwacht 0/0
EXEC [dwh].[usp_Load_GenericUpsert]
  @ProcessName   = N'tests.FULL_DimProduct_TEST',
  @SourceSchema  = N'silver',
  @SourceObject  = N'DimProductV2_TEST',
  @TargetSchema  = N'dwh',
  @TargetTable   = N'DimProduct_TEST',
  @KeyColumns    = N'ProductBK',
  @UpdateColumns = N'Name';

SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.FULL_DimProduct_TEST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 0 AND @ru = 0)
  THROW 51002, 'FULL: tweede run moet 0/0 zijn', 1;

-- 1C. Wijzig bron en verwacht update=1
UPDATE [silver].[DimProductV2_TEST] SET [Name] = N'A+' WHERE [ProductBK]=1;
EXEC [dwh].[usp_Load_GenericUpsert]
  @ProcessName   = N'tests.FULL_DimProduct_TEST',
  @SourceSchema  = N'silver',
  @SourceObject  = N'DimProductV2_TEST',
  @TargetSchema  = N'dwh',
  @TargetTable   = N'DimProduct_TEST',
  @KeyColumns    = N'ProductBK',
  @UpdateColumns = N'Name';

SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.FULL_DimProduct_TEST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 0 AND @ru = 1)
  THROW 51003, 'FULL: update-run verwacht RowsInserted=0, RowsUpdated=1', 1;

------------------------------------------------------------
-- TEST 2: INCR_DATE — dwh.usp_Load_GenericIncrDate
------------------------------------------------------------
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

-- Seed bron
INSERT INTO [silver].[IncrDemo_TEST] ([BK],[DateCol],[Val]) VALUES
 (1,'2025-01-01T00:00:00.000',N'v1'),
 (2,'2025-01-02T00:00:00.000',N'v2'),
 (3,'2025-01-02T12:00:00.000',N'v3');

-- Config entry
MERGE [dwh].[LoadConfig] AS T
USING (VALUES
 (N'tests.IncrDemo__dwh.IncrDemo_TST', N'TST', N'INCR_DATE', N'silver', N'IncrDemo_TEST', N'dwh', N'IncrDemo_TEST', N'BK', N'Val', N'DateCol', NULL, 1, 1, N'demo incr test')
) AS S(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
ON (T.ProcessName=S.ProcessName)
WHEN MATCHED THEN UPDATE SET Env=S.Env, LoadType=S.LoadType, SourceSchema=S.SourceSchema, SourceObject=S.SourceObject,
  TargetSchema=S.TargetSchema, TargetTable=S.TargetTable, KeyColumns=S.KeyColumns, UpdateColumns=S.UpdateColumns,
  WatermarkColumn=S.WatermarkColumn, BatchSize=S.BatchSize, RequireUniqueKey=S.RequireUniqueKey, Enabled=S.Enabled, Comment=S.Comment
WHEN NOT MATCHED THEN
  INSERT(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
  VALUES(S.ProcessName,S.Env,S.LoadType,S.SourceSchema,S.SourceObject,S.TargetSchema,S.TargetTable,S.KeyColumns,S.UpdateColumns,S.WatermarkColumn,S.BatchSize,S.RequireUniqueKey,S.Enabled,S.Comment);

-- Reset WM naar laagste
EXEC [dwh].[usp_Watermark_Set] @ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST', @ValueDateTime='1900-01-01';

-- 2A. Eerste run: verwacht 3 inserts, 0 updates
EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST';
SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.IncrDemo__dwh.IncrDemo_TST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 3 AND @ru = 0)
  THROW 52001, 'INCR: eerste run verwacht RowsInserted=3, RowsUpdated=0', 1;

-- 2B. Idempotent: tweede run 0/0
EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST';
SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.IncrDemo__dwh.IncrDemo_TST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 0 AND @ru = 0)
  THROW 52002, 'INCR: tweede run moet 0/0 zijn', 1;

-- 2C. Nieuwe latere rij → verwacht 1 insert
INSERT INTO [silver].[IncrDemo_TEST] ([BK],[DateCol],[Val]) VALUES (4, DATEADD(day, 1, (SELECT MAX(DateCol) FROM [silver].[IncrDemo_TEST])), N'v4');
EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST';
SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.IncrDemo__dwh.IncrDemo_TST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 1 AND @ru = 0)
  THROW 52003, 'INCR: nieuwe latere rij verwacht RowsInserted=1, RowsUpdated=0', 1;

-- 2D. Back-dated rij (onder current WM) → verwacht 0/0 met huidige logica
DECLARE @currentWM datetime2;
SELECT @currentWM = ValueDateTime FROM [dwh].[Watermark] WHERE ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST';
INSERT INTO [silver].[IncrDemo_TEST] ([BK],[DateCol],[Val]) VALUES (5, DATEADD(day, -2, @currentWM), N'backdated');
EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=N'tests.IncrDemo__dwh.IncrDemo_TST';
SELECT TOP (1) @ri = RowsInserted, @ru = RowsUpdated
FROM [dwh].[JobRunLog]
WHERE ProcessName = N'tests.IncrDemo__dwh.IncrDemo_TST'
ORDER BY StartTimeUtc DESC;
IF NOT (@ri = 0 AND @ru = 0)
  THROW 52004, 'INCR: back-dated rij zou niet geladen mogen worden zonder overlap', 1;

PRINT 'ALLE TESTS OK';
ROLLBACK TRAN;
