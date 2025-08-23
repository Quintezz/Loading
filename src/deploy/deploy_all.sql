/* deploy_all.sql — end-to-end deploy
   Regels: 2-delige namen, geen GO in runtime-procs. GO hier toegestaan. */

------------------------------------------------------------
-- Schemas
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'dwh')    EXEC(N'CREATE SCHEMA [dwh]');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name=N'silver') EXEC(N'CREATE SCHEMA [silver]');
GO

------------------------------------------------------------
-- Database-opties
------------------------------------------------------------
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;
ALTER DATABASE CURRENT SET QUERY_STORE = ON;
ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY=(STALE_QUERY_THRESHOLD_DAYS=30));
ALTER DATABASE CURRENT SET AUTOMATIC_TUNING (FORCE_LAST_GOOD_PLAN = ON, CREATE_INDEX = ON, DROP_INDEX = ON);
GO

------------------------------------------------------------
-- Logging tabellen
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[JobRunLog]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[JobRunLog](
    JobRunId       UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_dwh_JobRunLog PRIMARY KEY DEFAULT NEWID(),
    ProcessName    NVARCHAR(200)    NOT NULL,
    StartTimeUtc   DATETIME2(3)     NOT NULL CONSTRAINT DF_dwh_JobRunLog_Start DEFAULT SYSUTCDATETIME(),
    EndTimeUtc     DATETIME2(3)     NULL,
    Status         NVARCHAR(30)     NULL,
    RowsRead       BIGINT           NULL,
    RowsInserted   BIGINT           NULL,
    RowsUpdated    BIGINT           NULL,
    RowsDeleted    BIGINT           NULL,
    ErrorMessage   NVARCHAR(MAX)    NULL,
    HostName       NVARCHAR(128)    NULL,
    AppName        NVARCHAR(128)    NULL
  );
END;

IF OBJECT_ID(N'[dwh].[JobRunLogEvent]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[JobRunLogEvent](
    JobRunEventId  BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dwh_JobRunLogEvent PRIMARY KEY,
    JobRunId       UNIQUEIDENTIFIER NOT NULL,
    EventTimeUtc   DATETIME2(3)     NOT NULL CONSTRAINT DF_dwh_JobRunLogEvent_Time DEFAULT SYSUTCDATETIME(),
    EventType      NVARCHAR(100)    NOT NULL,
    EventDetail    NVARCHAR(MAX)    NULL,
    Metric1Name    NVARCHAR(100)    NULL,
    Metric1Value   SQL_VARIANT      NULL,
    Metric2Name    NVARCHAR(100)    NULL,
    Metric2Value   SQL_VARIANT      NULL
  );
  CREATE INDEX IX_dwh_JobRunLogEvent_JobRunId ON [dwh].[JobRunLogEvent](JobRunId, EventTimeUtc);
END;
GO

------------------------------------------------------------
-- Logging procs
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_JobRun_Start]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_JobRun_Start];
GO
CREATE PROCEDURE [dwh].[usp_JobRun_Start]
  @ProcessName NVARCHAR(200),
  @JobRunId UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    DECLARE @id UNIQUEIDENTIFIER = NEWID();
    INSERT INTO [dwh].[JobRunLog](JobRunId, ProcessName, HostName, AppName)
    VALUES (@id, @ProcessName, HOST_NAME(), PROGRAM_NAME());
    SET @JobRunId = @id;
  END TRY
  BEGIN CATCH
    IF @JobRunId IS NULL SET @JobRunId = NEWID();
  END CATCH
END
GO

IF OBJECT_ID(N'[dwh].[usp_JobRun_Event]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_JobRun_Event];
GO
CREATE PROCEDURE [dwh].[usp_JobRun_Event]
  @JobRunId UNIQUEIDENTIFIER,
  @EventType NVARCHAR(100),
  @EventDetail NVARCHAR(MAX) = NULL,
  @Metric1Name NVARCHAR(100) = NULL, @Metric1Value SQL_VARIANT = NULL,
  @Metric2Name NVARCHAR(100) = NULL, @Metric2Value SQL_VARIANT = NULL
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    INSERT INTO [dwh].[JobRunLogEvent](JobRunId, EventType, EventDetail, Metric1Name, Metric1Value, Metric2Name, Metric2Value)
    VALUES (@JobRunId, @EventType, @EventDetail, @Metric1Name, @Metric1Value, @Metric2Name, @Metric2Value);
  END TRY
  BEGIN CATCH
  END CATCH
END
GO

IF OBJECT_ID(N'[dwh].[usp_JobRun_End]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_JobRun_End];
GO
CREATE PROCEDURE [dwh].[usp_JobRun_End]
  @JobRunId UNIQUEIDENTIFIER,
  @Status NVARCHAR(30),
  @RowsRead BIGINT = NULL,
  @RowsInserted BIGINT = NULL,
  @RowsUpdated BIGINT = NULL,
  @RowsDeleted BIGINT = NULL,
  @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    UPDATE [dwh].[JobRunLog]
      SET EndTimeUtc = SYSUTCDATETIME(),
          Status = @Status,
          RowsRead = @RowsRead,
          RowsInserted = @RowsInserted,
          RowsUpdated = @RowsUpdated,
          RowsDeleted = @RowsDeleted,
          ErrorMessage = @ErrorMessage
    WHERE JobRunId = @JobRunId;
  END TRY
  BEGIN CATCH
  END CATCH
END
GO

------------------------------------------------------------
-- Config tabellen
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[LoadConfig]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[LoadConfig](
    ProcessName      NVARCHAR(200) NOT NULL CONSTRAINT PK_dwh_LoadConfig PRIMARY KEY,
    Env              NVARCHAR(10)  NOT NULL,
    LoadType         NVARCHAR(20)  NOT NULL,  -- FULL|INCR_DATE
    SourceSchema     SYSNAME       NOT NULL,
    SourceObject     SYSNAME       NOT NULL,
    TargetSchema     SYSNAME       NOT NULL,
    TargetTable      SYSNAME       NOT NULL,
    KeyColumns       NVARCHAR(MAX) NOT NULL,
    UpdateColumns    NVARCHAR(MAX) NULL,
    WatermarkColumn  SYSNAME       NULL,
    BatchSize        INT           NULL,
    RequireUniqueKey BIT           NOT NULL CONSTRAINT DF_dwh_LoadConfig_ReqUK DEFAULT(1),
    Enabled          BIT           NOT NULL CONSTRAINT DF_dwh_LoadConfig_Enabled DEFAULT(1),
    OverlapDays      INT           NOT NULL CONSTRAINT DF_dwh_LoadConfig_OverlapDays DEFAULT(0),
    Comment          NVARCHAR(4000) NULL
  );
END;

IF OBJECT_ID(N'[dwh].[Watermark]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[Watermark](
    ProcessName    NVARCHAR(200) NOT NULL CONSTRAINT PK_dwh_Watermark PRIMARY KEY,
    ValueDateTime  DATETIME2(3)  NULL,
    ValueBigint    BIGINT        NULL,
    ValueString    NVARCHAR(128) NULL,
    ModifiedUtc    DATETIME2(3)  NOT NULL CONSTRAINT DF_dwh_Watermark_Mod DEFAULT SYSUTCDATETIME()
  );
END;
GO

------------------------------------------------------------
-- Watermark procs
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Watermark_Get]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Watermark_Get];
GO
CREATE PROCEDURE [dwh].[usp_Watermark_Get]
  @ProcessName NVARCHAR(200),
  @ValueDateTime DATETIME2(3) OUTPUT,
  @ValueBigint BIGINT OUTPUT,
  @ValueString NVARCHAR(128) OUTPUT
AS
BEGIN
  SET NOCOUNT ON;
  SELECT @ValueDateTime=ValueDateTime, @ValueBigint=ValueBigint, @ValueString=ValueString
  FROM [dwh].[Watermark] WHERE ProcessName=@ProcessName;
END
GO

IF OBJECT_ID(N'[dwh].[usp_Watermark_Set]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Watermark_Set];
GO
CREATE PROCEDURE [dwh].[usp_Watermark_Set]
  @ProcessName NVARCHAR(200),
  @ValueDateTime DATETIME2(3) = NULL,
  @ValueBigint BIGINT = NULL,
  @ValueString NVARCHAR(128) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  MERGE [dwh].[Watermark] AS T
  USING (SELECT @ProcessName AS ProcessName) AS S
  ON (T.ProcessName = S.ProcessName)
  WHEN MATCHED THEN UPDATE SET
    ValueDateTime = COALESCE(@ValueDateTime, T.ValueDateTime),
    ValueBigint   = COALESCE(@ValueBigint,   T.ValueBigint),
    ValueString   = COALESCE(@ValueString,   T.ValueString),
    ModifiedUtc   = SYSUTCDATETIME()
  WHEN NOT MATCHED THEN
    INSERT(ProcessName, ValueDateTime, ValueBigint, ValueString)
    VALUES(@ProcessName, @ValueDateTime, @ValueBigint, @ValueString);
END
GO

------------------------------------------------------------
-- FULL loader (idempotent diff)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Load_GenericUpsert]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Load_GenericUpsert];
GO
CREATE PROCEDURE [dwh].[usp_Load_GenericUpsert]
  @ProcessName    NVARCHAR(200),
  @SourceSchema   SYSNAME,
  @SourceObject   SYSNAME,
  @TargetSchema   SYSNAME,
  @TargetTable    SYSNAME,
  @KeyColumns     NVARCHAR(MAX),
  @UpdateColumns  NVARCHAR(MAX) = NULL
AS
BEGIN
  SET NOCOUNT ON; SET XACT_ABORT ON;

  DECLARE @JobRunId UNIQUEIDENTIFIER;
  EXEC dwh.usp_JobRun_Start @ProcessName=@ProcessName, @JobRunId=@JobRunId OUTPUT;

  BEGIN TRY
    DECLARE @src NVARCHAR(512)=QUOTENAME(@SourceSchema)+N'.'+QUOTENAME(@SourceObject);
    DECLARE @tgt NVARCHAR(512)=QUOTENAME(@TargetSchema)+N'.'+QUOTENAME(@TargetTable);

    DECLARE @keys TABLE(col SYSNAME PRIMARY KEY);
    INSERT INTO @keys(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyColumns, ',');

    DECLARE @upd TABLE(col SYSNAME PRIMARY KEY);
    IF @UpdateColumns IS NOT NULL AND LEN(@UpdateColumns)>0
      INSERT INTO @upd(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@UpdateColumns, ',');

    DECLARE @on NVARCHAR(MAX);
    SELECT @on=STUFF((SELECT ' AND T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col) FROM @keys FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,5,'');

    DECLARE @set NVARCHAR(MAX)=NULL;
    IF EXISTS(SELECT 1 FROM @upd)
      SELECT @set=STUFF((SELECT ', T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col) FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');

    DECLARE @ins TABLE(col SYSNAME PRIMARY KEY);
    INSERT INTO @ins(col) SELECT col FROM @keys;
    INSERT INTO @ins(col) SELECT col FROM @upd;

    DECLARE @cols_ins NVARCHAR(MAX), @cols_vals NVARCHAR(MAX);
    SELECT @cols_ins=STUFF((SELECT ','+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,''),
           @cols_vals=STUFF((SELECT ',S.'+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,'');

    DECLARE @diff NVARCHAR(MAX)=NULL;
    IF EXISTS(SELECT 1 FROM @upd)
      SELECT @diff=STUFF((
        SELECT ' OR ((T.'+QUOTENAME(col)+' <> S.'+QUOTENAME(col)+') OR (T.'+QUOTENAME(col)+' IS NULL AND S.'+QUOTENAME(col)+' IS NOT NULL) OR (T.'+QUOTENAME(col)+' IS NOT NULL AND S.'+QUOTENAME(col)+' IS NULL))'
        FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,4,'');

    DECLARE @sql NVARCHAR(MAX)=N'
      DECLARE @chg TABLE ([Act] nvarchar(10) NOT NULL);
      MERGE '+@tgt+N' AS T
      USING (SELECT * FROM '+@src+N') AS S
        ON '+@on+N'
      '+CASE WHEN @set IS NOT NULL AND @diff IS NOT NULL THEN N'WHEN MATCHED AND ('+@diff+N') THEN UPDATE SET '+@set ELSE N'' END+N'
      WHEN NOT MATCHED THEN INSERT('+@cols_ins+N') VALUES('+@cols_vals+N')
      OUTPUT $action INTO @chg([Act]);

      SELECT
        SUM(CASE WHEN UPPER(a.[Act])=''INSERT'' THEN 1 ELSE 0 END) AS RowsInserted,
        SUM(CASE WHEN UPPER(a.[Act])=''UPDATE'' THEN 1 ELSE 0 END) AS RowsUpdated,
        (SELECT COUNT_BIG(*) FROM '+@src+N') AS RowsRead
      FROM @chg AS a;';

    DECLARE @t TABLE(RowsInserted BIGINT, RowsUpdated BIGINT, RowsRead BIGINT);
    INSERT INTO @t EXEC sp_executesql @sql;

    DECLARE @ri BIGINT,@ru BIGINT,@rr BIGINT;
    SELECT @ri=RowsInserted,@ru=RowsUpdated,@rr=RowsRead FROM @t;

    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Succeeded',
      @RowsRead=@rr, @RowsInserted=@ri, @RowsUpdated=@ru, @RowsDeleted=0, @ErrorMessage=NULL;
  END TRY
  BEGIN CATCH
    IF XACT_STATE()<>0 ROLLBACK;
    DECLARE @errmsg NVARCHAR(MAX)=CONCAT(ERROR_NUMBER(),N'|',ERROR_SEVERITY(),N'|',ERROR_STATE(),N'|',ERROR_MESSAGE());
    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed',
      @RowsRead=NULL, @RowsInserted=NULL, @RowsUpdated=NULL, @RowsDeleted=NULL, @ErrorMessage=@errmsg;
    THROW;
  END CATCH
END
GO

------------------------------------------------------------
-- INCR_DATE loader (WM in INSERT, overlap, #F filter, diff-updates)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Load_GenericIncrDate]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Load_GenericIncrDate];
GO
CREATE PROCEDURE [dwh].[usp_Load_GenericIncrDate]
  @ProcessName NVARCHAR(200)
AS
BEGIN
  SET NOCOUNT ON; SET XACT_ABORT ON;

  DECLARE @JobRunId UNIQUEIDENTIFIER;
  EXEC dwh.usp_JobRun_Start @ProcessName=@ProcessName, @JobRunId=@JobRunId OUTPUT;

  BEGIN TRY
    DECLARE @SourceSchema SYSNAME,@SourceObject SYSNAME,@TargetSchema SYSNAME,@TargetTable SYSNAME,
            @KeyColumns NVARCHAR(MAX),@UpdateColumns NVARCHAR(MAX),
            @WatermarkColumn SYSNAME,@OverlapDays INT;

    SELECT @SourceSchema=SourceSchema,@SourceObject=SourceObject,@TargetSchema=TargetSchema,@TargetTable=TargetTable,
           @KeyColumns=KeyColumns,@UpdateColumns=UpdateColumns,@WatermarkColumn=WatermarkColumn,@OverlapDays=OverlapDays
    FROM dwh.LoadConfig WHERE ProcessName=@ProcessName;

    IF @SourceSchema IS NULL
    BEGIN
      EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed', @ErrorMessage=N'ProcessName niet gevonden in LoadConfig';
      RETURN;
    END

    SET @OverlapDays = ISNULL(@OverlapDays,0);

    DECLARE @wm DATETIME2(3),@vb BIGINT,@vs NVARCHAR(128);
    EXEC dwh.usp_Watermark_Get @ProcessName,@wm OUTPUT,@vb OUTPUT,@vs OUTPUT;
    IF @wm IS NULL SET @wm='1900-01-01T00:00:00.000';

    DECLARE @src NVARCHAR(512)=QUOTENAME(@SourceSchema)+N'.'+QUOTENAME(@SourceObject);
    DECLARE @tgt NVARCHAR(512)=QUOTENAME(@TargetSchema)+N'.'+QUOTENAME(@TargetTable);

    DECLARE @keys TABLE(col SYSNAME PRIMARY KEY);
    INSERT INTO @keys(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyColumns, ',');

    DECLARE @upd TABLE(col SYSNAME PRIMARY KEY);
    IF @UpdateColumns IS NOT NULL AND LEN(@UpdateColumns)>0
      INSERT INTO @upd(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@UpdateColumns, ',');

    DECLARE @ins TABLE(col SYSNAME PRIMARY KEY);
    INSERT INTO @ins(col) SELECT col FROM @keys;
    INSERT INTO @ins(col) SELECT u.col FROM @upd AS u WHERE NOT EXISTS(SELECT 1 FROM @ins i WHERE i.col=u.col);
    IF @WatermarkColumn IS NOT NULL
      INSERT INTO @ins(col) SELECT @WatermarkColumn WHERE NOT EXISTS(SELECT 1 FROM @ins i WHERE i.col=@WatermarkColumn);

    DECLARE @on NVARCHAR(MAX);
    SELECT @on=STUFF((SELECT ' AND T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col) FROM @keys FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,5,'');

    DECLARE @set NVARCHAR(MAX)=NULL;
    IF EXISTS(SELECT 1 FROM @upd)
      SELECT @set=STUFF((SELECT ', T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col) FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');

    DECLARE @diff NVARCHAR(MAX)=NULL;
    IF EXISTS(SELECT 1 FROM @upd)
      SELECT @diff=STUFF((
        SELECT ' OR ((T.'+QUOTENAME(col)+' <> S.'+QUOTENAME(col)+') OR (T.'+QUOTENAME(col)+' IS NULL AND S.'+QUOTENAME(col)+' IS NOT NULL) OR (T.'+QUOTENAME(col)+' IS NOT NULL AND S.'+QUOTENAME(col)+' IS NULL))'
        FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,4,'');

    DECLARE @cols_ins NVARCHAR(MAX), @cols_vals NVARCHAR(MAX);
    SELECT @cols_ins=STUFF((SELECT ','+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,''),
           @cols_vals=STUFF((SELECT ',S.'+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,'');

    DECLARE @sql NVARCHAR(MAX)=N'
      SELECT * INTO #F FROM '+@src+N'
      WHERE '+QUOTENAME(@WatermarkColumn)+N' > DATEADD(DAY,-@ov,@wm);

      DECLARE @chg TABLE([Act] nvarchar(10) NOT NULL);

      MERGE '+@tgt+N' AS T
      USING #F AS S
        ON '+@on+N'
      '+CASE WHEN @set IS NOT NULL AND @diff IS NOT NULL THEN N'WHEN MATCHED AND ('+@diff+N') THEN UPDATE SET '+@set ELSE N'' END+N'
      WHEN NOT MATCHED THEN INSERT('+@cols_ins+N') VALUES('+@cols_vals+N')
      OUTPUT $action INTO @chg([Act]);

      SELECT
        SUM(CASE WHEN UPPER(a.[Act])=''INSERT'' THEN 1 ELSE 0 END) AS RowsInserted,
        SUM(CASE WHEN UPPER(a.[Act])=''UPDATE'' THEN 1 ELSE 0 END) AS RowsUpdated,
        (SELECT COUNT_BIG(*) FROM #F) AS RowsRead,
        (SELECT MAX('+QUOTENAME(@WatermarkColumn)+N') FROM #F) AS NewWM
      FROM @chg AS a;';

    DECLARE @t TABLE(RowsInserted BIGINT, RowsUpdated BIGINT, RowsRead BIGINT, NewWM DATETIME2(3));
    INSERT INTO @t EXEC sp_executesql @sql, N'@wm datetime2, @ov int', @wm=@wm, @ov=@OverlapDays;

    DECLARE @ri BIGINT,@ru BIGINT,@rr BIGINT,@newwm DATETIME2(3);
    SELECT @ri=RowsInserted,@ru=RowsUpdated,@rr=RowsRead,@newwm=NewWM FROM @t;

    IF @newwm IS NOT NULL
      EXEC dwh.usp_Watermark_Set @ProcessName=@ProcessName, @ValueDateTime=@newwm;

    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Succeeded',
      @RowsRead=@rr, @RowsInserted=@ri, @RowsUpdated=@ru, @RowsDeleted=0, @ErrorMessage=NULL;
  END TRY
  BEGIN CATCH
    IF XACT_STATE()<>0 ROLLBACK;
    DECLARE @errmsg NVARCHAR(MAX)=CONCAT(ERROR_NUMBER(),N'|',ERROR_SEVERITY(),N'|',ERROR_STATE(),N'|',ERROR_MESSAGE());
    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed',
      @RowsRead=NULL, @RowsInserted=NULL, @RowsUpdated=NULL, @RowsDeleted=NULL, @ErrorMessage=@errmsg;
    THROW;
  END CATCH
END
GO

------------------------------------------------------------
-- Dispatcher
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Dispatch_Load]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Dispatch_Load];
GO
CREATE PROCEDURE [dwh].[usp_Dispatch_Load]
  @Env nvarchar(10) = N'TST'
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @ProcessName nvarchar(200), @LoadType nvarchar(20),
          @SourceSchema sysname, @SourceObject sysname,
          @TargetSchema sysname, @TargetTable sysname,
          @KeyColumns nvarchar(max), @UpdateColumns nvarchar(max);

  DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT ProcessName, LoadType, SourceSchema, SourceObject, TargetSchema, TargetTable, KeyColumns, UpdateColumns
    FROM [dwh].[LoadConfig]
    WHERE Enabled=1 AND Env=@Env
    ORDER BY ProcessName;

  OPEN c;
  FETCH NEXT FROM c INTO @ProcessName,@LoadType,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable,@KeyColumns,@UpdateColumns;
  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      IF @LoadType = N'FULL'
        EXEC [dwh].[usp_Load_GenericUpsert]
             @ProcessName=@ProcessName,
             @SourceSchema=@SourceSchema,@SourceObject=@SourceObject,
             @TargetSchema=@TargetSchema,@TargetTable=@TargetTable,
             @KeyColumns=@KeyColumns,@UpdateColumns=@UpdateColumns;
      ELSE IF @LoadType = N'INCR_DATE'
        EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=@ProcessName;
    END TRY
    BEGIN CATCH
      PRINT CONCAT(N'Fout in proces ', @ProcessName, N': ', ERROR_MESSAGE());
    END CATCH;
    FETCH NEXT FROM c INTO @ProcessName,@LoadType,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable,@KeyColumns,@UpdateColumns;
  END
  CLOSE c; DEALLOCATE c;
END
GO

------------------------------------------------------------
-- Governance & metrics
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_Governance_Checks]', N'P') IS NOT NULL DROP PROCEDURE [dwh].[usp_Governance_Checks];
GO
CREATE PROCEDURE [dwh].[usp_Governance_Checks]
AS
BEGIN
  SET NOCOUNT ON;
  SELECT OBJECT_SCHEMA_NAME(m.object_id) AS schema_name,
         OBJECT_NAME(m.object_id) AS object_name,
         issue = CASE
           WHEN m.definition LIKE '%@@ROWCOUNT%'   THEN 'Forbidden: @@ROWCOUNT'
           WHEN m.definition LIKE '%SET ROWCOUNT%' THEN 'Forbidden: SET ROWCOUNT'
           WHEN m.definition LIKE '% USE %'        THEN 'Forbidden: USE in module'
           ELSE 'OK'
         END
  FROM sys.sql_modules AS m
  JOIN sys.objects o ON o.object_id=m.object_id AND o.type IN ('P','V','FN','TF','IF')
  WHERE m.definition LIKE '%@@ROWCOUNT%'
     OR m.definition LIKE '%SET ROWCOUNT%'
     OR m.definition LIKE '% USE %'
  ORDER BY 1,2;
END
GO

IF OBJECT_ID(N'[dwh].[vJobRun_Metrics]', N'V') IS NOT NULL DROP VIEW [dwh].[vJobRun_Metrics];
GO
CREATE VIEW [dwh].[vJobRun_Metrics]
AS
SELECT CONVERT(date, StartTimeUtc) AS RunDate,
       ProcessName,
       COUNT_BIG(*) AS Runs,
       SUM(COALESCE(RowsRead,0))     AS RowsRead,
       SUM(COALESCE(RowsInserted,0)) AS RowsInserted,
       SUM(COALESCE(RowsUpdated,0))  AS RowsUpdated,
       SUM(COALESCE(RowsDeleted,0))  AS RowsDeleted
FROM [dwh].[JobRunLog]
GROUP BY CONVERT(date, StartTimeUtc), ProcessName;
GO
