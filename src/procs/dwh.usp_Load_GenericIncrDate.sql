IF OBJECT_ID(N'[dwh].[usp_Load_GenericIncrDate]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Load_GenericIncrDate];
GO
CREATE PROCEDURE [dwh].[usp_Load_GenericIncrDate]
  @ProcessName NVARCHAR(200)
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @JobRunId UNIQUEIDENTIFIER;
  EXEC dwh.usp_JobRun_Start @ProcessName=@ProcessName, @JobRunId=@JobRunId OUTPUT;

  BEGIN TRY
    DECLARE @SourceSchema SYSNAME, @SourceObject SYSNAME, @TargetSchema SYSNAME, @TargetTable SYSNAME,
            @KeyColumns NVARCHAR(MAX), @UpdateColumns NVARCHAR(MAX), @WatermarkColumn SYSNAME;

    SELECT @SourceSchema=SourceSchema, @SourceObject=SourceObject, @TargetSchema=TargetSchema, @TargetTable=TargetTable,
           @KeyColumns=KeyColumns, @UpdateColumns=UpdateColumns, @WatermarkColumn=WatermarkColumn
    FROM dwh.LoadConfig
    WHERE ProcessName=@ProcessName;

    IF @SourceSchema IS NULL
    BEGIN
      EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed', @ErrorMessage=N'ProcessName niet gevonden in LoadConfig';
      RETURN;
    END

    DECLARE @wm DATETIME2(3), @vb BIGINT, @vs NVARCHAR(128);
    EXEC dwh.usp_Watermark_Get @ProcessName, @wm OUTPUT, @vb OUTPUT, @vs OUTPUT;
    IF @wm IS NULL SET @wm = '1900-01-01T00:00:00.000';

    DECLARE @src NVARCHAR(512)=QUOTENAME(@SourceSchema)+N'.'+QUOTENAME(@SourceObject);
    DECLARE @tgt NVARCHAR(512)=QUOTENAME(@TargetSchema)+N'.'+QUOTENAME(@TargetTable);

    -- kolomsets
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

    -- ON, SET, DIFF
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

    -- dynamisch: materialiseer filter in #F
    DECLARE @sql NVARCHAR(MAX)=N'
      SELECT * INTO #F FROM '+@src+N' WHERE '+QUOTENAME(@WatermarkColumn)+N' > @wm;

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
    INSERT INTO @t EXEC sp_executesql @sql, N'@wm datetime2', @wm=@wm;

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
