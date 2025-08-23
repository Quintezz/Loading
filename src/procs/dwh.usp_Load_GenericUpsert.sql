/* FULL loader idempotency fix — update alleen bij verschil per kolom
   Implementatie: WHEN MATCHED AND <diff> THEN UPDATE SET ...
   DROP+CREATE met GO, geen GO in proc-body. */

IF OBJECT_ID(N'[dwh].[usp_Load_GenericUpsert]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Load_GenericUpsert];
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
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @JobRunId UNIQUEIDENTIFIER;
  EXEC dwh.usp_JobRun_Start @ProcessName=@ProcessName, @JobRunId=@JobRunId OUTPUT;

  BEGIN TRY
    DECLARE @src NVARCHAR(512) = QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceObject);
    DECLARE @tgt NVARCHAR(512) = QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetTable);

    -- Parse kolomlijsten
    DECLARE @keys TABLE(col SYSNAME);
    INSERT INTO @keys(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyColumns, ',');

    DECLARE @upd TABLE(col SYSNAME);
    IF @UpdateColumns IS NOT NULL AND LEN(@UpdateColumns) > 0
      INSERT INTO @upd(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@UpdateColumns, ',');

    -- Merge ON
    DECLARE @on NVARCHAR(MAX);
    SELECT @on = STUFF((SELECT ' AND T.' + QUOTENAME(col) + ' = S.' + QUOTENAME(col)
                        FROM @keys FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,5,'');

    -- SET-lijst voor UPDATE
    DECLARE @set NVARCHAR(MAX) = NULL;
    IF EXISTS(SELECT 1 FROM @upd)
      SELECT @set = STUFF((SELECT ', T.' + QUOTENAME(col) + ' = S.' + QUOTENAME(col)
                           FROM @upd FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'');

    -- INSERT kolommen
    DECLARE @ins TABLE(col SYSNAME);
    INSERT INTO @ins(col) SELECT col FROM @keys;
    INSERT INTO @ins(col) SELECT col FROM @upd;

    DECLARE @cols_ins NVARCHAR(MAX), @cols_vals NVARCHAR(MAX);
    SELECT @cols_ins = STUFF((SELECT ',' + QUOTENAME(col) FROM @ins FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,''),
           @cols_vals = STUFF((SELECT ',S.' + QUOTENAME(col) FROM @ins FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'');

    -- Diff-predicaat: alleen updaten bij verschil (NULL-safe)
    DECLARE @diff NVARCHAR(MAX) = NULL;
    IF EXISTS (SELECT 1 FROM @upd)
      SELECT @diff = STUFF((
        SELECT ' OR ((T.' + QUOTENAME(col) + ' <> S.' + QUOTENAME(col) + ')
                      OR (T.' + QUOTENAME(col) + ' IS NULL AND S.' + QUOTENAME(col) + ' IS NOT NULL)
                      OR (T.' + QUOTENAME(col) + ' IS NOT NULL AND S.' + QUOTENAME(col) + ' IS NULL))'
        FROM @upd FOR XML PATH(''), TYPE).value('.','nvarchar(max)'), 1, 4, '');

    -- Dynamische MERGE
    DECLARE @sql NVARCHAR(MAX) = N'
      DECLARE @chg TABLE ([Act] nvarchar(10) NOT NULL);
      MERGE ' + @tgt + N' AS T
      USING (SELECT * FROM ' + @src + N') AS S
        ON ' + @on + N'
      ' + CASE WHEN @set IS NOT NULL AND @diff IS NOT NULL
                THEN N'WHEN MATCHED AND (' + @diff + N') THEN UPDATE SET ' + @set
                ELSE N'' END + N'
      WHEN NOT MATCHED THEN INSERT (' + @cols_ins + N') VALUES (' + @cols_vals + N')
      OUTPUT $action INTO @chg([Act]);

      SELECT
        SUM(CASE WHEN UPPER(a.[Act]) = ''INSERT'' THEN 1 ELSE 0 END) AS RowsInserted,
        SUM(CASE WHEN UPPER(a.[Act]) = ''UPDATE'' THEN 1 ELSE 0 END) AS RowsUpdated,
        (SELECT COUNT_BIG(*) FROM ' + @src + N') AS RowsRead
      FROM @chg AS a;';

    DECLARE @t TABLE(RowsInserted BIGINT, RowsUpdated BIGINT, RowsRead BIGINT);
    INSERT INTO @t EXEC sp_executesql @sql;

    DECLARE @ri BIGINT, @ru BIGINT, @rr BIGINT;
    SELECT @ri=RowsInserted, @ru=RowsUpdated, @rr=RowsRead FROM @t;

    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Succeeded',
         @RowsRead=@rr, @RowsInserted=@ri, @RowsUpdated=@ru, @RowsDeleted=0, @ErrorMessage=NULL;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    DECLARE @errmsg NVARCHAR(MAX) = CONCAT(ERROR_NUMBER(),N'|',ERROR_SEVERITY(),N'|',ERROR_STATE(),N'|',ERROR_MESSAGE());
    EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed',
         @RowsRead=NULL, @RowsInserted=NULL, @RowsUpdated=NULL, @RowsDeleted=NULL, @ErrorMessage=@errmsg;
    THROW;
  END CATCH
END
GO
