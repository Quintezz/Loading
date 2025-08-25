/* [dwh].[usp_Load_GenericFullReplace]
   Doel: harde verversing (REPLACE) van target met volledige broninhoud.
   Gedrag: TRUNCATE + INSERT (snel). Fallback naar DELETE + INSERT als TRUNCATE faalt (FK's).
   Metrics: RowsRead = broncount; RowsInserted = broncount; RowsDeleted = targetcount (bij TRUNCATE/DELETE), RowsUpdated = 0.
   Regels: geen @@ROWCOUNT; 2-delige namen; geen GO in proc-body.
*/
IF OBJECT_ID(N'[dwh].[usp_Load_GenericFullReplace]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Load_GenericFullReplace];
GO
CREATE PROCEDURE [dwh].[usp_Load_GenericFullReplace]
  @ProcessName   NVARCHAR(200),
  @SourceSchema  SYSNAME,
  @SourceObject  SYSNAME,
  @TargetSchema  SYSNAME,
  @TargetTable   SYSNAME
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @JobRunId UNIQUEIDENTIFIER;
  EXEC [dwh].[usp_JobRun_Start] @ProcessName=@ProcessName, @JobRunId=@JobRunId OUTPUT;

  BEGIN TRY
    DECLARE @src NVARCHAR(512) = QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceObject);
    DECLARE @tgt NVARCHAR(512) = QUOTENAME(@TargetSchema) + N'.' + QUOTENAME(@TargetTable);

    -- Tellers vooraf
    DECLARE @RowsRead BIGINT = 0, @RowsDeleted BIGINT = 0, @RowsInserted BIGINT = 0;

    DECLARE @sqlCountSrc NVARCHAR(MAX) = N'SELECT @rc = COUNT_BIG(*) FROM ' + @src + N';';
    EXEC sp_executesql @sqlCountSrc, N'@rc BIGINT OUTPUT', @rc=@RowsRead OUTPUT;

    DECLARE @sqlCountTgt NVARCHAR(MAX) = N'SELECT @rc = COUNT_BIG(*) FROM ' + @tgt + N';';
    EXEC sp_executesql @sqlCountTgt, N'@rc BIGINT OUTPUT', @rc=@RowsDeleted OUTPUT;

    -- Probeer TRUNCATE; bij fout: DELETE
    DECLARE @didTruncate BIT = 0;
    BEGIN TRY
      DECLARE @sqlTrunc NVARCHAR(MAX) = N'TRUNCATE TABLE ' + @tgt + N';';
      EXEC sp_executesql @sqlTrunc;
      SET @didTruncate = 1;
    END TRY
    BEGIN CATCH
      -- TRUNCATE faalde (waarschijnlijk FK's); val terug op DELETE
      DECLARE @sqlDel NVARCHAR(MAX) = N'DELETE FROM ' + @tgt + N';';
      EXEC sp_executesql @sqlDel;
    END CATCH;

    -- Insert alle bronrijen
    DECLARE @sqlIns NVARCHAR(MAX) = N'INSERT INTO ' + @tgt + N' SELECT * FROM ' + @src + N';';
    EXEC sp_executesql @sqlIns;
    -- RowsInserted = broncount (we inserten alles)
    SET @RowsInserted = @RowsRead;

    EXEC [dwh].[usp_JobRun_End]
      @JobRunId=@JobRunId, @Status=N'Succeeded',
      @RowsRead=@RowsRead, @RowsInserted=@RowsInserted, @RowsUpdated=0, @RowsDeleted=@RowsDeleted, @ErrorMessage=NULL;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    DECLARE @errmsg NVARCHAR(MAX) = CONCAT(ERROR_NUMBER(),N'|',ERROR_SEVERITY(),N'|',ERROR_STATE(),N'|',ERROR_MESSAGE());
    EXEC [dwh].[usp_JobRun_End]
      @JobRunId=@JobRunId, @Status=N'Failed',
      @RowsRead=NULL, @RowsInserted=NULL, @RowsUpdated=NULL, @RowsDeleted=NULL, @ErrorMessage=@errmsg;
    THROW;
  END CATCH
END
GO
