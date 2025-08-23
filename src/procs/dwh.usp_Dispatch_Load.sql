/* Dispatcher: voert alle ingeschakelde processen uit voor een Env.
   Regels: 2-delig, geen GO in proc-body, geen @@ROWCOUNT.
*/
IF OBJECT_ID(N'[dwh].[usp_Dispatch_Load]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Dispatch_Load];
GO
CREATE PROCEDURE [dwh].[usp_Dispatch_Load]
  @Env nvarchar(10) = N'TST'
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @ProcessName nvarchar(200),
          @LoadType    nvarchar(20),
          @SourceSchema sysname,
          @SourceObject sysname,
          @TargetSchema sysname,
          @TargetTable  sysname,
          @KeyColumns   nvarchar(max),
          @UpdateColumns nvarchar(max);

  DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT ProcessName, LoadType, SourceSchema, SourceObject, TargetSchema, TargetTable, KeyColumns, UpdateColumns
    FROM [dwh].[LoadConfig]
    WHERE Enabled = 1 AND Env = @Env
    ORDER BY ProcessName;

  OPEN c;
  FETCH NEXT FROM c INTO @ProcessName,@LoadType,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable,@KeyColumns,@UpdateColumns;

  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      IF @LoadType = N'FULL'
        EXEC [dwh].[usp_Load_GenericUpsert]
             @ProcessName=@ProcessName,
             @SourceSchema=@SourceSchema,
             @SourceObject=@SourceObject,
             @TargetSchema=@TargetSchema,
             @TargetTable=@TargetTable,
             @KeyColumns=@KeyColumns,
             @UpdateColumns=@UpdateColumns;
      ELSE IF @LoadType = N'INCR_DATE'
        EXEC [dwh].[usp_Load_GenericIncrDate] @ProcessName=@ProcessName;
      ELSE
        PRINT CONCAT(N'Onbekende LoadType voor ', @ProcessName, N': ', @LoadType);
    END TRY
    BEGIN CATCH
      -- Loader logt zelf; ga door met volgende
      PRINT CONCAT(N'Fout in proces ', @ProcessName, N': ', ERROR_MESSAGE());
    END CATCH;

    FETCH NEXT FROM c INTO @ProcessName,@LoadType,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable,@KeyColumns,@UpdateColumns;
  END

  CLOSE c; DEALLOCATE c;
END
GO
