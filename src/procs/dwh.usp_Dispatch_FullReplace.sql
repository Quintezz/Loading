/* [dwh].[usp_Dispatch_FullReplace]
   Doel: voer ALLE processen met LoadType='FULL_REPLACE' uit voor een Env (optioneel 1 proces).
   Regels: 2-delige namen, geen @@ROWCOUNT, geen GO binnen proc-body.
*/
IF OBJECT_ID(N'[dwh].[usp_Dispatch_FullReplace]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Dispatch_FullReplace];
GO
CREATE PROCEDURE [dwh].[usp_Dispatch_FullReplace]
  @Env          NVARCHAR(10)   = N'TST',
  @ProcessName  NVARCHAR(200)  = NULL,   -- optioneel: voer alleen dit proces uit
  @StopOnError  BIT            = 0       -- 1 = stop bij fout; 0 = ga door
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @p NVARCHAR(200),
          @SourceSchema  SYSNAME,
          @SourceObject  SYSNAME,
          @TargetSchema  SYSNAME,
          @TargetTable   SYSNAME;

  DECLARE c CURSOR LOCAL FAST_FORWARD FOR
    SELECT ProcessName, SourceSchema, SourceObject, TargetSchema, TargetTable
    FROM [dwh].[LoadConfig]
    WHERE Enabled=1
      AND Env=@Env
      AND LoadType=N'FULL_REPLACE'
      AND (@ProcessName IS NULL OR ProcessName=@ProcessName)
    ORDER BY ProcessName;

  OPEN c;
  FETCH NEXT FROM c INTO @p,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable;

  WHILE @@FETCH_STATUS = 0
  BEGIN
    BEGIN TRY
      EXEC [dwh].[usp_Load_GenericFullReplace]
           @ProcessName=@p,
           @SourceSchema=@SourceSchema,
           @SourceObject=@SourceObject,
           @TargetSchema=@TargetSchema,
           @TargetTable=@TargetTable;
    END TRY
    BEGIN CATCH
      PRINT CONCAT(N'FULL_REPLACE fout in ', @p, N': ', ERROR_MESSAGE());
      IF @StopOnError = 1
        THROW;
      -- anders doorgaan met volgende
    END CATCH;

    FETCH NEXT FROM c INTO @p,@SourceSchema,@SourceObject,@TargetSchema,@TargetTable;
  END

  CLOSE c; DEALLOCATE c;
END
GO

-- Gebruik:
-- EXEC dwh.usp_Dispatch_FullReplace @Env=N'TST';
-- EXEC dwh.usp_Dispatch_FullReplace @Env=N'TST', @ProcessName=N'silver.DimCarrier_dwh.DimCarrier_TST';
