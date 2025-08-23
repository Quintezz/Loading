/* src/sql/admin/logging.procs.sql */
-- Logging procs. Gebruik CREATE OR ALTER en scheid batches. Geen GO binnen proc-bodies.

CREATE OR ALTER PROCEDURE [dwh].[usp_JobRun_Start]
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

CREATE OR ALTER PROCEDURE [dwh].[usp_JobRun_Event]
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
    -- ignore
  END CATCH
END
GO

CREATE OR ALTER PROCEDURE [dwh].[usp_JobRun_End]
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
    -- ignore
  END CATCH
END
GO
