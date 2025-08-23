/* src/sql/admin/logging.tables.sql */
IF OBJECT_ID(N'[dwh].[JobRunLog]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[JobRunLog](
    JobRunId       uniqueidentifier NOT NULL CONSTRAINT PK_dwh_JobRunLog PRIMARY KEY DEFAULT NEWID(),
    ProcessName    nvarchar(200)    NOT NULL,
    StartTimeUtc   datetime2(3)     NOT NULL CONSTRAINT DF_dwh_JobRunLog_Start DEFAULT SYSUTCDATETIME(),
    EndTimeUtc     datetime2(3)     NULL,
    Status         nvarchar(30)     NULL, -- Started|Succeeded|Failed|Partial
    RowsRead       bigint           NULL,
    RowsInserted   bigint           NULL,
    RowsUpdated    bigint           NULL,
    RowsDeleted    bigint           NULL,
    ErrorMessage   nvarchar(max)    NULL,
    HostName       nvarchar(128)    NULL,
    AppName        nvarchar(128)    NULL
  );
END;

IF OBJECT_ID(N'[dwh].[JobRunLogEvent]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[JobRunLogEvent](
    JobRunEventId  bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_dwh_JobRunLogEvent PRIMARY KEY,
    JobRunId       uniqueidentifier NOT NULL,
    EventTimeUtc   datetime2(3)     NOT NULL CONSTRAINT DF_dwh_JobRunLogEvent_Time DEFAULT SYSUTCDATETIME(),
    EventType      nvarchar(100)    NOT NULL,
    EventDetail    nvarchar(max)    NULL,
    Metric1Name    nvarchar(100)    NULL,
    Metric1Value   sql_variant      NULL,
    Metric2Name    nvarchar(100)    NULL,
    Metric2Value   sql_variant      NULL
  );
  CREATE INDEX IX_dwh_JobRunLogEvent_JobRunId ON [dwh].[JobRunLogEvent](JobRunId, EventTimeUtc);
END;
