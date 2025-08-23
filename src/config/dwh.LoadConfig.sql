/* src/sql/config/dwh.LoadConfig.sql */
IF OBJECT_ID(N'[dwh].[LoadConfig]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[LoadConfig](
    ProcessName        nvarchar(200) NOT NULL CONSTRAINT PK_dwh_LoadConfig PRIMARY KEY,
    Env                nvarchar(10)  NOT NULL,            -- TST|PROD
    LoadType           nvarchar(20)  NOT NULL,            -- FULL|INCR_DATE
    SourceSchema       sysname       NOT NULL,
    SourceObject       sysname       NOT NULL,            -- view of tabel
    TargetSchema       sysname       NOT NULL,
    TargetTable        sysname       NOT NULL,
    KeyColumns         nvarchar(max) NOT NULL,            -- CSV
    UpdateColumns      nvarchar(max) NULL,                -- CSV
    WatermarkColumn    sysname       NULL,                -- bij INCR_DATE
    BatchSize          int           NULL,
    RequireUniqueKey   bit           NOT NULL CONSTRAINT DF_dwh_LoadConfig_ReqUK DEFAULT(1),
    Enabled            bit           NOT NULL CONSTRAINT DF_dwh_LoadConfig_Enabled DEFAULT(1),
    Comment            nvarchar(4000) NULL
  );
END;
