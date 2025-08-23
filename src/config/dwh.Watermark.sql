/* src/sql/config/dwh.Watermark.sql */
IF OBJECT_ID(N'[dwh].[Watermark]', N'U') IS NULL
BEGIN
  CREATE TABLE [dwh].[Watermark](
    ProcessName    nvarchar(200) NOT NULL CONSTRAINT PK_dwh_Watermark PRIMARY KEY,
    ValueDateTime  datetime2(3)  NULL,
    ValueBigint    bigint        NULL,
    ValueString    nvarchar(128) NULL,
    ModifiedUtc    datetime2(3)  NOT NULL CONSTRAINT DF_dwh_Watermark_Mod DEFAULT SYSUTCDATETIME()
  );
END;
