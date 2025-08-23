------------------------------------------------------------
-- Ensure WM-index op bron (indien bron een tabel is)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_EnsureSourceWMIndex]', N'P') IS NOT NULL
DROP PROCEDURE [dwh].[usp_EnsureSourceWMIndex];
GO
CREATE PROCEDURE [dwh].[usp_EnsureSourceWMIndex]
@ProcessName nvarchar(200)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @SourceSchema sysname, @SourceObject sysname, @WatermarkColumn sysname;
SELECT @SourceSchema=SourceSchema, @SourceObject=SourceObject, @WatermarkColumn=WatermarkColumn
FROM [dwh].[LoadConfig]
WHERE ProcessName=@ProcessName;


IF @SourceSchema IS NULL OR @SourceObject IS NULL OR @WatermarkColumn IS NULL RETURN;


DECLARE @obj nvarchar(512) = QUOTENAME(@SourceSchema)+N'.'+QUOTENAME(@SourceObject);
-- Alleen tabellen indexeren
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(@obj) AND type = 'U') RETURN;


DECLARE @ixName sysname = LEFT(N'IX_'+@SourceObject+N'_'+@WatermarkColumn, 128);
IF EXISTS (
SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(@obj) AND name = @ixName
) RETURN;


DECLARE @sql nvarchar(max) = N'CREATE NONCLUSTERED INDEX ' + QUOTENAME(@ixName) + N' ON ' + @obj + N' (' + QUOTENAME(@WatermarkColumn) + N' ASC);';
EXEC sp_executesql @sql;
END
GO