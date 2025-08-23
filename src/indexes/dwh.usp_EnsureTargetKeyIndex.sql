-- Ensure UNIQUE index op target KeyColumns (per ProcessName)
------------------------------------------------------------
IF OBJECT_ID(N'[dwh].[usp_EnsureTargetKeyIndex]', N'P') IS NOT NULL
DROP PROCEDURE [dwh].[usp_EnsureTargetKeyIndex];
GO
CREATE PROCEDURE [dwh].[usp_EnsureTargetKeyIndex]
@ProcessName nvarchar(200)
AS
BEGIN
SET NOCOUNT ON;
DECLARE @TargetSchema sysname, @TargetTable sysname, @KeyColumns nvarchar(max);
SELECT @TargetSchema=TargetSchema, @TargetTable=TargetTable, @KeyColumns=KeyColumns
FROM [dwh].[LoadConfig]
WHERE ProcessName=@ProcessName;


IF @TargetSchema IS NULL OR @TargetTable IS NULL OR @KeyColumns IS NULL RETURN;


DECLARE @obj nvarchar(512) = QUOTENAME(@TargetSchema)+N'.'+QUOTENAME(@TargetTable);
DECLARE @indexName sysname = LEFT(N'UX_'+@TargetTable+N'_BK', 128);


IF EXISTS (
SELECT 1 FROM sys.indexes
WHERE object_id = OBJECT_ID(@obj) AND name = @indexName
) RETURN;


DECLARE @cols nvarchar(max) = STUFF((
SELECT ',' + QUOTENAME(LTRIM(RTRIM(value))) + ' ASC'
FROM STRING_SPLIT(@KeyColumns, ',')
FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'');


IF @cols IS NULL OR LEN(@cols)=0 RETURN;


DECLARE @sql nvarchar(max) = N'CREATE UNIQUE INDEX ' + QUOTENAME(@indexName) + N' ON ' + @obj + N' (' + @cols + N');';
EXEC sp_executesql @sql;
END
GO