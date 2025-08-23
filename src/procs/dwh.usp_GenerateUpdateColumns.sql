CREATE OR ALTER PROCEDURE [dwh].[usp_GenerateUpdateColumns]
  @Schema SYSNAME,
  @Table SYSNAME,
  @KeyCols NVARCHAR(MAX),
  @UpdateColumns NVARCHAR(MAX) OUTPUT
AS
BEGIN
  SET NOCOUNT ON;

  SELECT @UpdateColumns =
    STRING_AGG(QUOTENAME(c.name), ',')
    WITHIN GROUP (ORDER BY c.column_id)
  FROM sys.columns AS c
  WHERE c.object_id = OBJECT_ID(@Schema + '.' + @Table)
    AND c.name NOT IN (
      SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyCols, ',')
    );
END
