/* src/sql/procs/dwh.usp_Watermark_Set.sql */
IF OBJECT_ID(N'[dwh].[usp_Watermark_Set]', N'P') IS NULL
  EXEC('CREATE PROCEDURE [dwh].[usp_Watermark_Set] AS RETURN 0;');
ALTER PROCEDURE [dwh].[usp_Watermark_Set]
  @ProcessName nvarchar(200),
  @ValueDateTime datetime2(3) = NULL,
  @ValueBigint bigint = NULL,
  @ValueString nvarchar(128) = NULL
AS
BEGIN
  SET NOCOUNT ON;
  MERGE [dwh].[Watermark] AS T
  USING (SELECT @ProcessName AS ProcessName) AS S
  ON (T.ProcessName = S.ProcessName)
  WHEN MATCHED THEN UPDATE SET
    ValueDateTime = COALESCE(@ValueDateTime, T.ValueDateTime),
    ValueBigint = COALESCE(@ValueBigint, T.ValueBigint),
    ValueString = COALESCE(@ValueString, T.ValueString),
    ModifiedUtc = SYSUTCDATETIME()
  WHEN NOT MATCHED THEN
    INSERT(ProcessName, ValueDateTime, ValueBigint, ValueString) VALUES(@ProcessName, @ValueDateTime, @ValueBigint, @ValueString);
END;
