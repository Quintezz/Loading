/* src/sql/procs/dwh.usp_Watermark_Get.sql */
IF OBJECT_ID(N'[dwh].[usp_Watermark_Get]', N'P') IS NULL
  EXEC('CREATE PROCEDURE [dwh].[usp_Watermark_Get] AS RETURN 0;');
ALTER PROCEDURE [dwh].[usp_Watermark_Get]
  @ProcessName nvarchar(200),
  @ValueDateTime datetime2(3) OUTPUT,
  @ValueBigint bigint OUTPUT,
  @ValueString nvarchar(128) OUTPUT
AS
BEGIN
  SET NOCOUNT ON;
  SELECT @ValueDateTime = ValueDateTime, @ValueBigint = ValueBigint, @ValueString = ValueString
  FROM [dwh].[Watermark] WHERE ProcessName = @ProcessName;
END;
