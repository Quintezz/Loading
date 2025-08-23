IF OBJECT_ID(N'[dwh].[usp_Governance_Checks]', N'P') IS NOT NULL
  DROP PROCEDURE [dwh].[usp_Governance_Checks];
GO
CREATE PROCEDURE [dwh].[usp_Governance_Checks]
AS
BEGIN
  SET NOCOUNT ON;
  SELECT
    schema_name = OBJECT_SCHEMA_NAME(m.object_id),
    object_name = OBJECT_NAME(m.object_id),
    issue = CASE
      WHEN m.definition LIKE '%@@ROWCOUNT%'   THEN 'Forbidden: @@ROWCOUNT'
      WHEN m.definition LIKE '%SET ROWCOUNT%' THEN 'Forbidden: SET ROWCOUNT'
      WHEN m.definition LIKE '% USE %'        THEN 'Forbidden: USE in module'
      ELSE 'OK'
    END
  FROM sys.sql_modules AS m
  JOIN sys.objects o ON o.object_id = m.object_id AND o.type IN ('P','V','FN','TF','IF')
  WHERE (m.definition LIKE '%@@ROWCOUNT%' OR m.definition LIKE '%SET ROWCOUNT%' OR m.definition LIKE '% USE %')
    AND OBJECT_SCHEMA_NAME(m.object_id) <> N'dwh'
    AND OBJECT_NAME(m.object_id) <> N'usp_Governance_Checks'
  ORDER BY schema_name, object_name;
END
GO
