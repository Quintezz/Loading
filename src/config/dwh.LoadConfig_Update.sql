------------------------------------------------------------
-- 1) Schema-update: LoadConfig uitbreiden
------------------------------------------------------------
IF COL_LENGTH('dwh.LoadConfig','OverlapDays') IS NULL
BEGIN
  ALTER TABLE [dwh].[LoadConfig]
    ADD [OverlapDays] INT NOT NULL CONSTRAINT DF_dwh_LoadConfig_OverlapDays DEFAULT(0);
END;
GO
