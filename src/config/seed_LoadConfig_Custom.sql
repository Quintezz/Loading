/* src/sql/config/seed_LoadConfig_Custom.sql */

MERGE [dwh].[LoadConfig] AS T
USING (VALUES
  -- FULL loader DimEmployee
  (N'silver.DimEmployee__dwh.DimEmployee',  N'TST', N'FULL',
   N'silver', N'DimEmployee',
   N'dwh',    N'DimEmployee',
   N'EmplBK', N'Name',
   NULL, NULL, 1, 1,
   N'FULL push DimEmployee'),

  -- INCR_DATE loader FactPayables
  (N'silver.FactPayables_INCR__dwh.FactPayables_TST', N'TST', N'INCR_DATE',
   N'silver', N'FactPayables',
   N'dwh',    N'FactPayables',
   N'ClosedDate,Voucher,VendorBK', N'Amount',
   N'ClosedDate', 100000, 1, 1,
   N'INCR by ClosedDate deze kolommen')
) AS S(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,
       KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
ON (T.ProcessName = S.ProcessName)
WHEN MATCHED THEN
  UPDATE SET Env=S.Env, LoadType=S.LoadType, SourceSchema=S.SourceSchema, SourceObject=S.SourceObject,
             TargetSchema=S.TargetSchema, TargetTable=S.TargetTable,
             KeyColumns=S.KeyColumns, UpdateColumns=S.UpdateColumns,
             WatermarkColumn=S.WatermarkColumn, BatchSize=S.BatchSize,
             RequireUniqueKey=S.RequireUniqueKey, Enabled=S.Enabled, Comment=S.Comment
WHEN NOT MATCHED THEN
  INSERT(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,
         KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
  VALUES(S.ProcessName,S.Env,S.LoadType,S.SourceSchema,S.SourceObject,S.TargetSchema,S.TargetTable,
         S.KeyColumns,S.UpdateColumns,S.WatermarkColumn,S.BatchSize,S.RequireUniqueKey,S.Enabled,S.Comment);
