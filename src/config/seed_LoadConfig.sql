/* src/sql/config/seed_LoadConfig.sql */
MERGE [dwh].[LoadConfig] AS T
USING (VALUES
  (N'silver.DimProductV2__dwh.DimProduct_TST', N'TST', N'FULL',      N'silver', N'DimProductV2',   N'dwh', N'DimProduct', N'ProductBK',                                  N'Name',   NULL,       NULL, 1, 1, N'FULL push dim product'),
  (N'silver.FactSales_INCR__dwh.FactSales_TST',N'TST', N'INCR_DATE', N'silver', N'FactSales_INCR', N'dwh', N'FactSales',  N'SalesDate,ProductId,CustomerId',            N'Amount', N'SalesDate',100000, 1, 1, N'INCR by SalesDate')
) AS S(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
ON (T.ProcessName = S.ProcessName)
WHEN MATCHED THEN UPDATE SET
  Env=S.Env, LoadType=S.LoadType, SourceSchema=S.SourceSchema, SourceObject=S.SourceObject,
  TargetSchema=S.TargetSchema, TargetTable=S.TargetTable, KeyColumns=S.KeyColumns, UpdateColumns=S.UpdateColumns,
  WatermarkColumn=S.WatermarkColumn, BatchSize=S.BatchSize, RequireUniqueKey=S.RequireUniqueKey, Enabled=S.Enabled, Comment=S.Comment
WHEN NOT MATCHED THEN
  INSERT(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,Comment)
  VALUES(S.ProcessName,S.Env,S.LoadType,S.SourceSchema,S.SourceObject,S.TargetSchema,S.TargetTable,S.KeyColumns,S.UpdateColumns,S.WatermarkColumn,S.BatchSize,S.RequireUniqueKey,S.Enabled,S.Comment);
