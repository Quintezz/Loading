-- Controleer ontbrekende target-UX
SELECT lc.ProcessName, lc.TargetSchema, lc.TargetTable, lc.KeyColumns
FROM dwh.LoadConfig lc
OUTER APPLY (
SELECT 1 AS has_ux
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID(QUOTENAME(lc.TargetSchema)+'.'+QUOTENAME(lc.TargetTable))
AND i.is_unique = 1
) ux
WHERE lc.Enabled=1 AND lc.Env='TST' AND ux.has_ux IS NULL;


-- Controleer WM-index op bron-tabel
SELECT lc.ProcessName, lc.SourceSchema, lc.SourceObject, lc.WatermarkColumn
FROM dwh.LoadConfig lc
OUTER APPLY (
SELECT 1 AS has_wm
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID(QUOTENAME(lc.SourceSchema)+'.'+QUOTENAME(lc.SourceObject))
AND EXISTS (
SELECT 1 FROM sys.index_columns ic
WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
AND ic.column_id = (SELECT column_id FROM sys.columns WHERE object_id = i.object_id AND name = lc.WatermarkColumn)
)
) wm
WHERE lc.Enabled=1 AND lc.Env='TST' AND wm.has_wm IS NULL;