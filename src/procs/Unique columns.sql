DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = STRING_AGG(
    'SELECT ''' + c.name + ''' AS ColumnName
     FROM [silver].[FactInvoiceMarkup]
     HAVING COUNT_BIG(DISTINCT [' + c.name + ']) = COUNT_BIG(*)',
    ' UNION ALL '
)
FROM sys.columns c
JOIN sys.objects o ON c.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.name = 'FactInvoiceMarkup'
  AND s.name = 'silver';

EXEC sp_executesql @sql;


