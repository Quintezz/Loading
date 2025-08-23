IF OBJECT_ID(N'[dwh].[usp_Load_GenericIncrStatus]', N'P') IS NOT NULL
-- kolomsets
DECLARE @keys TABLE(col SYSNAME PRIMARY KEY);
INSERT INTO @keys(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@KeyColumns, ',');


DECLARE @upd TABLE(col SYSNAME PRIMARY KEY);
IF @UpdateColumns IS NOT NULL AND LEN(@UpdateColumns)>0
INSERT INTO @upd(col) SELECT LTRIM(RTRIM(value)) FROM STRING_SPLIT(@UpdateColumns, ',');


DECLARE @ins TABLE(col SYSNAME PRIMARY KEY);
INSERT INTO @ins(col) SELECT col FROM @keys;
INSERT INTO @ins(col) SELECT u.col FROM @upd AS u WHERE NOT EXISTS(SELECT 1 FROM @ins i WHERE i.col=u.col);
IF @WatermarkColumn IS NOT NULL
INSERT INTO @ins(col) SELECT @WatermarkColumn WHERE NOT EXISTS(SELECT 1 FROM @ins i WHERE i.col=@WatermarkColumn);
IF @StatusColumn IS NOT NULL
INSERT INTO @ins(col) SELECT @StatusColumn WHERE NOT EXISTS(SELECT 1 FROM @ins i WHERE i.col=@StatusColumn);


-- ON, SET, DIFF
DECLARE @on NVARCHAR(MAX);
SELECT @on=STUFF((SELECT ' AND T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col)
FROM @keys FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,5,'');


DECLARE @set NVARCHAR(MAX)=NULL;
IF EXISTS(SELECT 1 FROM @upd)
SELECT @set=STUFF((SELECT ', T.'+QUOTENAME(col)+' = S.'+QUOTENAME(col)
FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'');


DECLARE @diff NVARCHAR(MAX)=NULL;
IF EXISTS(SELECT 1 FROM @upd)
SELECT @diff=STUFF((
SELECT ' OR ((T.'+QUOTENAME(col)+' <> S.'+QUOTENAME(col)+') OR (T.'+QUOTENAME(col)+' IS NULL AND S.'+QUOTENAME(col)+' IS NOT NULL) OR (T.'+QUOTENAME(col)+' IS NOT NULL AND S.'+QUOTENAME(col)+' IS NULL))'
FROM @upd FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,4,'');


DECLARE @cols_ins NVARCHAR(MAX), @cols_vals NVARCHAR(MAX);
SELECT @cols_ins=STUFF((SELECT ','+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,''),
@cols_vals=STUFF((SELECT ',S.'+QUOTENAME(col) FROM @ins FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,1,'');


-- Dynamisch: materialiseer #F met statusfilter + optionele watermark; maak InsertFlag op basis van InsertStatusCsv
DECLARE @sql NVARCHAR(MAX)=N'
SELECT S.*, CASE WHEN EXISTS (SELECT 1 FROM STRING_SPLIT(@insCsv,'','') i WHERE LTRIM(RTRIM(i.value)) = CONVERT(nvarchar(4000), S.'+QUOTENAME(@StatusColumn)+N')) THEN 1 ELSE 0 END AS InsertFlag
INTO #F
FROM '+@src+N' AS S
WHERE (
EXISTS (SELECT 1 FROM STRING_SPLIT(@insCsv,'','') i WHERE LTRIM(RTRIM(i.value)) = CONVERT(nvarchar(4000), S.'+QUOTENAME(@StatusColumn)+N'))
OR EXISTS (SELECT 1 FROM STRING_SPLIT(@updCsv,'','') u WHERE LTRIM(RTRIM(u.value)) = CONVERT(nvarchar(4000), S.'+QUOTENAME(@StatusColumn)+N'))
)'+CASE WHEN @WatermarkColumn IS NOT NULL THEN N' AND S.'+QUOTENAME(@WatermarkColumn)+N' > @wm' ELSE N'' END+N';


DECLARE @chg TABLE([Act] nvarchar(10) NOT NULL);


MERGE '+@tgt+N' AS T
USING #F AS S
ON '+@on+N'
'+CASE WHEN @set IS NOT NULL AND @diff IS NOT NULL THEN N'WHEN MATCHED AND ('+@diff+N') THEN UPDATE SET '+@set ELSE N'' END+N'
WHEN NOT MATCHED AND S.InsertFlag = 1 THEN INSERT('+@cols_ins+N') VALUES('+@cols_vals+N')
OUTPUT $action INTO @chg([Act]);


SELECT
SUM(CASE WHEN UPPER(a.[Act])=''INSERT'' THEN 1 ELSE 0 END) AS RowsInserted,
SUM(CASE WHEN UPPER(a.[Act])=''UPDATE'' THEN 1 ELSE 0 END) AS RowsUpdated,
(SELECT COUNT_BIG(*) FROM #F) AS RowsRead,
'+(CASE WHEN @WatermarkColumn IS NOT NULL THEN N'(SELECT MAX('+QUOTENAME(@WatermarkColumn)+N') FROM #F)' ELSE N'NULL' END)+N' AS NewWM;';


DECLARE @t TABLE(RowsInserted BIGINT, RowsUpdated BIGINT, RowsRead BIGINT, NewWM DATETIME2(3));
INSERT INTO @t EXEC sp_executesql @sql, N'@wm datetime2, @insCsv nvarchar(max), @updCsv nvarchar(max)', @wm=@wm, @insCsv=@InsertStatusCsv, @updCsv=@UpdateStatusCsv;


DECLARE @ri BIGINT,@ru BIGINT,@rr BIGINT; DECLARE @newwm DATETIME2(3);
SELECT @ri=RowsInserted,@ru=RowsUpdated,@rr=RowsRead,@newwm=NewWM FROM @t;


IF @newwm IS NOT NULL
EXEC dwh.usp_Watermark_Set @ProcessName=@ProcessName, @ValueDateTime=@newwm;


EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Succeeded', @RowsRead=@rr, @RowsInserted=@ri, @RowsUpdated=@ru, @RowsDeleted=0, @ErrorMessage=NULL;
END TRY
BEGIN CATCH
IF XACT_STATE()<>0 ROLLBACK;
DECLARE @errmsg NVARCHAR(MAX)=CONCAT(ERROR_NUMBER(),N'|',ERROR_SEVERITY(),N'|',ERROR_STATE(),N'|',ERROR_MESSAGE());
EXEC dwh.usp_JobRun_End @JobRunId=@JobRunId, @Status=N'Failed', @RowsRead=NULL, @RowsInserted=NULL, @RowsUpdated=NULL, @RowsDeleted=NULL, @ErrorMessage=@errmsg;
THROW;
END CATCH
END
GO