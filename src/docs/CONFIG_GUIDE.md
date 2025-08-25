# CONFIG GUIDE — dwh.LoadConfig & dwh.Watermark

Doel: eenduidige configuratie van laadprocessen (FULL/INCR\_DATE) zonder code‑wijzigingen.

## 1. Tabellen

### 1.1 `dwh.LoadConfig`

| Kolom              | Type             | Verplicht | Beschrijving                                         |
| ------------------ | ---------------- | --------- | ---------------------------------------------------- |
| `ProcessName`      | nvarchar(200) PK | ✓         | Uniek. Patroon: `<SourceView>__<Target>_<ENV>`.      |
| `Env`              | nvarchar(10)     | ✓         | `TST`/`ACC`/`PROD`. Dispatcher filtert hierop.       |
| `LoadType`         | nvarchar(20)     | ✓         | `FULL` of `INCR_DATE`.                               |
| `SourceSchema`     | sysname          | ✓         | Schema van bron (vaak `silver`).                     |
| `SourceObject`     | sysname          | ✓         | View of tabel.                                       |
| `TargetSchema`     | sysname          | ✓         | Doelschema (vaak `dwh`).                             |
| `TargetTable`      | sysname          | ✓         | Doeltabel.                                           |
| `KeyColumns`       | nvarchar(max)    | ✓         | CSV van business keys. Volgorde = indexvolgorde.     |
| `UpdateColumns`    | nvarchar(max)    |           | CSV van te updaten kolommen (leeg = alleen inserts). |
| `WatermarkColumn`  | sysname          | \*        | Alleen bij `INCR_DATE`. Type `datetime2` aanbevolen. |
| `BatchSize`        | int              |           | Niet gebruikt (gereserveerd).                        |
| `RequireUniqueKey` | bit              | ✓         | `1` ⇒ target heeft UX op BK (gefilterd toegestaan).  |
| `Enabled`          | bit              | ✓         | Dispatcher neemt alleen `1` mee.                     |
| `OverlapDays`      | int              | ✓         | Default `0`. Late arrivals venster.                  |
| `Comment`          | nvarchar(4000)   |           | Owner/notes.                                         |

### 1.2 `dwh.Watermark`

| Kolom           | Type             | Beschrijving                                         |
| --------------- | ---------------- | ---------------------------------------------------- |
| `ProcessName`   | nvarchar(200) PK | Foreign key by contract naar LoadConfig.ProcessName. |
| `ValueDateTime` | datetime2(3)     | WM voor INCR\_DATE.                                  |
| `ValueBigint`   | bigint           | Alternatief WM (niet gebruikt).                      |
| `ValueString`   | nvarchar(128)    | Alternatief WM (niet gebruikt).                      |
| `ModifiedUtc`   | datetime2(3)     | Laatste wijziging.                                   |

## 2. Naamconventies

- **ProcessName**: exact schema/object in naam. Voorbeeld: `silver.FactSales_INCR__dwh.FactSales_TST`.
- **Twee‑delige namen** in alle kolommen (`schema.object`). Geen drie‑delige referenties.
- **Sleutelvolgorde** in `KeyColumns` = indexvolgorde (UX).

## 3. Voorbeelden

### 3.1 FULL (Dim)

```sql
MERGE dwh.LoadConfig AS T
USING (VALUES(
 N'silver.DimProductV2__dwh.DimProduct_TST','TST','FULL',
 N'silver','DimProductV2','dwh','DimProduct',
 N'ProductBK', N'Name', NULL, NULL, 1, 1, 0, N'FULL push dim'
)) AS S(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,
        KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,OverlapDays,Comment)
ON (T.ProcessName=S.ProcessName)
WHEN MATCHED THEN UPDATE SET Env=S.Env,LoadType=S.LoadType,SourceSchema=S.SourceSchema,SourceObject=S.SourceObject,
  TargetSchema=S.TargetSchema,TargetTable=S.TargetTable,KeyColumns=S.KeyColumns,UpdateColumns=S.UpdateColumns,
  WatermarkColumn=S.WatermarkColumn,BatchSize=S.BatchSize,RequireUniqueKey=S.RequireUniqueKey,Enabled=S.Enabled,
  OverlapDays=S.OverlapDays,Comment=S.Comment
WHEN NOT MATCHED THEN INSERT(ProcessName,Env,LoadType,SourceSchema,SourceObject,TargetSchema,TargetTable,
  KeyColumns,UpdateColumns,WatermarkColumn,BatchSize,RequireUniqueKey,Enabled,OverlapDays,Comment)
VALUES(S.ProcessName,S.Env,S.LoadType,S.SourceSchema,S.SourceObject,S.TargetSchema,S.TargetTable,
  S.KeyColumns,S.UpdateColumns,S.WatermarkColumn,S.BatchSize,S.RequireUniqueKey,S.Enabled,S.OverlapDays,S.Comment);
```

### 3.2 INCR\_DATE (Fact) met samengestelde sleutel en overlap

```sql
MERGE dwh.LoadConfig AS T
USING (VALUES(
 N'silver.FactInvoiceMarkup_INCR__dwh.FactInvoiceMarkup_TST','TST','INCR_DATE',
 N'silver','FactInvoiceMarkupV2','dwh','FactInvoiceMarkup',
 N'InvoiceNum,DATAAREAID', N'MarkupAmountTCY,MarkupText', N'TRANSDATE',
 NULL,1,1,2,N'Incremental markup'
)) AS S(...)
-- idem als FULL
```

## 4. Validatietools

### 4.1 Bestaan bron/doel

```sql
SELECT ProcessName,
  src = OBJECT_ID(QUOTENAME(SourceSchema)+'.'+QUOTENAME(SourceObject)),
  tgt = OBJECT_ID(QUOTENAME(TargetSchema)+'.'+QUOTENAME(TargetTable))
FROM dwh.LoadConfig WHERE Enabled=1;
```

### 4.2 Kolom‑lint (BK/Update bestaan in target)

```sql
SELECT lc.ProcessName, miss_col = LTRIM(RTRIM(s.value))
FROM dwh.LoadConfig lc
CROSS APPLY STRING_SPLIT(COALESCE(lc.KeyColumns+','+COALESCE(lc.UpdateColumns,''), lc.KeyColumns), ',') s
LEFT JOIN sys.columns c
  ON c.object_id = OBJECT_ID(QUOTENAME(lc.TargetSchema)+'.'+QUOTENAME(lc.TargetTable))
 AND c.name = LTRIM(RTRIM(s.value))
WHERE lc.Enabled=1 AND c.name IS NULL;
```

### 4.3 Watermark sanity

```sql
DECLARE @p nvarchar(200)=N'<ProcessName>'; DECLARE @wm datetime2;
EXEC dwh.usp_Watermark_Get @p, @wm OUTPUT, NULL, NULL;
SELECT @wm AS Watermark,
       rows_after_wm = COUNT_BIG(*),
       min_wm = MIN([WMcol]),
       max_wm = MAX([WMcol])
FROM [silver].[<SourceObject>]
WHERE [WMcol] > DATEADD(DAY,-(SELECT OverlapDays FROM dwh.LoadConfig WHERE ProcessName=@p), @wm);
```

## 5. Richtlijnen & keuzes

### 5.1 FULL vs INCR\_DATE

| Aspect            | FULL        | INCR\_DATE                     |
| ----------------- | ----------- | ------------------------------ |
| Herhaalbaarheid   | Hoog        | Hoog (met WM)                  |
| Doorlooptijd      | Medium/Hoog | Laag/Medium                    |
| Retro‑wijzigingen | OK          | Alleen binnen overlap/backfill |

### 5.2 `KeyColumns`

- Business‑sleutel, niet surrogate key.
- Volgorde = indexvolgorde (UX).
- NULLs toegestaan ⇒ gefilterde UNIQUE index overwegen.

### 5.3 `UpdateColumns`

- Alleen werkelijk te muteren kolommen.
- Beperk set bij grote facts.

### 5.4 `WatermarkColumn`

- `datetime2(3)` aanbevolen.
- Indien string: uniforme stijl (`126` ISO 8601) en conversie in view.

### 5.5 `OverlapDays`

- Default `0`.
- Richtlijn: latency + 1 dag marge.
- Alternatief: periodieke backfill (N dagen).

## 6. Promotie (TST → ACC → PROD)

- Zelfde `ProcessName` in alle omgevingen; alleen `Env` wijzigt.
- Promoot **LoadConfig** via script/DevOps.
- **Geen** WM‑promotie; WM is env‑specifiek.

## 7. Veelgemaakte fouten & oplossingen

| Fout                  | Oorzaak                                     | Oplossing                                      |
| --------------------- | ------------------------------------------- | ---------------------------------------------- |
| ‘Invalid object name’ | Typo in `SourceObject`/`TargetTable`        | `OBJECT_ID`‑check (4.1); correctie en redeploy |
| INCR laadt niets      | WM te hoog of verkeerde WM‑kolom            | `usp_Watermark_Get`; WM reset of kolom fix     |
| FULL 2e run ≠ 0/0     | Geen diff‑predicate of `UpdateColumns` leeg | Loader herdeployen; `UpdateColumns` vullen     |
| UX fail               | NULL/duplicate BK in target                 | Gefilterde UNIQUE of data opschonen            |

## 8. Appendix — Seed patronen

### 8.1 Eén script voor meerdere processen

```sql
MERGE dwh.LoadConfig AS T
USING (VALUES
 (N'silver.DimA__dwh.DimA_TST','TST','FULL',N'silver',N'DimA',N'dwh',N'DimA',N'AKey',N'Name',NULL,NULL,1,1,0,N''),
 (N'silver.FactB_INCR__dwh.FactB_TST','TST','INCR_DATE',N'silver',N'FactB',N'dwh',N'FactB',N'BK1,BK2',N'Val',N'TXDATE',NULL,1,1,2,N'')
) AS S(...)
ON (T.ProcessName=S.ProcessName)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT(...)
VALUES(...);
```

### 8.2 WM reset (bulk)

```sql
UPDATE dwh.Watermark SET ValueDateTime='1900-01-01' WHERE ProcessName IN (
 N'silver.FactB_INCR__dwh.FactB_TST', N'silver.FactC_INCR__dwh.FactC_TST');
```

## 9. Geavanceerd

### 9.1 Data‑type matrix WatermarkColumn

| WM‑type bron          | Doeladvies               | Loader‑kosten       |
| --------------------- | ------------------------ | ------------------- |
| `datetime2`           | `datetime2(3)`           | Laag                |
| `date`                | `date` of `datetime2(3)` | Laag                |
| `datetime`            | `datetime2(3)`           | Laag                |
| `nvarchar` (ISO 8601) | `datetime2(3)` via view  | Medium (CONVERT)    |
| `nvarchar` (vrij)     | **Niet doen**            | Hoog (parse‑errors) |

### 9.2 Composite keys (BK)

- Indexvolgorde = `KeyColumns` volgorde.
- Overweeg **INCLUDE** kolommen op target UX voor veelgebruikte selecties (read‑heavy).

### 9.3 Lint‑query’s (config hygiene)

```sql
-- Witruimtes/uppercase normaliseren (rapportage)
SELECT ProcessName,
       KeyColumns   = STRING_AGG(LTRIM(RTRIM(value)), ',') WITHIN GROUP (ORDER BY (SELECT 1))
FROM dwh.LoadConfig
CROSS APPLY STRING_SPLIT(KeyColumns, ',')
GROUP BY ProcessName;

-- Drie‑delige namen spotten
SELECT * FROM dwh.LoadConfig
WHERE SourceObject LIKE '%.%.%' OR TargetTable LIKE '%.%.%';
```

### 9.4 Promotiesjabloon (YAML/DevOps – concept)

```yaml
steps:
- task: SqlDacpacDeploymentOnSqlServer@0
  inputs:
    SqlFile: 'scripts/promote_loadconfig_acc.sql'
    ConnectedServiceName: 'svc-acc'
```

### 9.5 KPI’s per proces (uit `vJobRun_Metrics`)

```sql
SELECT TOP (100)
  RunDate, ProcessName, RowsRead, RowsInserted, RowsUpdated
FROM dwh.vJobRun_Metrics
ORDER BY RunDate DESC, ProcessName;
```

---

**Checklist (DoD Config):**

-

