# RUNBOOK — DWH Refresh & Governance

Kern: generieke FULL/INCR\_DATE‑loaders, dispatcher per omgeving, logging, governance, indexering en tests. Dit runbook beschrijft uitvoering, beheer en herstel.

## 0. Scope & rollen

- **Eigenaar**: Quinten Gefferie.
- **Operator**: ETL‑dienstaccount (rol `role_etl_exec`).
- **Omgevingen**: `TST` → `ACC` → `PROD`.

## 1. Voorwaarden

- Database‑opties: RCSI, Query Store, Automatic Tuning **aan**.
- Loggingtabellen en helper‑procs aanwezig.
- Configtabellen aanwezig: `dwh.LoadConfig`, `dwh.Watermark`.

Snel check:

```sql
SELECT is_read_committed_snapshot_on FROM sys.databases WHERE database_id=DB_ID();
SELECT actual_state_desc FROM sys.database_query_store_options;
```

## 2. Dagelijkse run (dispatcher)

Start alle ingeschakelde processen voor een omgeving:

```sql
EXEC dwh.usp_Dispatch_Load @Env=N'TST'; -- of ACC/PROD
```

**Nadien**

```sql
SELECT TOP (20) * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;
```

## 3. Ad‑hoc run (per proces)

**FULL**

```sql
EXEC dwh.usp_Load_GenericUpsert
  @ProcessName=N'<naam>',
  @SourceSchema=N'silver', @SourceObject=N'<bron>',
  @TargetSchema=N'dwh',   @TargetTable=N'<target>',
  @KeyColumns=N'Key1,Key2', @UpdateColumns=N'ColA,ColB';
```

**INCR\_DATE**

```sql
EXEC dwh.usp_Load_GenericIncrDate @ProcessName=N'<naam>';
```

## 4. Configuratie (LoadConfig)

**Doel:** één canonieke bron voor laadgedrag. Loaders lezen hieruit; geen hard‑coded paden in procs.

### 4.1 Datacontract & validaties

- **Tabel:** `dwh.LoadConfig` (per omgeving).
- **Primair sleutelveld:** `ProcessName` (uniek).
- **Referentiële aannames:** `Source*` bestaat, `Target*` is tabel, `KeyColumns` bestaat in bron én doel.

| Kolom                 | Vereist | Format                         | Richtlijn                                                |
| --------------------- | ------- | ------------------------------ | -------------------------------------------------------- |
| `ProcessName`         | ✓       | `<SourceView>__<Target>_<ENV>` | Case‑consistent; geen spaties.                           |
| `Env`                 | ✓       | `TST/ACC/PROD`                 | Dispatcher filtert hierop.                               |
| `LoadType`            | ✓       | `FULL`/`INCR_DATE`             | Kies op basis van bronwatermark.                         |
| `SourceSchema/Object` | ✓       | `schema`,`object`              | Gebruik views voor transform; tabellen voor performance. |
| `TargetSchema/Table`  | ✓       | `schema`,`tabel`               | Alleen tabellen; geen views.                             |
| `KeyColumns`          | ✓       | CSV                            | Volgorde = indexvolgorde.                                |
| `UpdateColumns`       |         | CSV                            | Leeg ⇒ alleen inserts.                                   |
| `WatermarkColumn`     | \*      | kolomnaam                      | Verplicht bij `INCR_DATE`. Type `datetime2` aanbevolen.  |
| `OverlapDays`         | ✓       | int (≥0)                       | Default `0`. Vangt late arrivals.                        |
| `BatchSize`           |         | int                            | Niet gebruikt (gereserveerd).                            |
| `RequireUniqueKey`    | ✓       | bit                            | `1` ⇒ target heeft UX op BK (gefilterd ok).              |
| `Enabled`             | ✓       | bit                            | Dispatcher voert alleen `1` uit.                         |
| `Comment`             |         | nvarchar                       | Eigenaar / notities.                                     |

**Automatische checks (SQL):**

```sql
-- Bestaan bron/doel
SELECT ProcessName,
  src = OBJECT_ID(QUOTENAME(SourceSchema)+'.'+QUOTENAME(SourceObject)),
  tgt = OBJECT_ID(QUOTENAME(TargetSchema)+'.'+QUOTENAME(TargetTable))
FROM dwh.LoadConfig WHERE Enabled=1;
-- Validatie keykolommen in doel
SELECT lc.ProcessName,c.name
FROM dwh.LoadConfig lc
CROSS APPLY STRING_SPLIT(lc.KeyColumns, ',') s
LEFT JOIN sys.columns c ON c.object_id=OBJECT_ID(QUOTENAME(lc.TargetSchema)+'.'+QUOTENAME(lc.TargetTable))
                       AND c.name = LTRIM(RTRIM(s.value))
WHERE c.name IS NULL;   -- geen match ⇒ fout in config
```

### 4.2 Voorbeelden (VOLLEDIG)

**FULL**: één sleutel, één updatekolom

```sql
MERGE dwh.LoadConfig AS T
USING (VALUES(
 N'silver.DimProductV2__dwh.DimProduct_TST','TST','FULL',
 N'silver','DimProductV2','dwh','DimProduct',
 N'ProductBK', N'Name', NULL, NULL, 1, 1, 0, N'FULL push dimproduct'
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

**INCR\_DATE**: samengestelde sleutel, overlap=2

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

### 4.3 Keuzematrix FULL vs INCR

| Situatie                                         | Advies                                     |
| ------------------------------------------------ | ------------------------------------------ |
| Bron heeft betrouwbare timestamp (insert/update) | `INCR_DATE` met `WatermarkColumn`          |
| Geen timestamp of veel retro‑wijzigingen         | `FULL` of hybride (INCR + periodieke FULL) |
| Kleine dimensionele sets                         | `FULL`                                     |

### 4.4 Veelgemaakte fouten & remedies

- **Fout bronnaam** → altijd `OBJECT_ID` check (zie 4.1).
- **Text‑WM** → normaliseer in view (`CONVERT(datetime2, col, 126)`).
- **Verkeerde **`` → UX fails of dubbele merges; valideer met sys.columns.
- ``** leeg bij verwacht verschil** → nooit updates; vul aan of kies `FULL`.

---

## 5. Watermark & OverlapDays

**Gedrag in loader:**

1. Lees `@wm` uit `dwh.Watermark` (default `'1900-01-01'`).
2. Filter bron: `>[DATEADD(DAY, -OverlapDays, @wm)]`.
3. MERGE; INSERT set bevat **altijd** `WatermarkColumn`.
4. Zet WM = `MAX(WatermarkColumn)` van verwerkte set.

### 5.1 Operationele scenario’s

- **Cold start (leeg target)**: reset WM → run levert alleen inserts.
- **Late arrivals (N dagen)**: zet `OverlapDays=N` tijdelijk of permanent.
- **Backfill (vaste periode)**: verhoog `OverlapDays` tijdelijk of maak aparte backfill‑proc met datumvenster.

### 5.2 Tuning‑richtlijnen

| Eigenschap                | Richtlijn                  |
| ------------------------- | -------------------------- |
| Data‑latency < 1 dag      | `OverlapDays=1`            |
| Latency 2–3 dagen         | `OverlapDays=3`            |
| Onregelmatige retro‑posts | Plan maandelijkse backfill |

### 5.3 Controles

```sql
-- WM en rijen na WM
DECLARE @wm datetime2; EXEC dwh.usp_Watermark_Get N'<proc>', @wm OUTPUT, NULL, NULL;
SELECT COUNT_BIG(*) AS rows_after_wm,
       MIN([WMcol]) AS min_wm, MAX([WMcol]) AS max_wm
FROM [silver].[<Source>] WHERE [WMcol] > DATEADD(DAY,-<OverlapDays>,@wm);
```

---

## 6. Logging

### 6.1 Wat loggen we

- **Run**: start/eind, status, `RowsRead/Inserted/Updated/Deleted`, error.
- **Events** (optioneel): begin/bronstats/merge/summary/eind.

### 6.2 Praktische queries

```sql
-- Laatste runs per proces
SELECT TOP (1) WITH TIES ProcessName, StartTimeUtc, Status, RowsInserted, RowsUpdated, ErrorMessage
FROM dwh.JobRunLog
ORDER BY ROW_NUMBER() OVER (PARTITION BY ProcessName ORDER BY StartTimeUtc DESC);

-- Foutlog vandaag
SELECT * FROM dwh.JobRunLog
WHERE CONVERT(date,StartTimeUtc)=CONVERT(date,SYSUTCDATETIME()) AND Status='Failed'
ORDER BY StartTimeUtc DESC;
```

### 6.3 Regels

- Logging **mag nooit** loader breken (TRY/CATCH in helpers).
- Metrics via **COUNT\_BIG**/OUTPUT; geen `@@ROWCOUNT`.

---

## 7. Indexen (BK & WM)

### 7.1 Doel & keuzes

- UX op `KeyColumns` (gefilterd `IS NOT NULL` toegestaan bij NULL‑BK’s).
- NCI op `WatermarkColumn` (alleen voor brontabellen).

### 7.2 Ensure & check

```sql
EXEC dwh.usp_EnsureIndexes_ForEnv @Env=N'TST';
-- targets zonder UX
SELECT lc.ProcessName FROM dwh.LoadConfig lc
WHERE lc.Enabled=1 AND lc.Env='TST' AND NOT EXISTS (
  SELECT 1 FROM sys.indexes i WHERE i.object_id=OBJECT_ID(QUOTENAME(lc.TargetSchema)+'.'+QUOTENAME(lc.TargetTable)) AND i.is_unique=1);
```

### 7.3 UX‑strategieën

- **Strikt**: maak BK kolommen `NOT NULL` + UX. Vereist opschonen.
- **Pragmatisch**: gefilterde UX `WHERE <elk BK> IS NOT NULL`.

---

## 8. Governance & monitoring

### 8.1 Governance‑scan

```sql
EXEC dwh.usp_Governance_Checks; -- verwacht leeg (excl. self)
```

**Actie:** verwijder `@@ROWCOUNT`/`SET ROWCOUNT`/`USE` uit modules; herdeploy.

### 8.2 KPI’s & performance

- Dagelijks: rijen verwerkt per proces (`vJobRun_Metrics`).
- Performance baseline: waits/IO/top CPU (`usp_Perf_Baseline`).

---

## 9. Rechten

### 9.1 Rollen

- `role_etl_exec`: EXEC op dwh, SELECT op silver, DML op dwh.
- `role_readonly`: SELECT op dwh.

### 9.2 Voorbeeld toewijzing

```sql
ALTER ROLE role_etl_exec ADD MEMBER [app_etl_spn];
ALTER ROLE role_readonly ADD MEMBER [bi_reader];
```

---

## 10. Deploy & smoke

### 10.1 Volgorde

```sql
:r ./deploy/deploy_all.sql
:r ./tests/00_smoke.sql
```

**Resultaat:** objecten bestaan; runlog OK; metrics‑view levert rijen.

### 10.2 Veelvoorkomende issues

- Ontbrekende permissies op `silver` ⇒ GRANT SELECT.
- `OBJECT_ID` nul in deploy ⇒ volgorde; rerun na `config`.

---

## 11. Tests (E2E)

### 11.1 Testsets

- `tests_loaders.sql`: FULL+INCR incl. idempotentie en update‑case.
- `tests_dispatcher.sql`: dispatcher, idempotentie, late arrival.
- `tests_incr_overlap.sql`: venster binnen/buiten.

### 11.2 Eigenschappen

- Draaien **in 1 transactie** en eindigen met **ROLLBACK** → geen restdata.
- Verwachte asserts zitten in de scripts (THROW met code bij fail).

---

## 12. Incidenten & herstel (runbooks)

| Scenario          | Diagnose                                | Stappen                                                                           |
| ----------------- | --------------------------------------- | --------------------------------------------------------------------------------- |
| INCR laadt niets  | WM te hoog; verkeerde WM‑kolom          | 1) `usp_Watermark_Get` 2) rijen na WM tellen 3) WM reset/OverlapDays ↑ 4) her‑run |
| FULL 2e run ≠ 0/0 | Geen diff‑predicate; UpdateColumns fout | 1) Check proc‑versie 2) Check UpdateColumns 3) herdeploy loader                   |
| UX fail op target | Dubbel/NULL in BK                       | 1) Analyse dub/NULL 2) gefilterde UX of datafix 3) her‑run                        |
| Trage INCR        | WM‑index ontbreekt                      | 1) EnsureSourceWMIndex 2) check plans 3) Query Store baseline                     |
| Dispatcher fail   | Eén proces faalt                        | 1) JobRunLog.Error 2) Proces isoleren 3) Ad‑hoc run 4) fix en dispatcher opnieuw  |

---

## 13. ADF/Orchestratie

- Pipeline `pl_dwh_dispatch(Env)`; activity: `SqlServerStoredProcedure` → `dwh.usp_Dispatch_Load`.
- Retries: 3 met exponentiële backoff. Timeout per activity: 60 min (afhankelijk van workload).
- Alerting op fail: mail/Teams/webhook.
- Geen harde parallelisatie tenzij bronlocks geen risico vormen.

---

## 14. Change management

**PR‑checklist:** conventies, tests, docs, indeximpact. **Release:** tag `vX.Y.Z`; CHANGELOG bijwerken; smoke uitvoeren na deploy.

---

## 15. Contact & eigenaarschap

- Incidentroute: Data Engineering on‑call.
- Functioneel eigenaar per proces: vastleggen in `LoadConfig.Comment`.

