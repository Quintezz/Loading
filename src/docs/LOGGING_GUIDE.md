# LOGGING GUIDE — DWH Refresh & Governance

Doel: eenduidig en robuust loggen van laadruns en events, zonder runtime‑impact of afhankelijkheden.

## 1. Objecten
- **Tabellen**
  - `dwh.JobRunLog` — 1 rij per run (begin/einde, status, metrics, fout).
  - `dwh.JobRunLogEvent` — 0..n rijen per run (events/metrics/kpi’s).
- **Procs**
  - `dwh.usp_JobRun_Start(@ProcessName, @JobRunId OUTPUT)`
  - `dwh.usp_JobRun_Event(@JobRunId, @EventType, @EventDetail, @Metric1Name, @Metric1Value, @Metric2Name, @Metric2Value)`
  - `dwh.usp_JobRun_End(@JobRunId, @Status, @RowsRead, @RowsInserted, @RowsUpdated, @RowsDeleted, @ErrorMessage)`

**Indexering**
- `IX_dwh_JobRunLogEvent_JobRunId(JobRunId, EventTimeUtc)`
- (aanbevolen) `IX_dwh_JobRunLog_Start(ProcessName, StartTimeUtc DESC)` — read‑optimalisatie

## 2. Log‑patroon (loader)
```sql
DECLARE @jr uniqueidentifier;
EXEC dwh.usp_JobRun_Start @ProcessName=N'<proc>', @JobRunId=@jr OUTPUT;
BEGIN TRY
  -- bronstatistieken (optioneel)
  EXEC dwh.usp_JobRun_Event @jr, N'SOURCE_STATS', N'<uitleg>', N'Rows', 12345;

  -- laadstap (MERGE/INSERT) + metrics (RowsRead/Inserted/Updated/Deleted)
  EXEC dwh.usp_JobRun_Event @jr, N'MERGE_SUMMARY', N'detail', N'Inserted', 1000, N'Updated', 50;

  EXEC dwh.usp_JobRun_End @jr, N'Succeeded', @RowsRead=12345, @RowsInserted=1000, @RowsUpdated=50, @RowsDeleted=0, @ErrorMessage=NULL;
END TRY
BEGIN CATCH
  DECLARE @err nvarchar(max)=ERROR_MESSAGE();
  EXEC dwh.usp_JobRun_End @jr, N'Failed', NULL,NULL,NULL,NULL, @ErrorMessage=@err;
  THROW;
END CATCH;
```

**Regels**
- Logging mag de loader **niet** breken: helper‑procs hebben eigen TRY/CATCH en swallowen fouten.
- **Geen** `@@ROWCOUNT`; metrics expliciet via `COUNT_BIG()`/OUTPUT‑tellingen.
- Log **altijd** `ProcessName` zoals in `LoadConfig`.

## 3. Eventtypes (aanbevolen)
| EventType       | Gebruik                                   | Metrics (voorbeeld)            |
|-----------------|--------------------------------------------|--------------------------------|
| `START`         | Begin run                                  |                                |
| `SOURCE_STATS`  | Bronstatistiek(en)                          | `Rows=…`                       |
| `FILTER_STATS`  | Na WM/Overlap filter                        | `RowsAfterWM=…`                |
| `MERGE_SUMMARY` | Samenvatting insert/update/delete           | `Inserted=…`, `Updated=…`      |
| `END`           | Einde run                                   |                                |
| `ERROR`         | Foutconditie (kan ook alleen in End)        | `ErrCode=…` (optioneel)        |

## 4. Lezen & rapportage
**Laatste run per proces**
```sql
SELECT TOP (1) WITH TIES ProcessName, StartTimeUtc, EndTimeUtc, Status,
       RowsRead, RowsInserted, RowsUpdated, RowsDeleted, ErrorMessage
FROM dwh.JobRunLog
ORDER BY ROW_NUMBER() OVER (PARTITION BY ProcessName ORDER BY StartTimeUtc DESC);
```

**Chronologische event‑stream voor één run**
```sql
DECLARE @jr uniqueidentifier = (SELECT TOP (1) JobRunId FROM dwh.JobRunLog WHERE ProcessName=N'<proc>' ORDER BY StartTimeUtc DESC);
SELECT EventTimeUtc, EventType, EventDetail, Metric1Name, Metric1Value, Metric2Name, Metric2Value
FROM dwh.JobRunLogEvent WHERE JobRunId=@jr ORDER BY EventTimeUtc ASC;
```

**Dagelijkse KPI’s (view)**
```sql
SELECT * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;
```

## 5. Retentie & onderhoud
- Retentie (advies):
  - `JobRunLogEvent`: 90 dagen
  - `JobRunLog`: 365 dagen
- Opschoning (voorbeeld batch‑patroon):
```sql
-- Events ouder dan 90 dagen
DELETE TOP (5000) FROM dwh.JobRunLogEvent WHERE EventTimeUtc < DATEADD(DAY,-90,SYSUTCDATETIME());
-- Runs ouder dan 365 dagen en zonder events
DELETE TOP (1000) FROM dwh.JobRunLog WHERE EndTimeUtc < DATEADD(DAY,-365,SYSUTCDATETIME());
```
Voer opschoning via Agent/ADF regelmatig uit.

## 6. Troubleshooting
| Symptoom                      | Diagnose                                  | Actie                                     |
|------------------------------|-------------------------------------------|-------------------------------------------|
| Geen regels in JobRunLog     | Start‑proc niet aangeroepen               | Loader‑patroon volgen                     |
| Alleen START, geen END       | Loader crash vóór End                      | Error afvangen; End in CATCH laten loggen |
| Metrics = NULL               | Loader voert geen tellers uit              | `COUNT_BIG()`/OUTPUT toevoegen            |
| Report traag op events       | Missende index op `JobRunLogEvent`         | `IX ... (JobRunId, EventTimeUtc)`         |

## 7. Beveiliging & rechten
- ETL‑rol (`role_etl_exec`) heeft: EXEC op dwh, SELECT op silver, DML op dwh.
- Readonly‑rol (`role_readonly`) heeft: SELECT op dwh (incl. logtabellen/views).
- Geen directe grants aan individuele users; werk via rollen.

## 8. Best practices
- Gebruik **correlatie‑id** = `JobRunId` in alle debug/monitoringqueries.
- Log **maximaal** wat zinvol is; geen gevoelige data in `EventDetail`.
- Gebruik **UTC** (procs loggen `SYSUTCDATETIME()`).
- Schrijf korte, eenduidige `EventType`‑waarden (geen zinnen).
- Voeg per loader minimaal 2 events toe: `SOURCE_STATS`, `MERGE_SUMMARY`.

