# DWH Refresh & Governance

Kern: generieke loaders (FULL/INCR_DATE), dispatcher per omgeving, logging, governance, indexering en tests. Ontworpen voor Azure SQL/SQL Server.

## Inhoud
- [Architectuur](#architectuur)
- [Conventies](#conventies)
- [Repo-structuur](#repo-structuur)
- [Installatie & Deploy](#installatie--deploy)
- [Configuratie (LoadConfig/Watermark)](#configuratie-loadconfigwatermark)
- [Loaders & Dispatcher](#loaders--dispatcher)
- [Watermark & OverlapDays](#watermark--overlapdays)
- [Logging](#logging)
- [Governance & Monitoring](#governance--monitoring)
- [Indexen](#indexen)
- [Tests](#tests)
- [Operatie (RUNBOOK)](#operatie-runbook)
- [Troubleshooting](#troubleshooting)
- [Roadmap & Changelog](#roadmap--changelog)
- [Commits](#commits)

---

## Architectuur
**Bron → Loader → Doel** met centrale configuratie.
- **Bron**: `silver.*` views/tabellen.
- **Loaders**: `dwh.usp_Load_GenericUpsert` (FULL) en `dwh.usp_Load_GenericIncrDate` (INCR_DATE).
- **Dispatcher**: `dwh.usp_Dispatch_Load @Env` voert alle ingeschakelde processen uit.
- **Configuratie**: `dwh.LoadConfig`, watermarks in `dwh.Watermark`.
- **Logging**: `dwh.JobRunLog` + `dwh.JobRunLogEvent`.

## Conventies
- Altijd **2‑delige objectnamen** `[schema].[object]`.
- **Geen** `@@ROWCOUNT`, **geen** `SET ROWCOUNT`, **geen** `USE` in runtime-procs.
- Loaders **idempotent**; updates alleen bij verschil (NULL‑safe diff).
- Watermark staat altijd in de **INSERT‑set** bij INCR_DATE.

## Repo-structuur
```
/src/admin      # opties, logging, governance, permissions
/src/config     # LoadConfig/Watermark + seeds
/src/procs      # loaders, dispatcher, ensure-indexes, watermark, perf
/src/views      # vJobRun_Metrics
/deploy         # deploy_all.sql, 00_smoke.sql
/tests          # tests_loaders.sql, tests_dispatcher.sql, tests_incr_overlap.sql
/pipelines      # pl_dwh_dispatch.json (ADF)
/docs           # README, RUNBOOK, TEST-PLAN, CHANGELOG, ROADMAP, COMMITS, CONFIG/LOGGING_GUIDE
```

## Installatie & Deploy
1. **Schemas & opties**: RCSI, Query Store, Automatic Tuning.
2. **Logging**: tabellen en helper‑procs.
3. **Config**: `LoadConfig` + `Watermark` (+ seed indien gewenst).
4. **Views & procs**: loaders, dispatcher, ensure‑indexen, governance, perf.
5. **Smoke**: `tests/00_smoke.sql`.

> Snelle route (SQLCMD/SSMS):
```sql
:r ./deploy/deploy_all.sql
:r ./tests/00_smoke.sql
```

## Configuratie (LoadConfig/Watermark)
`[dwh].[LoadConfig]` kolommen (belangrijkste):
- `ProcessName` (uniek, bv. `silver.FactInvoiceMarkup_INCR__dwh.FactInvoiceMarkup_TST`)
- `Env` (`TST/ACC/PROD`), `LoadType` (`FULL`|`INCR_DATE`)
- `SourceSchema`, `SourceObject`, `TargetSchema`, `TargetTable`
- `KeyColumns` (CSV), `UpdateColumns` (CSV)
- `WatermarkColumn` (bij INCR_DATE)
- `OverlapDays` (default 0)

Watermark beheer:
```sql
EXEC dwh.usp_Watermark_Get @ProcessName, @ValueDateTime OUTPUT, NULL, NULL;
EXEC dwh.usp_Watermark_Set @ProcessName, @ValueDateTime='1900-01-01'; -- reset
```

## Loaders & Dispatcher
**FULL**
```sql
EXEC dwh.usp_Load_GenericUpsert
  @ProcessName   = N'<naam>',
  @SourceSchema  = N'silver', @SourceObject = N'<bron>',
  @TargetSchema  = N'dwh',    @TargetTable  = N'<target>',
  @KeyColumns    = N'Key1,Key2',
  @UpdateColumns = N'ColA,ColB';
```
**INCR_DATE**
```sql
EXEC dwh.usp_Load_GenericIncrDate @ProcessName = N'<naam>';  -- leest LoadConfig + Watermark
```
**Dispatcher**
```sql
EXEC dwh.usp_Dispatch_Load @Env = N'TST';
```

## Watermark & OverlapDays
Filter: `> DATEADD(DAY, -OverlapDays, @wm)`.
- `OverlapDays=0`: snelste, geen late-arrival correctie.
- `OverlapDays>0`: vangt back‑dated events binnen venster; WM springt naar `MAX(WM)` van de run.

## Logging
- **Runs** in `dwh.JobRunLog` (tijden, status, metrics, error).
- **Events** in `dwh.JobRunLogEvent`.
- Metrics via **COUNT_BIG**/OUTPUT, niet via `@@ROWCOUNT`.

## Governance & Monitoring
- **Governance**: `EXEC dwh.usp_Governance_Checks;`
- **Metrics view**: `SELECT * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;`
- **Performance baseline**: `EXEC dwh.usp_Perf_Baseline;`

## Indexen
- **Target BK**: unieke index op `KeyColumns` (gefilterd op `IS NOT NULL` indien nodig).
- **Bron WM**: nonclustered index op `WatermarkColumn` (alleen voor tabellen).
- Ensure (alle Enabled in Env):
```sql
EXEC dwh.usp_EnsureIndexes_ForEnv @Env=N'TST';
```

## Tests
- **Loaders**: `:r ./tests/tests_loaders.sql` (FULL & INCR_DATE, idempotent, back‑dated).
- **Dispatcher**: `:r ./tests/tests_dispatcher.sql` (FULL+INCR, idempotent, late‑arrival).
- **Overlap**: `:r ./tests/tests_incr_overlap.sql` (binnen/buiten venster).

## Operatie (RUNBOOK)
- Dagelijks: `EXEC dwh.usp_Dispatch_Load @Env='TST';`
- Watermark reset bij herverwerking; `OverlapDays` per proces.
- Rechten: `role_etl_exec` (EXEC/DML dwh, SELECT silver), `role_readonly` (SELECT dwh).

## Troubleshooting
- Foutmelding? Check `dwh.JobRunLog.ErrorMessage` en laatste events.
- Geen rijen bij INCR? Controleer `WatermarkColumn`, WM‑waarde en `OverlapDays`.
- Unique‑index fouten? Gebruik gefilterde UNIQUE of eerst data opschonen.

## Roadmap & Changelog
- Zie `/docs/ROADMAP.md` en `/docs/CHANGELOG.md`.

## Commits
Conventional Commits, bv.:
```
feat(loaders): add INCR_DATE OverlapDays and #F-filter
fix(indexes): create filtered unique BK index for DimItem
```

