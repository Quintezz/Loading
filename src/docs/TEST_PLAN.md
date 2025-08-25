# TEST-PLAN — DWH Refresh & Governance

## 1. Scope
Valideert generieke loaders (FULL/INCR_DATE), dispatcher, OverlapDays, logging, governance, indexering, deploy en permissies in `TST`.

## 2. Randvoorwaarden
- Deploy uitgevoerd (`deploy/deploy_all.sql`).
- Rollen en rechten gezet (ETL‑account in `role_etl_exec`).
- `silver` en `dwh` schema’s bestaan.

## 3. Testdata & scripts
- Smoke: `tests/00_smoke.sql`.
- Loaders: `tests/tests_loaders.sql` (FULL + INCR_DATE, rollback).
- Dispatcher: `tests/tests_dispatcher.sql` (FULL + INCR, rollback).
- Overlap: `tests/tests_incr_overlap.sql` (rollback).

## 4. Testmatrix
| ID | Onderdeel | Doel | Metriek/verwachting |
|---|---|---|---|
| T0 | Smoke | Deploy baselines | Opties aan, objecten bestaan, runlog schrijft |
| T1 | FULL run #1 | Inserts | JobRunLog.RowsInserted > 0, RowsUpdated = 0 |
| T2 | FULL idempotent | 0/0 | Volgende run RowsInserted=0, RowsUpdated=0 |
| T3 | FULL update | 1 update | RowsInserted=0, RowsUpdated=1 |
| T4 | INCR run #1 | Inserts | RowsInserted > 0, RowsUpdated=0 |
| T5 | INCR idempotent | 0/0 | Volgende run 0/0 |
| T6 | INCR late‑arrival | 1 insert | Nieuwe rij met WM > huidige WM → RowsInserted=1 |
| T7 | INCR back‑dated | 0/0 | Nieuwe rij met WM < (WM−Overlap) → 0/0 |
| T8 | Dispatcher run #1 | FULL+INCR | FULL: 2/0, INCR: 3/0 (demo) |
| T9 | Dispatcher idempotent | 0/0 | Volgende run 0/0 per proces |
| T10 | Dispatcher late‑arrival | 1/0 | Nieuwe latere rij → 1/0 |
| G1 | Governance | Geen verboden patronen | `usp_Governance_Checks` geeft 0 rijen (excl. self) |
| I1 | Index target BK | UX aanwezig | Unieke (desnoods gefilterde) index op BK |
| I2 | Index source WM | NCI aanwezig | Index op `WatermarkColumn` (tabellen) |
| P1 | Permissies | ETL‑rol OK | EXEC op dwh, SELECT op silver, DML op dwh |
| B1 | Baseline | Perf‑queries | Waits/IO/top CPU zichtbaar |

## 5. Uitvoering
### 5.1 Smoke
```sql
:r ./tests/00_smoke.sql
```
**AC:** Opties aan, objecten ≠ NULL, dummy‑run in log.

### 5.2 Loaders E2E
```sql
:r ./tests/tests_loaders.sql
```
**AC:** Alle asserts pass; eindigt met `ALLE TESTS OK`.

### 5.3 Dispatcher E2E
```sql
:r ./tests/tests_dispatcher.sql
```
**AC:** Eindigt met `DISPATCHER TESTS OK`.

### 5.4 OverlapDays E2E
```sql
:r ./tests/tests_incr_overlap.sql
```
**AC:** Eindigt met `OVERLAP TESTS OK`.

### 5.5 Governance
```sql
EXEC dwh.usp_Governance_Checks;  -- verwacht 0 rijen (excl. self)
```
**AC:** Geen hits op productie‑procs/views.

### 5.6 Index‑ensure
```sql
EXEC dwh.usp_EnsureIndexes_ForEnv @Env=N'TST';
-- Targets zonder UX
SELECT lc.* FROM dwh.LoadConfig lc
WHERE lc.Enabled=1 AND lc.Env='TST' AND NOT EXISTS (
  SELECT 1 FROM sys.indexes i
  WHERE i.object_id=OBJECT_ID(QUOTENAME(lc.TargetSchema)+'.'+QUOTENAME(lc.TargetTable)) AND i.is_unique=1);
```
**AC:** Lege lijst of gefilterde UX aanwezig.

### 5.7 Permissies
```sql
-- ETL rol beschikt over EXEC dwh + SELECT silver + DML dwh
SELECT 'ok' WHERE HAS_PERMS_BY_NAME('dwh','SCHEMA','EXECUTE')=1;
SELECT 'ok' WHERE HAS_PERMS_BY_NAME('silver','SCHEMA','SELECT')=1;
SELECT 'ok' WHERE HAS_PERMS_BY_NAME('dwh','SCHEMA','UPDATE')=1 AND HAS_PERMS_BY_NAME('dwh','SCHEMA','INSERT')=1 AND HAS_PERMS_BY_NAME('dwh','SCHEMA','DELETE')=1 AND HAS_PERMS_BY_NAME('dwh','SCHEMA','SELECT')=1;
```
**AC:** ‘ok’ rijen aanwezig.

### 5.8 Performance baseline
```sql
EXEC dwh.usp_Perf_Baseline;
```
**AC:** Query levert rijen voor waits/IO/top CPU.

## 6. Acceptatiecriteria (DoD)
- T0–T10, G1, I1–I2, P1, B1 zijn **groen**.
- `dwh.vJobRun_Metrics` toont recente runs.
- `dwh.LoadConfig` gevuld voor alle processen in scope (Enabled=1 voor TST).

## 7. Rollback & cleanup
- Alle e2e‑tests draaien in één transactie en eindigen met **ROLLBACK**.
- Handmatige tests: drop alleen *_TEST tabellen uit scripts.
- Geen aanpassing van productieobjecten buiten scope.

## 8. CI‑aanwijzingen
- Pipeline stap 1: `:r ./deploy/deploy_all.sql` + smoke.
- Pipeline stap 2: loaders/dispatcher/overlap tests (in TST DB), capture output; fail op THROW.
- Artefact: export `JobRunLog` en `vJobRun_Metrics` als build evidence.

## 9. Risico’s & mitigaties
- **Late arrivals > OverlapDays** → periodieke backfill (laatste N dagen) of OverlapDays verhogen.
- **NULL/duplicate BK** → gefilterde UNIQUE of dataopschoning.
- **Bron zonder WM** → kies FULL of bekijk andere incremental strategie.

## 10. Traceability
- Tests dekken: Loaders (FULL/INCR), Dispatcher, Overlap, Governance, Indexen, Permissies, Baseline.
- Gerelateerde documenten: RUNBOOK.md, DEPLOY_GUIDE.md, CONFIG_GUIDE.md.

