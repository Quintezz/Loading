# CHANGELOG — DWH Refresh & Governance

Alle wijzigingen volgens [Conventional Commits](./COMMITS.md). Datums in UTC.

## [0.2.0] — 2025-08-23
### Features
- **Loaders**: generieke `FULL` (idempotente diff) en `INCR_DATE` (WM in INSERT, #F‑filter, `OverlapDays`).
- **Dispatcher**: `dwh.usp_Dispatch_Load @Env` voert alle ingeschakelde processen uit.
- **Config**: `dwh.LoadConfig` uitgebreid met `OverlapDays` (default 0).
- **Indexen**: ensure‑procs voor target‑BK (UNIQUE/gefilt.) en source‑WM (NCI).

### Docs
- `README.md`, `RUNBOOK.md`, `CONFIG_GUIDE.md`, `DEPLOY_GUIDE.md`, `TEST-PLAN.md` toegevoegd.

### Tests
- E2E: `tests_loaders.sql`, `tests_dispatcher.sql`, `tests_incr_overlap.sql` (rollback‑harnas).
- Smoke: `tests/00_smoke.sql`.

### Governance & Monitoring
- `dwh.usp_Governance_Checks` (forbidden patterns) en `dwh.vJobRun_Metrics`.
- `dwh.usp_Perf_Baseline` voor waits/IO/top CPU.

### Fixes/Hardening
- FULL‑diff met NULL‑safe predicate.
- INCR‑INSERT bevat altijd `WatermarkColumn`.
- Filtermaterialisatie (#F) om CTE‑scope/compile‑fouten te vermijden.

### Breaking changes
- Geen.

### Upgrade‑stappen
1. Voer `deploy/deploy_all.sql` uit.
2. RUN `tests/00_smoke.sql`.
3. (Optioneel) `EXEC dwh.usp_EnsureIndexes_ForEnv @Env='TST';`

---

## [0.1.0] — 2025-08-20
### Features
- Baseline: schema’s (`dwh`, `silver`), loggingtabellen/procs, `LoadConfig`/`Watermark`.

### Docs
- Eerste `README` en notities.

### Breaking changes
- Geen.

---

## [Unreleased]
### Kandidaten
- **Backfill proc**: herlaad laatste *N* dagen per proces.
- **SCD**: generieke SCD1/2 loader‑varianten.
- **Metrics view v2**: runtime‑percentiles per proces.
- **ADF**: voorbeeldpipeline‑bundle + ARM/Bicep voorbeeld.

### Overwegingen
- Filtered UNIQUE default voor targets met mogelijk NULL‑BK’s.
- CI‑stap voor smoke + E2E in TST.

---

## Migratie‑notities
- Promoot **LoadConfig** tussen omgevingen; **Watermark** niet (env‑specifiek).
- Rollback = herdeploy vorige tag; geen destructieve DDL in baseline.

---

## Objecten per release (overzicht)
**0.2.0 — procs**
- `dwh.usp_Load_GenericUpsert`, `dwh.usp_Load_GenericIncrDate`, `dwh.usp_Dispatch_Load`
- `dwh.usp_Watermark_Get`, `dwh.usp_Watermark_Set`
- `dwh.usp_EnsureTargetKeyIndex`, `dwh.usp_EnsureSourceWMIndex`, `dwh.usp_EnsureIndexes_ForEnv`
- `dwh.usp_Governance_Checks`, `dwh.usp_Perf_Baseline`

**0.2.0 — tabellen & views**
- `dwh.JobRunLog`, `dwh.JobRunLogEvent`, `dwh.LoadConfig`, `dwh.Watermark`
- `dwh.vJobRun_Metrics`

**0.2.0 — tests**
- `tests/00_smoke.sql`, `tests/tests_loaders.sql`, `tests/tests_dispatcher.sql`, `tests/tests_incr_overlap.sql`

