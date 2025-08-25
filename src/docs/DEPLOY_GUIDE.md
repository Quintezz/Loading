# DEPLOY GUIDE — DWH Refresh & Governance

Doel: veilig en reproduceerbaar uitrollen van schemas, tabellen, procs, views en tests over `TST → ACC → PROD`.

## 1. Voorwaarden

- Toegang: `db_owner` of gelijkwaardig voor DDL.
- Tools: SSMS/ADS met **SQLCMD** support (Parse as SQLCMD = aan).
- Netwerk/Firewall: outbound naar doel‑DB open.
- Back‑up/restorebeleid bekend (PROD) + change window.
- Service‑account voor ETL is bekend en aan `role_etl_exec` te koppelen.

### 1.1 Preflight checklist

- **Branch/tag**: release‑branch up‑to‑date, code review gedaan, versie/CHANGELOG bijgewerkt.
- **Wijzigingsimpact**: alleen DDL zonder dataverlies; geen breaking changes in runtime‑procs.
- **Toegang**: deploy‑account kan DDL uitvoeren; ETL‑account beschikbaar voor tests.
- **Permissies**: `role_etl_exec`/`role_readonly` klaar; SPN’s bekend.
- **DB‑opties**: RCSI/QStore/Automatic Tuning toegestaan in doelomgeving.
- **Capaciteit**: vCore/DTU, IO‑budget, tempdb en logruimte voldoende.
- **Connectiviteit**: firewall/NSG open; test `SELECT 1;` op doel.
- **Back‑ups**: PITR/backup‑policy actief; rollback‑plan (vorige tag + scripts) klaar.
- **Schema’s**: `dwh` en `silver` aanwezig of worden aangemaakt.
- **LoadConfig promotie**: lijst processen + `Env`/`Enabled` bepaald; owners akkoord.
- **Secrets**: connection strings/KeyVault referenties gecontroleerd.
- **ADF/Orkestratie**: pipeline‑rechten en runtime SPN/MI klaar (indien gebruikt).
- **Monitoring**: Query Store aan; alerting/observability klaar.
- **Communicatie**: change window/impact gecommuniceerd.
- **Testplan**: smoke + E2E tests beschikbaar en bekend.

**Preflight‑queries**

```sql
-- 1) Sessie/lock sanity
SELECT COUNT(*) AS active_sessions FROM sys.dm_exec_sessions WHERE is_user_process=1;
-- 2) Vrije logruimte (SQL MI/VM)
DBCC SQLPERF(LOGSPACE);
-- 3) QS status
SELECT actual_state_desc FROM sys.database_query_store_options;
-- 4) Schijfruimte (Azure SQL niet van toepassing)
SELECT SUM(size)*8/1024 AS data_mb FROM sys.database_files;
```

## 2. Omgevingsvariabelen

- **Database‑opties**: RCSI, Query Store, Automatic Tuning **aan**.
- **Schema’s**: `dwh`, `silver` bestaan of worden aangemaakt.
- **Accounts**: ETL‑account in `role_etl_exec`, leesaccounts in `role_readonly`.

Snel toepassen (indien nodig) — staat ook in `deploy_all.sql`:

```sql
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;
ALTER DATABASE CURRENT SET QUERY_STORE = ON;
ALTER DATABASE CURRENT SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY=(STALE_QUERY_THRESHOLD_DAYS=30));
ALTER DATABASE CURRENT SET AUTOMATIC_TUNING (FORCE_LAST_GOOD_PLAN = ON, CREATE_INDEX = ON, DROP_INDEX = ON);
```

## 3. Volgorde (SQLCMD)

Voer in **deze** volgorde uit. `GO` is toegestaan in deploy‑scripts; geen `GO` in runtime‑procs.

```sql
:r ./deploy/deploy_all.sql
:r ./tests/00_smoke.sql
```

### 3.1 Wat gebeurt er in `deploy_all.sql`

1. Schema’s & DB‑opties (RCSI, QDS, Automatic Tuning)
2. Logging‑tabellen + helper‑procs
3. Config‑tabellen (`LoadConfig`, `Watermark`)
4. Watermark‑procs (`Get/Set`)
5. Loaders FULL/INCR (idempotent diff, WM in INSERT, OverlapDays)
6. Dispatcher (`usp_Dispatch_Load`)
7. Governance‑proc + `vJobRun_Metrics`

### 3.2 Smoke (`tests/00_smoke.sql`)

- Verifieert DB‑opties
- Objecten bestaan
- Dummy‑run in `JobRunLog`

### 3.3 Alternatieve runners

- **Azure Data Studio**: SQLCMD‑runner of Tasks met `:r` includes.
- **PowerShell**: `Invoke-Sqlcmd -InputFile deploy_all.sql` gevolgd door `00_smoke.sql`.

## 4. Post‑deploy stappen

1. **Permissies** (indien nog niet gedaan):

```sql
:r ./src/admin/permissions_roles.sql
```

2. **Indexen afdwingen** (targets BK, source WM):

```sql
EXEC dwh.usp_EnsureIndexes_ForEnv @Env=N'TST';
```

3. **Config seeden** (optioneel demo):

```sql
:r ./src/config/seed_LoadConfig.sql
```

4. **Dispatcher dry‑run** en metrics:

```sql
EXEC dwh.usp_Dispatch_Load @Env=N'TST';
SELECT TOP (20) * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;
```

## 5. Validaties (acceptatiecriteria)

- `:r ./tests/tests_loaders.sql` → `ALLE TESTS OK`
- `:r ./tests/tests_dispatcher.sql` → `DISPATCHER TESTS OK`
- `:r ./tests/tests_incr_overlap.sql` → `OVERLAP TESTS OK`
- `EXEC dwh.usp_Governance_Checks;` → geen hits (excl. self)
- `EXEC dwh.usp_EnsureIndexes_ForEnv @Env=N'TST';` → geen missende indexen

### 5.1 Query‑snippets voor snelle checks

```sql
-- Governance leeg
EXEC dwh.usp_Governance_Checks;
-- Laatste runs per proces
SELECT TOP (1) WITH TIES ProcessName, StartTimeUtc, Status, RowsInserted, RowsUpdated
FROM dwh.JobRunLog ORDER BY ROW_NUMBER() OVER (PARTITION BY ProcessName ORDER BY StartTimeUtc DESC);
-- WM sanity voor 1 proces
DECLARE @wm datetime2; EXEC dwh.usp_Watermark_Get N'<proc>', @wm OUTPUT, NULL, NULL;
SELECT COUNT_BIG(*) rows_after_wm FROM silver.<Source> WHERE <WMcol> > DATEADD(DAY,-(SELECT OverlapDays FROM dwh.LoadConfig WHERE ProcessName=N'<proc>'), @wm);
```

## 6. Promotie naar ACC/PROD

1. Promoot **code** (scripts) 1‑op‑1 (zelfde structuur/inhoud).
2. Promoot **LoadConfig** regels via gecontroleerd script of DevOps pipeline.
3. **WM niet promoten**: watermarks zijn per omgeving.
4. Herhaal validaties (smoke + governance + indexen + tests waar passend).

### 6.1 DevOps pipeline (voorbeeld)

```yaml
stages:
- stage: Deploy_ACC
  jobs:
  - job: sql
    steps:
    - task: SqlAzureDacpacDeployment@1
      inputs:
        deployType: SqlTask
        sqlFile: 'deploy/deploy_all.sql'
        connectedServiceNameARM: 'svc-acc'
    - task: SqlAzureDacpacDeployment@1
      inputs:
        deployType: SqlTask
        sqlFile: 'tests/00_smoke.sql'
        connectedServiceNameARM: 'svc-acc'
```

## 7. Rollbackplan

- DDL is idempotent; rollback = “redeploy vorige versie”.
- Geen destructieve DDL in baseline.
- Productiedata wordt niet aangepast door `deploy_all.sql`; loaders worden los gestart.
- **Hot rollback** (alleen procs/views): herdeploy vorige commit van betreffende file(s).

## 8. Hotfix‑richtlijnen

- Kleine proc‑fix: voer alleen het proc‑bestand uit (géén hele deploy).
- Na hotfix: relevante test(s) draaien + governance‑check.
- Documenteer in `docs/CHANGELOG.md` + commit `fix(...)`.

## 9. Veelvoorkomende issues (uitgebreid)

| Symptoom                              | Oorzaak                                       | Oplossing                                                     |
| ------------------------------------- | --------------------------------------------- | ------------------------------------------------------------- |
| `Invalid object name` tijdens deploy  | Volgorde/afhankelijkheden                     | Run `deploy_all.sql` volledig; herhaal smoke                  |
| Smoke faalt op opties                 | DB‑opties niet toegestaan op PaaS‑tier/beleid | Zet via Portal/DBA; bevestig met query; rerun smoke           |
| Governance‑hits                       | `@@ROWCOUNT`/`SET ROWCOUNT`/`USE` in modules  | Aanpassen, redeploy en re‑run governance                      |
| Index ensure fail (duplicate/NULL BK) | Data‑kwaliteit                                | Gefilterde UNIQUE (`WHERE BK IS NOT NULL`) of eerst opschonen |
| Permission denied op silver           | Rol niet toegewezen                           | `ALTER ROLE role_etl_exec ADD MEMBER <principal>`; rerun      |
| Query Store disabled                  | Policy of tier                                | Enable (ACC/PROD); alternatief monitoring inschakelen         |

## 10. ADF/Orchestratie (optioneel)

- Pipeline `pl_dwh_dispatch(Env)` met activity: `SqlServerStoredProcedure` → `dwh.usp_Dispatch_Load`.
- Parameters: `Env` (`TST/ACC/PROD`).
- Retries: 3; timeout 60 min; alerting op fail.
- **Parallel**: alleen als bronlocks geen risico zijn; anders sequentieel.

## 11. Versiebeheer & release

- **Commits**: Conventional Commits; update `docs/CHANGELOG.md` per feature/fix.
- **Tag**: `vX.Y.Z` (bijv. `v0.2.0`).
- **Artefacts**: export `dwh.JobRunLog` en `dwh.vJobRun_Metrics` van deploydag als evidence.

## 12. Appendix

### 12.1 Handige queries

```sql
-- Laatste 10 runs
SELECT TOP (10) ProcessName, StartTimeUtc, Status, RowsInserted, RowsUpdated
FROM dwh.JobRunLog ORDER BY StartTimeUtc DESC;
-- Metrics per dag
SELECT * FROM dwh.vJobRun_Metrics ORDER BY RunDate DESC, ProcessName;
```

### 12.2 SQLCMD tips

- Gebruik `:r` voor include van bestanden (geen copy/paste).
- Eén “deploy” connection; geen lokale transacties rond `:r`.
- Zet “Parse as SQLCMD” aan in SSMS/ADS.
- Houd errorlist in de gaten; los fouten top‑down op (options → tables → procs → tests).

### 12.3 Pre‑/Post‑deploy scripts (optioneel)

- **Pre**: permissies/opties controleren, connection test, governance‑scan oud.
- **Post**: ensure‑indexen, dispatcher dry‑run, metrics snapshot.

