# TESTS GUIDE — DWH Refresh & Governance

Doel: hoe je smoke‑ en E2E‑tests draait, uitbreidt en interpreteert. Alle E2E‑tests draaien in één transactie en eindigen met **ROLLBACK** (geen restdata).

## 1. Testsoorten

- **Smoke**: basiscontrole na deploy (`tests/00_smoke.sql`).
- **Loaders**: FULL + INCR\_DATE (idempotent, updates) (`tests/tests_loaders.sql`).
- **Dispatcher**: FULL+INCR via `@Env` (`tests/tests_dispatcher.sql`).
- **OverlapDays**: binnen/buiten venster (`tests/tests_incr_overlap.sql`).
- **Governance**: forbidden patterns (`dwh.usp_Governance_Checks`).
- **Index‑ensure**: UX BK en WM‑index (`usp_EnsureIndexes_ForEnv`).

## 2. Uitvoeren (SQLCMD)

```sql
:r ./deploy/deploy_all.sql      -- indien nodig
:r ./tests/00_smoke.sql
:r ./tests/tests_loaders.sql
:r ./tests/tests_dispatcher.sql
:r ./tests/tests_incr_overlap.sql
```

**AC:**

- Smoke → geen nullen, dummy‑run gelogd
- Loaders → `ALLE TESTS OK`
- Dispatcher → `DISPATCHER TESTS OK`
- Overlap → `OVERLAP TESTS OK`

## 3. Omgevingsvereisten

- Schema’s `dwh` en `silver` aanwezig.
- Rollen: testaccount kan DDL (voor \*\_TEST tabellen) en EXEC procs.
- Query Store aan (optioneel voor baseline).

## 4. Testdata

- E2E‑scripts maken eigen \*\_TEST tabellen in `silver`/`dwh` en droppen die.
- Geen afhankelijkheid op productie‑tabellen.
- WM wordt per test op `'1900-01-01'` gezet en niet opgeslagen door ROLLBACK.

## 5. Fouten/THROW interpreteren

- Elke assert gooit een **THROW** met code en boodschap:
  - `51001..51003` → FULL: inserts/idempotentie/update
  - `52001..52004` → INCR: inserts/idempotentie/late/back‑dated
  - `53001..53005` → Dispatcher: FULL/INCR/late‑arrival
  - `54001..54003` → OverlapDays: baseline/within/outside
- Herstel: lees de boodschap, controleer `dwh.JobRunLog` en herhaal deelstap.

## 6. Uitbreiden (nieuwe cases)

### 6.1 Nieuwe FULL‑case

- Seed `silver.<DimX>_TEST` en target `dwh.<DimX>_TEST`.
- Call `usp_Load_GenericUpsert` met juiste keys/updates.
- Voeg 3 asserts toe: insert, idempotent, update.

### 6.2 Nieuwe INCR‑case

- Seed `silver.<IncrX>_TEST` met WM‑kolom (`datetime2`).
- Voeg LoadConfig‑regel toe (ProcessName, keys, updates, WM, OverlapDays).
- Reset WM, run loader, asserts: insert/idempotent/late/back‑dated.

### 6.3 Dispatcher‑case

- 1 FULL + 1 INCR testproces in `LoadConfig` (Env=TST, Enabled=1).
- Run `usp_Dispatch_Load @Env='TST'`, assert metrics per proces.

## 7. Best practices

- **Transactie‑harnas**: houd E2E‑cases binnen één `BEGIN TRAN`/`ROLLBACK`.
- **Determinisme**: vaste datums (ISO 8601) en expliciete keys.
- **Schoonmaken**: drop \*\_TEST tabellen vóór aanmaken.
- **Isolatie**: testprocesnamen prefixen met `tests.*`.
- **WM‑sanity**: voor INCR testcases altijd `usp_Watermark_Set` vooraf.

## 8. CI‑integratie (indicatief)

- Stap 1: `:r ./deploy/deploy_all.sql` (TST DB) + smoke.
- Stap 2: loaders/dispatcher/overlap tests.
- Capture SSMS/ADS output als artefact.
- Fail de pipeline bij **THROW**.

## 9. Troubleshooting (kort)

| Symptoom              | Diagnose                                | Actie                                      |
| --------------------- | --------------------------------------- | ------------------------------------------ |
| “Invalid object name” | Volgorde, schema ontbreekt              | Run `deploy_all.sql`; check schema’s       |
| FULL 2e run ≠ 0/0     | Diff‑predicate uit / UpdateColumns leeg | Herdeploy FULL‑loader; vul UpdateColumns   |
| INCR laadt niets      | WM te hoog of verkeerde WM‑kolom        | `usp_Watermark_Get`; reset WM of kolom fix |
| Overlap outside ≠ 0/0 | OverlapDays > 0?                        | Zet OverlapDays=0 of corrigeer test        |

## 10. Referentie

- Loaders: `dwh.usp_Load_GenericUpsert`, `dwh.usp_Load_GenericIncrDate`
- Dispatcher: `dwh.usp_Dispatch_Load`
- Config: `dwh.LoadConfig`, `dwh.Watermark`
- Logging: `dwh.JobRunLog`, `dwh.JobRunLogEvent`, `dwh.vJobRun_Metrics`

---

## 11. Testontwerp (patronen)

- **Arrange‑Act‑Assert** per case: seed → run → assert.
- **Minimal set**: maak datasets zo klein mogelijk (3–5 rijen) voor begrijpelijke asserts.
- **Scheiding FULL/INCR**: verschillende \*\_TEST tabellen om side‑effects te voorkomen.
- **Klonen**: gebruik `SELECT INTO` uit productietabellen niet in tests; definieer schema expliciet.

## 12. Parametrisering

- Gebruik variabelen voor `@ProcessName`, `@Env`, `@WMcol`, `@Overlap` bovenaan elk script.
- Maak helper‑CTE’s of temp‑tabellen voor tellers (maar vermijd CTE‑scope valkuilen in dynamische SQL).

## 13. Resultaatcaptatie

- Metrics komen uit `dwh.JobRunLog` (laatste run per ProcessName).
- Gebruik `TOP (1) WITH TIES ... ROW_NUMBER() OVER (PARTITION BY ...)` om per proces de laatste run te pakken.
- Log events (optioneel) voor extra asserts (`MERGE_SUMMARY`).

## 14. Prestatie‑asserts (optioneel)

- Stel zachte drempels in (bijv. run < 60s) en log tijdsduur uit `DATEDIFF(ms, StartTimeUtc, EndTimeUtc)`.
- Geen harde **THROW** op tijd in basis‑E2E; alleen rapporteren.

## 15. Negatieve tests

- Verkeerde `ProcessName` → loader moet **failen** met duidelijke fout (`ProcessName niet gevonden`).
- Verkeerde `WatermarkColumn` → **THROW** of 0 rijen; assert op foutpad en geen datamutatie.

## 16. Concurrency (basis)

- Start twee INCR‑runs parallel op demo‑set; verwachte uitkomst: één slaagt, de ander werkt op lege `#F` en levert 0/0. Idempotentie blijft intact.

## 17. Datakwaliteit

- Target BK‑UX: voeg test die duplicate/NULL BK’s simuleert en assert dat **filtered UNIQUE** of datafix nodig is.
- WM‑kolom op nvarchar: test conversiepad (alleen in view); assert geen TRY\_CONVERT/ISDATE errors in proc (we gebruiken typed WM).

## 18. Security/permissies

- Test dat ETL‑rol `EXEC` op dwh en `SELECT` op silver heeft; zie permissie‑queries in RUNBOOK.
- Test dat read‑only geen `EXEC` op loaders heeft.

## 19. Resilience

- Forceer fout in loader (bijv. tijdelijk invalid target) en assert dat `JobRunLog` status `Failed` met `ErrorMessage` gevuld wordt.
- Assert dat loggingfout (events) de run **niet** breekt (helpers swallowen errors).

## 20. CI‑matrix (uitbreiding)

| Stap       | Action                                       | Fail‑conditie          | Artefact                       |
| ---------- | -------------------------------------------- | ---------------------- | ------------------------------ |
| Build      | Lint scripts (regex op GO in procs, 2‑delig) | Lint‑hit               | Lint‑rapport                   |
| Deploy     | `deploy_all.sql` + smoke                     | `THROW`/missing object | Smoke output                   |
| Tests      | loaders/dispatcher/overlap                   | **THROW**              | Test output + JobRunLog export |
| Governance | `usp_Governance_Checks`                      | hits > 0               | Governance rapport             |

## 21. Voorbeeld PowerShell runner (indicatief)

```powershell
$files = @('deploy/deploy_all.sql','tests/00_smoke.sql','tests/tests_loaders.sql','tests/tests_dispatcher.sql','tests/tests_incr_overlap.sql')
foreach ($f in $files) {
  Invoke-Sqlcmd -InputFile $f -ServerInstance $env:SQL_SERVER -Database $env:SQL_DB -Verbose -ErrorAction Stop
}
```

## 22. Naming & structuur tests

- Bestanden onder `/tests` met prefix `NN_` voor volgorde: `00_smoke.sql`, `10_loaders.sql`, `20_dispatcher.sql`, `30_incr_overlap.sql`.
- Geen spaties in bestandsnamen.
- Per test **één** duidelijke eindboodschap (PRINT) naast THROW’s.

## 23. Evidence & rapportage

- Exporteer `JobRunLog` (laatste dag) en `vJobRun_Metrics` als CSV/artefact bij CI.
- Log de **WM‑waarde** vóór en na INCR‑tests ter controle.

## 24. Definition of Done (tests)

- Alle E2E‑tests groen (geen THROW).
- Geen governance‑hits.
- Index‑ensure geen missers.
- WM‑sanity OK.
- Testbestanden bevatten rollback‑harnas en zijn deterministisch.

