# DWH Refresh Repo

Doel: beherende SQL-procedures, config en pipelines voor DWH-refresh.

## Structuur
- `src/procs/` — jouw stored procedures (uit `procs.zip` geplaatst).
- `src/config/` — schema en tabellen voor config en logging.
- `tests/` — smoke tests en integratietests.
- `pipelines/ADF/` — pipeline-definities (JSON).
- `deploy/` — deploy-scripts.
- `docs/` — documentatie (stappenplan, runbook, guides).

## Snelstart
1. Voer `deploy/01_schemas.sql`, dan `deploy/02_tables.sql`, dan `deploy/03_seeds.sql` uit in volgorde.
2. Controleer met `tests/99_smoke_tests.sql`.
3. Draai pipelines uit `pipelines/ADF/` of via je orkestratie.

## Conventies
- Objectnamen: `[schema].[object]` (geen wrapper-views).
- Geen `@@ROWCOUNT` gebruiken voor metrics. Gebruik expliciete `COUNT_BIG()` of OUTPUT-metrics.
