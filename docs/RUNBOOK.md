# RUNBOOK

## Doel
Operationele handeling voor het draaien van de DWH-refresh.

## Triggers
- Dagelijks schema via ADF of scheduler.

## Stappen
1. Controleer `cfg.LoadConfig` voor ingeschakelde processen.
2. Start pipeline `pl_dwh_refresh`.
3. Monitor `log.JobRunLog` en `log.JobRunLogEvent`.

## Fallback
- Stop job. Noteer `JobRunId`.
- Analyseer laatste events: `SELECT * FROM log.JobRunLogEvent WHERE JobRunId = ... ORDER BY EventTime DESC`.
- Herstart enkel gefaalde processen na fix.

## KPI's
- Doorlooptijd, aantal records verwerkt, aantal errors.
