# LOGGING_GUIDE

## Tabellen
- log.JobRunLog
- log.JobRunLogEvent

## Procedures
- log.usp_JobRunLog_Start
- log.usp_JobRunLog_End
- log.usp_JobRunLog_Event

## Patronen
- Geen @@ROWCOUNT voor metrics. Sla aantallen expliciet op in JobRunLog.* kolommen.
