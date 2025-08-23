# CONFIG_GUIDE

## Tabellen
### cfg.LoadConfig
- ProcessName (PK)
- IsEnabled (bit, default 1)
- Mode (varchar(10), 'FULL' of 'INCR')
- IncrDate (date, NULL)
- RequireUniqueKey (bit, default 0)
- BatchSize (int, default 50000)

Gebruik `ProcessName` als dispatcher sleutel.

