# STAPPENPLAN

Chronologisch logboek van uitgevoerde stappen.

## Gate 0 — Baseline
- **Doel**: Schemas en basistabellen aanmaken.
- **Acties**:
  - `deploy/01_schemas.sql`
  - `deploy/02_tables.sql`
- **Test**: `tests/99_smoke_tests.sql` -> alle counts ≥ 0.
- **Resultaat**: [vul in]

## Gate 1 — Config & Seeds
- **Doel**: `cfg.LoadConfig` vullen met processen.
- **Acties**:
  - `deploy/03_seeds.sql` aanvullen en uitvoeren.
- **Test**: `SELECT * FROM cfg.LoadConfig`.
- **Resultaat**: [vul in]

## Gate 2 — Loaders koppelen
- **Doel**: Procedures aan `LoadConfig` hangen met ProcessName-patroon `<ViewName>__<Target>_<TST|PROD>`.
- **Acties**: procs in `src/procs/` nalopen en registreren.
- **Test**: proef-run per proces; logging zichtbaar.
- **Resultaat**: [vul in]

## Gate 3 — Orkestratie
- **Doel**: `pl_dwh_refresh` actief met correcte parameters.
- **Acties**: ADF JSON deployen of handmatig aanmaken.
- **Test**: end-to-end job run zonder errors.
- **Resultaat**: [vul in]

## Gate 4 — Validatie
- **Doel**: idempotentie, watermarks, en unique keys borgen.
- **Acties**: smoke + integratietests uitbreiden.
- **Test**: alle tests groen.
- **Resultaat**: [vul in]
