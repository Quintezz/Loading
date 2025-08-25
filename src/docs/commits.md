# COMMITS — DWH Refresh & Governance

Hanteer **Conventional Commits**: `<type>(<scope>): <korte samenvatting>`

## Types
- `feat`: nieuwe functionaliteit
- `fix`: bugfix
- `docs`: documentatie
- `perf`: performance
- `refactor`: herstructurering zonder functionele wijziging
- `test`: tests/fixtures
- `chore`: infra/deploy/CI/metadata
- `revert`: draai een commit terug (automat. aangemaakt)

## Scopes (projectspecifiek)
- `loaders`, `incr`, `full`, `dispatch`, `config`, `wm`, `overlap`,
- `indexes`, `governance`, `logging`, `perf`, `views`,
- `tests`, `deploy`, `docs`, `pipeline`, `permissions`

## Voorbeelden
```
feat(loaders): add INCR_DATE OverlapDays with #F filter and wm-insert
fix(indexes): create filtered unique BK on DimItem to allow NULL BKs
perf(incr): add WM index ensure to speed up incremental filter
refactor(views): move vJobRun_Metrics to /src/views and include via deploy
chore(deploy): add deploy_all.sql and 00_smoke.sql with SQLCMD includes
docs(runbook): expand LoadConfig rules and WM reset procedures
test(dispatch): add E2E dispatcher test (idempotency + late-arrival)
revert(loaders): revert diff predicate due to regression in updates
```

## Body en footer (optioneel)
- **Body**: reden/wat is veranderd (korte alinea’s).
- **Footer**: breaking changes of issue‑referenties.

Voorbeeld:
```
feat(config): add OverlapDays to LoadConfig

Introduceert OverlapDays (int, default 0) om late arrivals te verwerken in INCR_DATE.
Loader filtert nu op DATEADD(DAY,-OverlapDays,@wm).

BREAKING CHANGE: none
Refs: #123
```

## Commitregels per veelvoorkomende wijziging
- Loaders: `feat(loaders): …` of `fix(loaders): …`
- Dispatcher: `feat(dispatch): …`
- Config schema/seed: `feat(config): …` of `chore(config): …`
- Index ensure: `perf(indexes): …`
- Governance: `chore(governance): …`
- Tests: `test(loaders|dispatch|overlap): …`
- Docs: `docs(<doc-naam>): …` (README, RUNBOOK, TEST-PLAN, …)
- Deploy: `chore(deploy): …`

## PR‑checklist
- [ ] Tests groen (smoke + E2E waar van toepassing)
- [ ] Geen `@@ROWCOUNT`/`SET ROWCOUNT`/`USE`
- [ ] 2‑delige objectnamen
- [ ] Index‑impact beoordeeld (BK/WM)
- [ ] Docs bijgewerkt (RUNBOOK/CHANGELOG)

## Tagging & releases
- Tag semantisch: `v<major>.<minor>.<patch>` (bijv. `v0.2.0`).
- Release notes: samenvatting per type (feat/fix/docs/test/chore).
- Bewaar smoke/E2E output en `vJobRun_Metrics` snapshot als evidence.

## Template (git message)
```
<type>(<scope>): <korte samenvatting>

<body – optioneel>

BREAKING CHANGE: <uitleg – optioneel>
Refs: <tickets – optioneel>
```

