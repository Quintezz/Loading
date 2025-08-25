# ROADMAP — DWH Refresh & Governance

Statusoverzicht per gate. Doel: voorspelbare oplevering TST → ACC → PRO.

## 0. Samenvatting
- **Bereikt**: G1–G5 (TST).
- **Open**: backfill‑proc, governance‑uitbreiding, ACC/PROD promotie.

## 1. Gates
| Gate | Titel | Doel | Belangrijkste deliverables | Acceptatie | Status |
|---|---|---|---|---|---|
| G1 | Basis & Logging | DB‑opties, schemas, logging | `db_options`, `JobRunLog*`, `usp_JobRun_*` | Smoke: dummy‑run gelogd | ✔ TST |
| G2 | Loaders | Generieke FULL/INCR | `usp_Load_GenericUpsert`, `usp_Load_GenericIncrDate` (WM in INSERT, diff) | E2E loaders = OK | ✔ TST |
| G3 | Dispatcher | Orchestratie per Env | `usp_Dispatch_Load`, ADF JSON | Dispatcher test = OK | ✔ TST |
| G4 | Governance & Indexen | Kwaliteit + performance | `usp_Governance_Checks`, `vJobRun_Metrics`, Ensure‑index procs | Governance leeg, Ensure OK | ✔ TST |
| G5 | Documentatie & Tests | Operatie en validatie | README, RUNBOOK, CONFIG/DEPLOY, TEST‑PLAN, CHANGELOG | Docs compleet, tests groen | ✔ TST |
| G6 | ACC Release | ACC‑promotie | Deploy ACC, LoadConfig promotie | Smoke+governance ACC OK | ◻ |
| G7 | PROD Release | PROD‑promotie | Deploy PROD, LoadConfig promotie | Smoke+governance PROD OK | ◻ |

## 2. Work‑items (openstaande taken)
- **Backfill**: generieke proc voor herlaad laatste *N* dagen per proces.
- **Governance V2**: extra lint (drie‑delige namen, spaties in objecten, NOLOCK detectie).
- **Filtered UX standaard**: Ensure‑proc aanpassen voor samengestelde BK (alle BK `IS NOT NULL`).
- **ACC/PROD runbooks**: concreet change‑venster, rollback en alerts.
- **CI**: pipeline stap voor smoke + E2E op TST (faalt op THROW).

## 3. Planning (indicatief)
| Item | Einddatum | Eigenaar | Afhankelijkheden |
|---|---|---|---|
| Backfill proc (N dagen) | 2025‑08‑26 | DE | Loaders G2 |
| Governance V2 | 2025‑08‑27 | DE | G5 |
| ACC Release | 2025‑08‑28 | DE/OPS | Governance V2, CI |
| PROD Release | 2025‑09‑02 | DE/OPS | ACC stable |

## 4. Risico’s & mitigatie
| Risico | Impact | Kans | Mitigatie |
|---|---|---|---|
| Late arrivals > OverlapDays | Gemiste rijen | Medium | Backfill N‑dagen job |
| NULL/dup BK in target | UX‑fouten | Medium | Filtered UNIQUE + dataopschoning |
| Ontbrekende permissies | Load fail | Low | Permissions script + smoke |
| Query Store off | Minder zichtbaarheid | Low | Enable op ACC/PROD |

## 5. Definition of Done (release)
- Smoke, governance, Ensure‑indexen **groen**.
- E2E tests (loaders/dispatcher/overlap) **groen**.
- README/RUNBOOK/CONFIG/DEPLOY/TEST‑PLAN/CHANGELOG **up‑to‑date**.
- Tag gezet en release notes gepubliceerd.

## 6. Bijlagen
- ADF: `pipelines/pl_dwh_dispatch.json`.
- Tests: `tests/00_smoke.sql`, `tests/tests_loaders.sql`, `tests/tests_dispatcher.sql`, `tests/tests_incr_overlap.sql`.

