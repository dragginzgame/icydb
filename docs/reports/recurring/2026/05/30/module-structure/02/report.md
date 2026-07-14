# Structure / Module / Visibility Discipline Audit Rerun - 2026-05-30

## 0. Run Metadata + Comparability Note

- target scope: `icydb-core` structural boundaries and visibility discipline, with `db` subsystem emphasis
- compared same-day baseline report path: `docs/audits/reports/2026-05/2026-05-30/module-structure.md`
- code snapshot identifier: `feab0cb31` plus local uncommitted module-structure refactor
- method tag/version: `Method V4`
- comparability status: `comparable same-day rerun`
- exclusions applied: test-only files/modules excluded from metrics and structural invariant scripts
- methodology changes vs same-day baseline: none; this rerun records the post-action module split and added hub-threshold invariant

## 1. Change Summary Since Same-Day Baseline

| Action | Result | Structural Effect |
| ---- | ---- | ---- |
| Split SQL write execution by statement family | `write/mod.rs`, `write/insert.rs`, and `write/update.rs` now own shared, INSERT, and UPDATE logic respectively | former `session::sql::execute::write` hub reduced from `769` LOC to `258` LOC |
| Split schema mutation DDL contracts by family | `schema::mutation::field` and `schema::mutation::index` now own field/index admission and accepted-after helpers | former `schema::mutation` root reduced from `2219` LOC to `1224` LOC |
| Split SQL DDL binding by family | `sql::ddl::field` and `sql::ddl::index` now own field/index binders while root keeps dispatch/report/lowering | former `sql::ddl` root reduced from `1933` LOC to `613` LOC |
| Add CI hub threshold guard | `check-module-structure-hub-thresholds.sh` wired into `make check-invariants` | future hub regrowth now fails CI before the next manual audit |

## 2. Hub Import Pressure After Actions

| Hub Module | LOC | Fanout | Max Branch Depth | Branch Sites | Threshold Status | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- |
| `db::session::sql::execute::write` | 258 | 4 | 1 | 9 | below `350` LOC / `5` fanout | Low |
| `db::session::sql::execute::write::insert` | 389 | 7 | 3 | 24 | below `450` LOC / `8` fanout | Low-Medium |
| `db::session::sql::execute::write::update` | 175 | 6 | 1 | 4 | below `250` LOC / `7` fanout | Low |
| `db::schema::mutation` | 1224 | 3 | 2 | 49 | below `1300` LOC / `4` fanout | Medium |
| `db::schema::mutation::field` | 407 | 1 | 1 | 3 | below `550` LOC / `2` fanout | Low |
| `db::schema::mutation::index` | 631 | 1 | 2 | 16 | below `750` LOC / `2` fanout | Low-Medium |
| `db::schema::mutation::runner` | 763 | 2 | 1 | 10 | below `850` LOC / `3` fanout | Low-Medium |
| `db::sql::ddl` | 613 | 1 | 2 | 11 | below `750` LOC / `2` fanout | Low-Medium |
| `db::sql::ddl::field` | 660 | 2 | 2 | 26 | below `750` LOC / `3` fanout | Low-Medium |
| `db::sql::ddl::index` | 667 | 1 | 2 | 26 | below `750` LOC / `2` fanout | Low-Medium |

Metric artifact: `docs/audits/reports/2026-05/2026-05-30/artifacts/module-structure-2/runtime-metrics.tsv`.

## 3. Structural Risk Index

| Category | Risk Index | Basis |
| ---- | ----: | ---- |
| Public Surface Discipline | 4 | no material public API widening; DDL DTO surface remains private-field/accessor based |
| Layer Directionality | 3 | invariant checks still show no tracked upward imports or cross-layer policy re-derivations |
| Circularity Safety | 2 | no new subsystem-level cycles introduced by the splits |
| Visibility Hygiene | 3 | large private DTO/helper clusters moved behind family modules with root dispatch only |
| Facade Containment | 4 | broad `db` facade and hidden macro support remain intentional |

### Overall Structural Risk Index

**3/10**

Structural pressure is now low-to-moderate. The remaining medium risk is concentrated in `schema::mutation` root and publication runner ownership, which are still catalog-native and now protected by hub thresholds.

## 4. Verification Readout

- `PASS`: `icydb-core` compile check
- `PASS`: full invariant suite, including module-structure hub thresholds
- `PASS`: whitespace diff check
- `PASS`: module-structure metrics artifact generated

## Follow-Up Actions

- Keep future SQL DDL additions family-owned under `sql::ddl::field` or `sql::ddl::index`; add new families only when a distinct DDL ownership boundary appears.
- Keep future schema mutation admission/accepted-after helpers out of `schema::mutation` root unless they are shared dispatch contracts.
- Tighten hub thresholds after the next landed slice if measured LOC/fanout drops further.
