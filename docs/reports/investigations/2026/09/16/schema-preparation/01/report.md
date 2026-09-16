# Whole-schema preparation instruction qualification

2026-09-16 · 0.257 · production unchanged in this measurement slice.

## Decision

Next, pass the existing accepted `Rc<SchemaInfo>` into scalar preparation instead
of deep-cloning its contents. This removes demonstrated repeated work without
eagerly retaining nested types, adding a cache or adding per-query counters.
That implementation is a separate handoff; no production ownership changes are
made here. Preserve accepted-root lifetime/isolation, current authority and the
existing expression-index reconstruction case when changing the handoff.

The 4-MiB query-cache retention cap does **not** bound cold SchemaInfo construction
or the temporary schema clone. A valid 19,881-byte bundle fixture produces a
31,242,782-byte conservative schema retention weight. Cold construction costs
778,474,277 instructions; four deep clone/drop pairs cost 786,228,439. This
confirms that per-kind expansion admission is not cumulative schema admission.
Cold aggregate construction remains open after removing warm copies; do not
describe the new expansion check as complete preparation admission.

## Source findings

- `session/query/cache.rs::schema_info_for_plan_cache_authority` obtains a
  borrowed accepted schema from EntityAuthority, then returns `schema_info.clone()`.
- EntityAuthority already retains `Option<Rc<SchemaInfo>>`; the immutable owner
  exists. `PreparedScalarPlanningState` currently takes an owned SchemaInfo and
  exposes a shared borrow, not a mutable schema interface.
- The filterless cache fast path can return before this copy. Filtered paths
  obtain the copied schema before normalizing/deriving identity and looking up
  their cached plan. The copy precedes their `PreparationWork::run` interval.
  Authenticated route-pin preparation also calls this helper inside its interval.
- SchemaInfo shares field labels, query-kind handles, catalog handles and primary
  key names, but clones the fields vector, FieldType trees, nested descriptors,
  index metadata and other owned members. This is not an entirely deep copy.
- Cold construction creates a query-kind tree and FieldType per direct field.
  Nested record types stay opaque; their leaves are projected only when needed.
  Do not undo that distinction by eagerly expanding every nested leaf.

## Method and boundaries

An isolated source copy adds [probe.rs.txt](probe.rs.txt) beneath `schema::info`.
It invokes production candidate preparation/reload, SchemaInfo construction,
derived Clone and the RetainedBytes visitor. There is no surrogate expansion or
retention algorithm. [runner.rs.txt](runner.rs.txt) makes 57 IC queries: 19 fixtures,
three repeats each, asserting identical returned values and instruction counts.
All pass. [samples.csv](samples.csv) contains every result.

Fixture assembly and initial bundle validation occur outside the intervals.
`admission` measures CandidateSchemaRevision::new on an already built bundle;
it includes validation, encoding, identity and root preparation, not just the
new expansion gate. `reload` measures reconstruction from copied encoded bytes.
`cold` measures SchemaInfo::from_accepted_snapshot_and_catalog. `clone` and `drop`
are separate first-copy intervals; `four_clones` includes four copies and their
destruction. Allocator state can affect a first copy; use the repeated-pair column
for comparison. `four_rc_clones` keeps the original shared owner alive, so it
does not include final schema destruction. Inputs/results are black-boxed and
cloned types/retained shared labels are checked for parity.

Fixtures have one entity, a scalar primary key, no indexes, no defaults, and
either scalar fields, shared map-newtype definitions, or record leaves using
those newtypes. All measured bundles pass candidate and persisted admission.
The maximum fixture has 254 data fields plus the primary key: its 255 not-null
constraints plus the primary-key constraint fill the 256-constraint limit.
The long scalar names are 125 bytes, within the proposal-name limit.

Catalog handles are constructed for this isolated metadata probe; there is no
real session, row access, visibility refresh or full accepted-runtime startup.
The cache helper and complete queries are **not** timed by this probe. The Rc
column is an ownership-operation comparison, not an implemented query speedup.
Neither the candidate admission cost nor cold setup is a newly measured
regression: there is no pre-change baseline in this experiment.

## Results: IC instructions

Fields below exclude the scalar primary key. Newtype depth 8 expands to 511
type nodes per direct field; depth 10 expands to 2,047 and stays below the new
per-kind admission ceiling. Nested fixtures have 16 such leaves per record.

| Fixture | Candidate admission | Reload | Cold schema | Four clone/drop pairs | Four Rc pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 scalar fields | 1,877,002 | 1,101,407 | 75,388 | 66,456 | 533 |
| 64 scalar fields | 14,204,685 | 7,037,194 | 373,716 | 231,192 | 533 |
| 254 scalar fields | 135,767,982 | 61,879,149 | 2,204,275 | 883,272 | 533 |
| 16 direct fields, depth 8 | 16,065,470 | 8,176,460 | 12,275,066 | 12,384,140 | 533 |
| 64 direct fields, depth 10 | 235,556,807 | 117,664,007 | 195,979,444 | 198,113,371 | 533 |
| 254 direct fields, depth 10 | 1,012,681,812 | 500,203,728 | 778,474,277 | 786,228,439 | 533 |
| 16 records, depth 8 | 231,821,298 | 116,465,213 | 285,388 | 1,481,676 | 533 |
| 8 records, depth 10 | 476,657,730 | 238,608,229 | 140,589 | 746,700 | 533 |

Nested schemas demonstrate why eager materialization is not the next step:
admission visits leaf projections, but cold metadata construction need not retain
their expanded trees. The scalar and nested cases already show avoidable warm
copying without relying on extreme expanded direct fields.

Retention weights include shared allocations per reference and are **not** exact
live heap or clone-allocation measurements. The 64-field/depth-10 schema has
weight 7,884,942 bytes; the 254-field/depth-10 schema has weight 31,242,782. Both
decline the standalone 4-MiB visitor. Complete cache-artifact retention includes
more owners and may decline earlier; no complete-artifact admission claim follows.

## Reproduction and footprint

Rust 1.98.1, workspace wasm-release profile (opt-level=z, fat LTO, one codegen
unit, panic=abort), SQL disabled, no wasm-opt. Probe raw Wasm: **463,023 bytes**,
**1,213 defined functions**, four imports. This is a standalone probe, not a
production Wasm delta. Native timing and cycle estimates are not used.

Copy current Cargo manifests/lockfile, crates, canisters, schema, testing and
toolchain to an isolated directory. Install the probe as
`crates/icydb-core/src/db/schema/info/qualification.rs`, declaring
`mod qualification;` in info.rs. Build core alone with `cargo rustc --locked
--offline -p icydb-core --lib --no-default-features --profile wasm-release
--target wasm32-unknown-unknown --crate-type cdylib`; copy the output to probe.wasm.
Compile the runner against cached PocketIC 16.0.0 dependencies and pass that
artifact directory, with POCKET_IC_BIN selecting server 16.0.0. Raw IC exports
have a probe-local unsafe-code allowance; production lint policy is unchanged.
Source paths in panic metadata may affect exact reproduced Wasm bytes.

The initial build needed production constructor/import corrections. An initial
255-data-field fixture trapped because it exceeded the constraint-count limit;
it is excluded from results. The corrected runner exits successfully. Two
isolated PocketIC instances were started; the runner releases its instance on
exit and the local server exits automatically. No shared ICP network changed.
Full/native suites were not rerun: production and maintained tests are unchanged.

HEAD: `38d4067a04a2dc11421fc91fa06fa077d9732840`, with earlier dirty work preserved.
Original directory: `/tmp/icydb-schema-qualification.EI12Ep`.

SHA-256:

- info.rs: `d4510c9fdfe830efa1cda5d90f7cfc66c65e786455196092d4697518034baf48`
- query_projection.rs: `5c3827b00547b896a105001cdfc60f230e8f3abdac213b24bab09721f5f7d194`
- publication.rs: `b7c70626156799928febac566a4b68f42153747fcc4707748ddf545c234e14f3`
- cache.rs: `23f0c3f82223d5d963a6c6df78282271eb9853d3238054d510b8f516513a5608`
- pipeline.rs: `fd4e706f2381a199bdc9c1b2ef409b240837fe1ff53093f8a6666bdb53580afc`
- Probe: `2f6ae2769f5d4228c56c07202ef58d4dcb4584226418b8df73b5c8d1a0d51a93`
- Runner: `3227f94ecaa6e0a365e0233f870dfafed9a157c6aa985ecf5ea93615a94bf064`
- Samples: `26c0c99adad4dada334b1eae400826f7af3bbe570a3b1c6d42351b0b2c027d90`
- Wasm: `2998740f17a89499aff4f3bf4ec95f6b1b8dc5e339e14ed4764aa865df31f05c`

Seven evidence/tracker/changelog files, approximately 440 added lines; zero
production changes and no runtime complexity increase in this handoff.
