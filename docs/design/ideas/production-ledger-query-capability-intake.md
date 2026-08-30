# Production Ledger Query Capability Intake

> # NON-AUTHORITATIVE INTAKE BACKLOG
>
> # NOT A NUMBERED MINOR LINE OR IMPLEMENTATION AUTHORITY
>
> # CURRENT DISPOSITIONS LIVE IN THE QUERY-CAPABILITY ROADMAP
>
> This file preserves the 2026-08-10 production evidence and candidate findings
> from the former proposed 0.225 umbrella. It has no version number, predecessor
> gate, landing tracker, or promotion authority. Do not implement from it.

Status: preserved intake evidence; dispositions superseded; implementation
prohibited

Audit cut: 2026-08-10

Current disposition authority:
[query capability roadmap](query-capability-roadmap.md)

Immediate recovery programme:
[post-0.224 programme disposition](post-0.224-design-programme.md)

## Document Purpose

This document records candidate IcyDB improvements exposed by a production
query audit of an ICRC-1 ledger transaction table. It preserves the findings in
one place so that they can be reviewed, rejected, regrouped, and promoted into
separate design-document slices.

It is deliberately not:

- a numbered minor scope;
- a line tracker or landing-patch plan;
- evidence that any candidate is absent from the current implementation;
- authorization to change SQL, storage, Candid, cursors, or persisted state;
- a promise that all candidates belong in one minor release; or
- permission to fold independent outcomes into one implementation patch.

Some capabilities may already exist partially in the maintained implementation.
In particular, parameterization, prepared execution, covering reads,
deterministic planning, aggregate execution, and resumable reads require a
current-state audit before new design. Historical design documents do not
establish present authority or a product gap; the maintained code, public
contracts, tests, and measured behavior do. Extend the current surface only
when that evidence leaves a concrete production gap.

## Motivating Workload

The audit used a locally indexed, verified transaction-history projection for
ICRC-1 token operations. ICRC-1 defines ledger token operations but does not
itself define a historical block-query API; historical ingestion is expected to
come from the ICRC-3 transaction log and its archives.

A representative entity contains:

```text
Icrc1Transaction {
    block_index: Nat64,              // primary key
    operation: Text,
    ledger_time_ns: Nat64,
    created_at_time_ns: Option<Nat64>,
    from_account: Option<Account>,
    to_account: Option<Account>,
    amount_raw: NatBig,
    fee_raw: NatBig,
    memo: Option<Blob>,
    block_hash: Blob,
    parent_hash: Option<Blob>,
    archive_canister: Option<Principal>,
    ingested_at_ns: Nat64,
}
```

Representative indexes are:

```text
PRIMARY KEY (block_index)
INDEX (from_account, ledger_time_ns, block_index) WHERE from_account IS NOT NULL
INDEX (to_account, ledger_time_ns, block_index) WHERE to_account IS NOT NULL
INDEX (operation, ledger_time_ns, block_index)
INDEX (ledger_time_ns, block_index)
INDEX (amount_raw, block_index)
INDEX (memo, block_index) WHERE memo IS NOT NULL
```

The production query set included exact block lookup, ledger-tip lookup, recent
transfers, account history across sender and recipient roles, memo
reconciliation, large transfers in a time range, daily operation totals,
mint/burn totals, fee monitoring, and top-recipient reports.

This workload is a diagnostic lens, not special-case authority. Accepted schema
snapshots remain runtime authority, and improvements must remain useful for
general IcyDB entities.

### Reference feed application normalized-catalogue evidence

A second production integration audit exercised the reference feed application's normalized
catalogue against 0.222.2, the regressed 0.222.3 release, and 0.223.0. It adds
evidence that the ledger-shaped audit alone could not expose:

- optional components in natural-identity indexes silently omit the complete
  index entry when `NULL`, so those rows receive neither uniqueness enforcement
  nor prefix lookup through that index;
- relation entities provide bounded indexed searches for repeated mechanics,
  but resolving one indexed relation result back to its canonical entity still
  requires application-managed query hops;
- the core planner retains an unknown field name that the public CLI diagnostic
  collapses to generic `E_QUERY_PLAN` prose;
- collection containment is useful as a residual predicate but is not a
  multivalue index access path;
- the public symbolic error code identifies execution-budget exhaustion even
  though downstream documentation did not make that typed classification
  discoverable; and
- accepted enum values are structurally useful but overly verbose in human CLI
  rendering.

The same integration verified useful existing behavior: 0.223.0 fixes the
accepted-enum runtime-corruption regression, all 28 IcyDB-backed reference application store
tests pass, and the intended indexes are selected for normalized HP, mechanic,
set, item, and variant candidate queries. Startup recovery/readiness findings
from this audit belong to the focused 0.225 readiness design and are not
duplicated as query work here.

The following selected-plan evidence is a baseline for future planner and
traversal slices, not a request to add more indexes:

| Query shape | Selected index |
| --- | --- |
| Pokemon HP range | `idx_pokemon_card_metadata__hp_id` |
| Pokemon attack-name prefix | `idx_pokemon_card_attack__name_id` |
| Pokemon ability-name prefix | `idx_pokemon_card_ability__name_id` |
| Magic face-name prefix | `idx_magic_card_face__name_id` |
| Set identity candidates | `idx_set__collection_id_name_id` |
| Items in a set | `idx_item__set_id_id` |
| Item identity candidates | `idx_item__set_id_name_id` |

## Audit Outcome

IcyDB can express useful indexed and bounded parts of these workloads, but the
audits exposed six broad sources of production friction:

1. callers need safer, more reusable query and pagination contracts;
2. physical index selection and its cost are not sufficiently expressive or
   observable for wide historical tables;
3. ledger-shaped values and nanosecond timestamps require avoidable projection
   or application work;
4. ingestion, rollups, large aggregates, and archival transitions need bounded
   durable execution models;
5. nullable index semantics and useful planner details are too easy for
   applications and operators to miss; and
6. normalized relation models lack one deliberately bounded traversal shape.

The candidates below are ordered by dependency and likely general value, not by
approved release priority.

## Candidate 1: Public Parameterized And Prepared SQL

### Production problem

Operators repeatedly query the same statement shape with different accounts,
block ranges, times, operations, and amounts. String construction is less safe,
prevents straightforward plan reuse, and makes canonical request identity
harder to reason about.

### Candidate outcome

Provide one typed public binding contract for scalar, optional, account,
principal, blob, Nat64, and NatBig values. Repeated execution should reuse a
validated compiled shape where current cache and authority invariants permit
it.

### Required audit before design

Inspect the maintained compiled-query cache and current canister, facade, CLI,
generated API, and Candid surfaces. The design must identify a real external
gap rather than recreate internal parameterization or prepared execution under
a new name.

### Non-negotiable boundary

Bindings are typed data, not textual substitution. Schema version, access
policy, cost limits, and accepted authority remain part of validation. No
compatibility spelling or parallel legacy dispatch is added before 1.0.

## Candidate 2: Authenticated, Snapshot-Safe SQL Pagination

### Production problem

Account history and time-window scans routinely exceed a single response. A
caller needs deterministic continuation without being allowed to forge an
index position, change the query under a cursor, or silently cross incompatible
schema/catalog authority.

### Candidate outcome

Return a bounded page plus an opaque authenticated continuation that commits to
the normalized query or prepared identity, bindings, order, accepted schema
identity, access context, and last stable key.

### Required audit before design

Reconcile the proposal with maintained resumable reads and all current cursor
formats. Determine whether the current cursor already supplies the required
integrity and snapshot semantics.

### Non-negotiable boundary

Continuation is keyset-based and bounded. It must not use an unbounded offset,
retain a hidden in-memory result set, or accept a cursor under a different
query, principal, policy, or incompatible accepted schema.

## Candidate 3: Explicit Covering Index Payloads

### Production problem

Queries such as recent account transfers may use an index to find matching
primary keys but still load every base row to return amount, counterparty, fee,
memo, or operation. On a historical ledger table, those row loads can dominate
the request.

### Candidate outcome

Evaluate an `INCLUDE`-style index payload contract so selected projection fields
can be read from the index without changing its ordered key semantics.

Conceptually:

```text
INDEX account_history
    ON (from_account, ledger_time_ns, block_index)
    INCLUDE (to_account, operation, amount_raw, fee_raw, memo)
```

### Required audit before design

Inspect the maintained physical index representation and executor. Establish
which query shapes are already truly covering and which still materialize base
rows.

### Non-negotiable boundary

Included fields are projection payload, not search keys. The design must make
write amplification, stable-memory bytes, rebuild cost, maximum entry size,
and row-layout/schema transition behavior explicit.

## Candidate 4: Statistics-Aware Index Selection

### Production problem

Several plausible indexes can satisfy the same predicate and order. Fixed
heuristics can choose poorly when operation, account, memo, or time-range
selectivity differs sharply between deployments.

### Candidate outcome

Add bounded, persisted planner statistics sufficient to compare admitted plans.
Initial candidates include row count, null fraction, bounded distinct-count
estimates, key-range summaries, and carefully selected prefix statistics.

### Non-negotiable boundary

Statistics are advisory and bounded. Plans remain deterministic for the same
accepted inputs and recorded statistics epoch. Missing or stale statistics must
produce a documented deterministic plan or a typed refusal, never generated
model fallback or an unbounded sampling pass.

## Candidate 5: Index And Plan Observability

### Production problem

Operators need to know whether a production query is covering, how many index
entries and base rows it is expected to touch, why one index won, and what each
index costs to store and maintain.

### Candidate outcome

Evaluate a coherent observability surface containing:

- `SHOW INDEXES FROM <entity> VERBOSE` details for ordered keys, included
  fields, uniqueness, predicates, entry counts, and physical byte estimates;
- `EXPLAIN` evidence for chosen and rejected access paths;
- estimated versus actual entry scans, row loads, result rows, instructions,
  and response bytes where execution statistics are safely available;
- warnings for avoidable base-row materialization, full scans, high-cardinality
  grouping, unusable order, and stale statistics; and
- DDL estimates for storage growth, rebuild work, and steady-state write cost.

### Relationship to 0.224

0.224 owns coherent introspection command shape and compact/verbose behavior.
It must not absorb this entire optimizer-observability program. Promotion must
review which syntax and typed envelopes are already established and add no
second competing introspection model.

### Non-negotiable boundary

The public response is typed and bounded. Renderer prose is not authority, and
observability must not trigger an unbounded count or table scan merely to
describe an index.

## Candidate 6: Account Component Expressions

### Production problem

ICRC accounts are structured owner/subaccount values. Operators commonly need
queries by owner across subaccounts, by an exact subaccount, or grouped by
owner. Requiring callers to maintain every useful denormalized form manually
adds schema and ingestion boilerplate.

### Candidate outcome

Evaluate typed field/component expressions for `Account`, including owner and
canonical subaccount access, and determine whether those expressions may be
indexed or used in accepted generated projections.

### Non-negotiable boundary

Account equality and canonical default-subaccount semantics must be singular
and shared across predicates, grouping, ordering, generated projections, and
indexes. The SQL layer must not invent a representation distinct from stored
and Candid account semantics.

## Candidate 7: Nanosecond Time And Bucket Expressions

### Production problem

Ledger timestamps are Nat64 nanoseconds since the Unix epoch. Daily totals,
hourly fee monitoring, and time-window reports need repeated arithmetic that is
easy to get wrong and awkward to index or explain.

### Candidate outcome

Evaluate typed nanosecond timestamp conversion and deterministic bucket
expressions such as UTC day/hour truncation. Define overflow, boundary, and
invalid-value behavior explicitly.

### Non-negotiable boundary

Time zones and locale do not affect stored bucket identity. Any indexable or
generated expression must have one canonical, versioned semantic definition.

## Candidate 8: Exact NatBig Aggregation Semantics

### Production problem

Token amounts and fees can exceed machine-width integer assumptions. `SUM`,
comparison, ordering, grouping, and result transport must remain exact for
NatBig data while staying bounded against hostile cardinality and result size.

### Candidate outcome

Audit and document exact behavior for NatBig aggregate input, accumulator,
overflow/impossibility, output encoding, ordering, and CLI rendering. Close any
identified gap with typed semantics rather than lossy coercion.

### Non-negotiable boundary

No floating-point conversion and no silent saturation. Accumulator and encoded
result sizes are charged to explicit request limits. Existing trusted group and
response limits remain authoritative until deliberately redesigned.

## Candidate 9: Bounded Append-Only Ingestion Primitive

### Production problem

Ledger synchronization inserts monotonically increasing blocks, often with a
known parent and expected next index. Generic per-row mutation pays avoidable
planning and validation costs, while unchecked bulk insertion risks gaps,
duplicates, partial application, or excessive single-message work.

### Candidate outcome

Evaluate a bounded append primitive or prepared mutation batch that validates a
contiguous block-index range, duplicate policy, parent-chain expectation, and
accepted schema once per admitted batch while retaining normal index and
constraint correctness.

### Non-negotiable boundary

This is an optimization of catalog-native mutation semantics, not a second
storage authority. It must be resumable or strictly request-bounded, idempotent
under a typed batch identity, and safe across traps and upgrades.

## Candidate 10: Incremental Rollups Or Maintained Materializations

### Production problem

Daily volume, operation totals, fee totals, and recipient activity are useful
continuously. Recomputing them over the full transaction history grows with all
rows even when only a small suffix changed.

### Candidate outcome

Evaluate accepted, incrementally maintained summaries for a deliberately small
class of deterministic aggregates. The design must decide whether these are
catalog entities maintained by explicit ingestion, database-maintained
materializations, or another singular authority.

### Non-negotiable boundary

There is one replay and repair model. Updates are atomic with their source
mutation or carry a durable typed lag/checkpoint state. Arbitrary SQL
materialized views are out of scope unless a later design proves bounded
maintenance, schema evolution, and recovery.

## Candidate 11: Durable Resumable Aggregate Jobs

### Production problem

Historical group-by, reconciliation, and audit queries may exceed one request
even with useful indexes. Raising a request cap does not make the work safe.

### Candidate outcome

Evaluate persisted aggregate jobs that advance through a stable key range over
multiple calls, checkpoint bounded accumulator state, expose typed progress,
and return a final bounded result or a durable result entity.

### Required audit before design

Reuse the maintained durable mutation-job lifecycle and identity principles
where semantics genuinely match. Do not couple read/aggregate execution to a
mutation-specific implementation accidentally.

### Non-negotiable boundary

Jobs are opt-in and typed; an ordinary query must not silently become a durable
job. Cardinality, accumulator bytes, checkpoint bytes, result bytes, expiry,
cancellation, ownership, schema changes, and upgrade recovery are all explicit.

## Candidate 12: Range Partitioning And Archive-Aware History

### Production problem

An indefinitely growing history table increases stable-memory footprint,
upgrade/rebuild work, statistics maintenance, and wide historical query cost.
ICRC ledgers already expose archived block ranges, so local operators may also
need deliberate hot/cold retention boundaries.

### Candidate outcome

Evaluate catalog-native range partitions by block index or ledger time, with
explicit online, archived, detached, or external states and deterministic
pruning during planning.

### Non-negotiable boundary

Partition metadata is accepted catalog authority. A query never silently omits
an unavailable range: it either proves the predicate excludes that range,
routes through an explicitly configured authority, or returns typed
incompleteness. Generated models cannot reconstruct missing partition state.

## Candidate 13: Explicit Nullable Unique-Index Semantics

### Production problem

A unique index containing an optional component omits the whole entry when that
component is `NULL`. The index therefore neither rejects duplicate natural
identities nor supports prefix lookup for those rows. The behavior is coherent
with IcyDB's current non-indexable-null key encoding but is too easy to mistake
for complete natural-identity enforcement. The reference application encountered this with
`release_date`, `collector_number`, and `edition_code` components in `Set`,
`Item`, and `ItemVariant` natural identities.

### Candidate outcome

Audit schema proposal, SQL DDL, accepted snapshots, generated diagnostics, and
documentation, then select one explicit current contract. The preferred
minimal direction is to require nullable unique-index behavior to be explicit,
for example through an admitted non-null partial predicate, and to diagnose an
implicit nullable unique key before publication. Document that a `NULL`
component omits the complete entry and its prefix paths.

### Non-negotiable boundary

Do not add `NULLS NOT DISTINCT`, encoded-null key variants, or a second unique
index representation without separate workload and storage evidence. A warning
alone is insufficient if accepted schema still claims an ambiguous natural
identity, and the design must preserve useful explicitly partial uniqueness.

## Candidate 14: Specific Bounded Public SQL Diagnostics

### Production problem

The core planner can identify an unknown field such as `id` in an `ORDER BY`
term, while the public/CLI boundary may retain only `E_QUERY_PLAN: query
planning failed`. Operators lose the field and clause needed to correct their
query even though neither fact exposes private storage state. The reproduced
query ordered `PokemonCardMetadata` by `hp DESC, id DESC`; its accepted relation
field is `pokemon_card_id`.

### Candidate outcome

Define one bounded typed diagnostic-detail path from parser/lowering/planner
ownership through the facade, Candid response, and CLI renderer. Unknown-field
diagnostics should retain the rejected query-visible field and clause or term
role. The same audit should place human enum shortening and documentation of
symbolic runtime-boundary classification without inventing parallel error
models.

### Non-negotiable boundary

Do not match or transport rendered error strings as authority. User-controlled
text is bounded before it crosses a public response, numeric diagnostic facts
remain typed, and structured clients do not have to parse CLI prose. Avoid one
predicate helper per error code when `Error::code()` and symbolic `ErrorCode`
comparison already provide the classification.

## Candidate 15: Bounded Indexed Relation Traversal

### Production problem

Normalized repeated mechanics are represented as relation entities so their
names and reverse links can be indexed. An indexed mechanic lookup can find the
relation rows cheaply, but reaching the canonical parent entity currently
requires a second application-managed bounded query.

### Candidate outcome

Audit a single-hop indexed semi-join expressed through a small standard SQL
shape such as an admitted `IN` subquery or `EXISTS`. Both sides must have an
accepted indexed access path, intermediate keys and final rows remain bounded,
and `EXPLAIN` identifies both access paths and their limits. Start with one hop;
if the exact reference application mechanic-to-metadata-to-item fixture proves that a second
indexed bridge is necessary, the promoted design may admit exactly that fixed
depth rather than arbitrary nesting.

### Non-negotiable boundary

No Cartesian products, unrestricted joins, recursive or caller-selected depth,
unbounded intermediate materialization, archive-spanning traversal, or
optimizer search explosion. Existing input-term, index-entry, row-load, result,
response-byte, and execution budgets remain authoritative, and pagination
cannot silently change relation snapshot semantics.

## Reference Feed Application Feedback Disposition

| Observed integration behavior | Intake action |
| --- | --- |
| 0.222.3 accepted-enum runtime corruption is fixed in 0.223.0 | Retain as predecessor and downstream regression evidence; do not create another feature slice |
| Populated recovery is bounded but readiness is implicit | Route to 0.225 explicit readiness and replicated-driver acceptance evidence |
| Nullable natural-identity index omits `NULL` rows | Candidate 13 current-state audit and focused schema/index-safety design |
| Unknown planner field collapses at the CLI boundary | Candidate 14 public typed-diagnostic design |
| Indexed relation results require manual parent lookup | Candidate 15 bounded fixed-depth traversal design |
| `COLLECTION_CONTAINS` is residual, and maps are not predicate-queryable | Add explicit public query/index documentation; multivalue indexes remain deferred |
| Execution-budget code 273 leaks into application protocol knowledge | Document symbolic `ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED`; add no per-code helper unless a later API audit disproves sufficiency |
| Enum cells render the full accepted Rust path | Consider CLI-only human shortening in Candidate 14; structured output remains canonical |
| the reference application's intended normalized indexes are selected | Freeze minimal representative `EXPLAIN` fixtures for promoted planner/traversal slices; do not import the complete application schema |

## Secondary Candidates To Place During Review

The audit also exposed smaller or cross-cutting candidates. Their current
placement is owned by the query-capability roadmap:

- fixed-length blob declarations for block hashes, parent hashes, subaccounts,
  and other domain values where length is a schema invariant;
- predicate and included-field detail in verbose index introspection;
- physical storage and write-amplification estimates during index DDL review;
- `EXPLAIN` comparison of estimated versus actual entry scans and row loads;
- typed planner warnings for scans, materialization, sort, and group risks;
- accepted generated/stored projections for canonical owner, subaccount, or
  time-bucket fields when expression indexes are not the chosen model;
- explicit documentation that collection containment is residual and cannot
  independently satisfy indexed public-read admission;
- concise human enum rendering that does not change structured values; and
- unique secondary-index DDL where uniqueness is accepted catalog authority
  and not already coherently supported across proposal, validation, mutation,
  and execution.

Fixed-length blobs remain deferred pending a separate schema-type audit; they
are not part of the expression line. Generated projections remain an
unnumbered expression-design question; the historical 0.237 assignment was not
adopted. Physical DDL cost, index bytes, entry shape, covering status, and
base-row avoidance remain historical 0.235 intake scope. Plan comparisons,
estimates, actuals, rejected routes, stale evidence, and warnings were narrowed
by released 0.236. Multivalue indexes and any unique-secondary expansion
remain deferred pending their required audits.

## Superseded Decomposition And Ordering

The intake's original multi-document grouping and dependency sketch are
retired. The exact candidate dispositions, Candidate 5 split, 0.231–0.242
order, dependencies, and promotion questions now live only in the
[query-capability roadmap](query-capability-roadmap.md). Every promoted minor
still requires its own substantive landing-patch tracker; this intake cannot
satisfy that requirement.

## Cross-Cutting Invariants

Any promoted design must preserve these project-wide rules:

1. Accepted schema snapshots and accepted catalog state are runtime authority.
2. Generated models never reconstruct missing runtime authority.
3. SQL DDL remains a frontend to catalog-native mutation semantics.
4. Every request and persisted decode is bounded and fallible.
5. Work exceeding one message is explicit, durable, resumable, and typed.
6. Query ordering and continuation are deterministic under their stated
   authority and snapshot contract.
7. Exact numeric values are not silently narrowed, rounded, or saturated.
8. Public result, Candid, CLI, and diagnostic shapes agree.
9. Pre-1.0 surface changes are hard cuts without aliases, shims, dual dispatch,
   or legacy cursor fallback.
10. Production execution avoids panic paths and string-matched errors.
11. Observability itself cannot perform unbounded work.
12. Physical optimizations report their stable-memory, write, instruction,
    response-size, and raw Wasm consequences.
13. Nullable index membership and uniqueness are explicit in accepted schema
    and operator documentation.
14. Query-visible diagnostic facts survive public transport as bounded typed
    data rather than renderer-only strings.
15. Relation traversal remains one fixed-depth bounded indexed access shape,
    not a general join planner or unbounded intermediate result.

## Review Questions

Before this intake can become approved design authority, review must answer:

1. Which candidates are already fully supported on the maintained public
   surface?
2. Which observed limitations are deliberate safety limits rather than missing
   features?
3. Which two or three capabilities remove the most production boilerplate for
   non-ledger users as well as ledger users?
4. Does authenticated pagination belong to ordinary SQL execution, resumable
   reads, or one shared continuation protocol?
5. Does the current index representation already support true covering payloads
   or expression-derived keys without a persisted-format cut?
6. What is the smallest statistics model that improves real plan choices while
   remaining deterministic and cheap to maintain?
7. Should rollups be explicit user-maintained entities before considering
   database-maintained materializations?
8. Can aggregate jobs reuse a generic durable-job substrate without coupling
   their state machine to mutation semantics?
9. Is partitioning justified after query, index, and job foundations settle?
10. Can nullable unique keys reuse explicit partial-index semantics, or does
    the current surface require a narrower admission rule?
11. What is the smallest typed public detail that preserves unknown field and
    clause identity without widening every error payload?
12. Can one indexed `IN`/`EXISTS` shape satisfy the demonstrated relation hop
    without admitting arbitrary join order or materialization?
13. Which public and persisted hard cuts would each accepted slice require?

## Required Evidence For Promoted Slices

Each resulting design document must define its own frozen baseline and focused
acceptance evidence. Across promoted lines, evidence should report:

- representative exact lookup, range scan, account history, reconciliation,
  aggregate, ingestion, and archive-query instruction counts;
- index entries visited, base rows loaded, groups retained, checkpoint bytes,
  and response bytes where relevant;
- stable-memory bytes per row and per affected index, plus write amplification;
- raw non-gzipped Wasm deltas, with gzip only as secondary context;
- upgrade, trap/retry, schema-change, authorization, and tamper cases for
  durable or authenticated state;
- deterministic plans and output across repeated equivalent executions;
- nullable unique-index admission and lookup behavior for present and `NULL`
  components, including explicitly partial indexes;
- exact public and CLI diagnostics for an unknown field in projection,
  predicate, grouping, and ordering clauses;
- smallest-admitted-depth relation-plan evidence showing every bounded index
  path, intermediate-key count, base-row loads, and budget rejection;
- files touched and approximate line delta per landing patch; and
- whether each implementation shape became simpler, stayed neutral, or became
  more complex.

Measurements must distinguish complexity in terms of total rows `N`, matched
rows `K`, scanned index entries `W`, loaded base rows `B`, retained groups `G`,
and persisted checkpoint bytes `P`. Claims such as "uses an index" are
insufficient without showing which of `W`, `B`, and response materialization
actually contracted.

## Explicit Non-Goals Of This Intake

This document does not approve:

- a ledger-specific SQL dialect or hard-coded ICRC entity;
- balance reconstruction by rescanning complete transaction history;
- unbounded collection scans or aggregates in one request;
- silent automatic conversion of normal SQL into a background job;
- implicit claims that a nullable unique index covers or constrains `NULL`
  rows;
- a general join optimizer or unbounded relation materialization;
- multivalue collection indexes without a separate demonstrated workload and
  storage/write-amplification design;
- arbitrary distributed joins across ledger archives;
- a second schema, partition, or materialization authority;
- best-effort queries that omit unavailable historical ranges;
- compatibility aliases for any eventual SQL or API cut; or
- implementation, changelog, version, Candid, fixture, or release work.

For current balances, deployments should continue to maintain a balance entity
or another explicit accepted projection rather than recomputing balances from
the entire event history on demand.

## Intake Promotion Boundary

No code work begins from this file. Candidate disposition and promotion gates
live only in the current query-capability roadmap and any later focused design.
This preserved intake must not be cited as an accepted product contract,
numbered-line scope, predecessor closeout, or implementation authorization.
