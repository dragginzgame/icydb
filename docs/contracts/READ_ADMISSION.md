# Read Admission

This contract defines IcyDB's maintained read lanes after the 0.213 hard cut.
Accepted schema, the planner result, and the built-in admission policy are the
only query-shape admission authorities. Generated models and application
callbacks never replace that admission decision. A generated SQL or schema
guard may authorize its caller before admission begins, as described below.

## Core Rule

Ordinary caller-facing reads use `PublicRead`. The admission owner evaluates
the selected plan before row execution and rejects unsafe shapes with a typed
`QueryReadAdmissionCode`. Rejection is never converted into an empty result.

The default policy has these frozen ceilings:

- maximum returned rows: 100;
- primary-key predicate input: 1024 terms and 64 KiB;
- grouped engine budget: 100 groups, 64 KiB per group, and 1024 distinct
  entries.

`PublicRead` also rejects unbounded full scans and materialized ordering. A
caller-supplied `LIMIT` does not prove safe access by itself: the selected
route must be bounded/index-backed.

`DiagnosticExplain` observes planning but cannot execute rows.

Trusted bypass surfaces are explicit method choices. They retain accepted
schema, planning, execution, and result-shape validation, but application code
owns authorization and the resource policy.

## Query Input Safety Limits

Typed/dynamic query input and SQL syntax admission share these ceilings:

- maximum authored expression/value depth: 128 levels;
- maximum counted input nodes: 4,096 across the request's components;
- maximum counted variable payload: 2 MiB across those components.

These preparation safety limits also apply to trusted reads; choosing a trusted
lane does not bypass them. They are separate from the public row/access policy
above and from resource charges for work performed during preparation.

Node accounting includes expression/value nodes and authored names, not just
predicates. Payload accounting includes names, strings, blobs and big-integer
magnitude bytes. SQL admission also accounts for effective parameter copies.
Depth measures nesting, not the number of siblings in a flat `AND`, `OR` or
`IN`; a wide flat query can instead reach the node or payload ceiling.

These are input-content limits, not a complete bound on heap allocation,
rendering, SQL parsing or endpoint deserialization. Endpoint authorization and
decoder limits remain the application's responsibility.

Typed/dynamic ORDER BY and aggregate operand copies also consume the current
request's preparation work and temporary-byte budgets. This applies before
cache lookup on both cold and warm calls, including trusted reads. Exhaustion
rejects the call; retrying within the same request does not reset its charges.
Copying preserves authored syntax and typed values rather than normalizing them.
Scalar ORDER BY and selected-field vectors charge destination backing before
allocation; selected names and implicit accepted-primary-key ordering are also
charged. Clauses retain authored order and duplicate projections. These charges
do not establish complete bounds for downstream planning.

General scalar preparation also charges its instruction interval for predicate
normalization, parameter-contract derivation and predicate fingerprinting before
shared-plan lookup. Filtered cache hits repeat this work and can reject for
instruction exhaustion without changing the retained plan. Existing filterless
fast hits skip this interval. Completion accounting covers successful and failed
preparation; it does not establish an intra-operation allocation or stack bound.

Schema-aware predicate materialization additionally charges structural/value
visits and admits child containers, boxes, field names, coercion parameters and
operand copies before their construction. Enum input lifting and accepted output
use a conservative source-shaped backing allowance; canonical enum values keep
the existing accepted validation and admitted copy path. Numeric conversion
admits possible fixed-width bigint output. These charges apply on filtered warm
calls, cursor-pinned preparation and SQL binding validation without creating a
new request budget. Exhaustion remains a resource error, not a literal error.
Every supplied child is normalized before boolean simplification may discard it.
Schema metadata derivation, sorting/comparison scratch and boolean normalization
remain separate; this does not establish complete normalization bounds.

When accepted-index predicates are prepared, malformed persisted SQL returns
typed `StoreCorruption`, not a successful false predicate or missing index.
Planner-visible index construction and its session/cache consumers propagate
this failure before returning a newly prepared plan. Mutation planning, integrity
inspection and staged index preparation use the same parse/normalization owner.
This does not make boolean normalization budgeted, add a cache revalidation pass
or change the meaning of absent predicates and valid false membership.

Nullable unique-index snapshot acceptance checks authored predicate field names
before boolean simplification, matching SQL DDL's validation ordering. Unknown
fields reject even in branches normalization would discard. Snapshot acceptance
uses its existing predicate rejection; invalid persisted input remains store
corruption. This is field-binding validation, not new schema/replay resource
admission or a change to nullable guard inference.

Journal `SchemaPut` preflight now retains its validated, encoded storage payload
and fingerprint through application. Replay/fold application consumes that
prepared result and does not repeat snapshot decoding or semantic construction.
The whole batch still finishes preflight before its first effect; snapshot
payloads remain retained until application. Current journal/schema limits are
unchanged. Recovery verification and other control-record kinds remain separate;
this is not a new recovery budget or complete replay-admission qualification.

Accepted-catalog publication also retains the batch-validated candidate through
application. Canonical fold preparation retains encoded snapshots, identity
updates and retention keys for consumption in the same atomic callback. Root
conflict checks and independent stored-bundle verification still run against
storage; those checks are not replaced with trust in the prepared value. Replay
still uses the existing live/durable publication owners. This removes specific
discard-and-rebuild work, not all construction during publication or verification,
and does not enable shared-normalizer resource rejection in those paths.

Candidate constructors own immutable bundle/root integrity. Root publication
checks current stored roots and expected/next revisions without re-hashing or
decoding that already-admitted candidate. Persisted input still passes through
the verifying constructor; bytes read back from storage are independently
verified. No resource admission or storage-corruption check is removed.

Shared-plan key construction, lookup comparisons and warm lifecycle validation
also charge instructions before returning a cached plan or compiling/rebinding.
This includes filterless early hits. Hits still skip compilation, and exhaustion
does not replace shared cache entries. These intervals are separate from the
normalization and compilation intervals.

Cache-key names, aliases, expression/container backing and the shared payload
charge existing construction resources before allocation. Only complete keys
enter the memo; failed construction can retry under a fresh request. Reusing an
immutable key skips those copies, but a bound-plan hit still constructs its
parameterized key. Value-hash errors propagate instead of becoming cache keys.
Parameter-contract derivation charges predicate/list visits and admits child
backing, field names and coercion parameters before allocation. Existing template
slot/payload limits stop traversal before further metadata copying; they remain
cache eligibility limits, not new query rejection limits. Ineligible construction
still consumes the work already performed. Exhaustion propagates on cold and warm
calls without publishing or replacing a plan. Literal payloads are not copied into
contracts. Payload-size estimation and insertion accounting remain separately
owned work; this is not full preparation qualification.

Normalized predicate cache fingerprints admit their output buffer before using
the shared encoder. The sizing walk charges predicate/nested-value visits and
reserves conservative byte-work, case-folding and map-reference allowances.
It performs no encoding, sorting or coercion itself. Admission can exceed actual
allocation, including map scratch for already-ordered input. Exhaustion returns
no digest and cannot publish/replace a cache entry. Successful fingerprint bytes
are unchanged.

Predicate-only continuation identity also admits its input copy through the
shared construction owner, then reserves final raw encoding output and possible
coercion/map/membership scratch. Raw membership allows its ordered view even
when canonical input will not allocate one. Preparation and execution supply
their existing cumulative authority, including DISTINCT's rewritten scalar
handoff; no fresh budget is created. Exhaustion returns no continuation contract
and does not replace the source query or publish a placeholder identity.
Expression-owned and absent filters do not enter this predicate-copy path.
Successful continuation bytes remain unchanged. Boolean normalization between
copying and final encoding, nested payload comparisons and other identity
sections remain separately owned work, not a complete hashing-bounds claim.

Grouped key and aggregate destination vectors also charge backing before
allocation. Group keys select the existing direct/path representation up front;
that name scan charges preparation work. Authored key count determines reserved
capacity, even when duplicate keys collapse. Typed/dynamic preparation pays
these costs before shared-plan lookup; a reusable concrete SQL command still
skips completed SQL lowering. Key-payload copying, duplicate comparisons and
downstream rebinding remain separate, incomplete accounting work.

Query raw index bounds charge both destination buffers and a byte-filling work
allowance before construction, including first materialization of deferred
prefix bounds during execution. The caller's existing preparation or execution
budget owns these charges; warm materialized bounds skip construction charges.
Admission failure leaves a deferred memo empty and remains a typed budget error,
including during advisory cardinality selection. Bound bytes and inclusivity
are unchanged. Prefix/range scalar operands and exact-count metadata keys also
admit scalar output before encoding. Text escaping and decimal digits reserve
conservative capacity; exhausted requests can reject before otherwise valid
encoding. This does not move stored-component size caps onto comparison literals.
Optional index-predicate literals use the same scalar admission before encoding.
The shared access planner also admits its eligible-index list before filtering:
one visit per visible index and destination backing for every visible contract.
Contracts share their immutable payload, so this charges the list, not a deep
copy of schema metadata. This conservative reservation can reject sooner or
leave spare capacity when filtering discards indexes. Rebinding, reranking,
cardinality selection and verbose explain use the same current request.
Construction exhaustion propagates; it is not an unsupported candidate or
unavailable-cardinality fallback, and failed projection leaves the previous
snapshot unchanged. This does not bound predicate implication checks, scoring,
or candidate-plan payloads. Indexed candidate projection separately admits
maximum backing for its candidate, alternative and rejection lists, then charges
each retained name copy. Cardinality tie-set enumeration admits its destination
list before retaining routes. These use visible-index counts without a second
sizing walk; unused capacity and earlier conservative rejection are possible.
Ranking reasons retain fixed-size evidence rather than a score list, and selected
identity borrows shared contracts. Copies of previously frozen snapshots,
candidate-route contents and proof/scoring work are not qualified by these
list/name checks.
Recursive candidate planning additionally charges each predicate dispatch and
admits AND/OR child-list backing before candidate extraction or child recursion.
Both reserve one slot per child; the conservative allowance includes children
later filtered out. AND-family inspection borrows its candidate and selection
consumes the winner, without cloned family operands or fallback append slots.
The AND index-range path box is admitted before child recursion; its underlying
bound construction remains a separate owner. Ordering preference is computed
once without changing family precedence or diagnostic reasons.
Exhaustion propagates through ordinary planning and alternative evaluation;
it is not semantic absence. These checks cover child-list backing and dispatch,
not operand payloads, redundancy proofs, family scoring or normalization work.
AND equality-prefix construction separately admits its literal-cache backing,
outer visits and selected path box. Its shared builder reserves index-arity
output backing after the first matched slot and admits key/literal visits,
expression conversion and retained value copies. Raw duplicates stay borrowed
until one value per slot is retained; the last equal representation is preserved.
Ordinary AND planning and exact-count branch proofs pass the current request to
this same fallible owner. Gaps and conflicts retain their existing semantics;
exhaustion is not candidate absence. Branch collection separately admits its
literal-cache growth and per-set backing before schema-compatibility caching.
Branch construction admits candidate/literal visits, output-list backing,
conversion/copies and the retained path. Exclusions are lowered once per
candidate and applied to the retained list; unchanged values stay borrowed.
Each exclusion set admits a conservative batch of branch visits before pruning,
not payload-comparison work; early removals can leave unused admitted work.
Singleton output moves into the fixed prefix, admitting growth if required.
Ordinary planning and count-only proofs share this owner and preserve their
existing distinct branch caps. Canonical sorting, payload comparisons, schema
validation internals and other candidate payloads are not qualified here.
AND range extraction separately admits comparison-cache growth, candidate visits,
an upfront key/compare walk allowance, prefix/slot storage and operand construction.
Raw duplicate equalities and starts-with inputs are borrowed where no retained
copy is needed. Range merges consume admitted values without copying prior bounds;
prefix output uses the existing strict or expression lower-only allowance.
Primary-key ranges admit their child walk, then copy bounds and admit their path
only after validating the complete interval. Secondary path-box admission remains
at the existing AND handoff. Early gaps and rejection may leave unused walk or
container capacity; field/type checks, payload comparisons and scoring internals
are not bounded by these construction checks. Limits and range semantics do not
change, but newly charged work can exhaust the current request.
Primary-key child reduction admits both child inspection passes up front,
reference-list backing and visits, and a conservative linear intersection-walk
allowance. It canonicalizes borrowed views and retains surviving references in
place; only the final owned keys, output list when needed, and path box are
copied/allocated under the current budget. Explicit empty children retain their
highest-priority reason without constructing an additional losing key candidate.
The prepared child candidate then enters the existing pure family selector.
Sorting and payload comparison remain outside these construction checks; no
input or session borrow escapes into the returned access path.
Exact secondary-index intersection construction admits candidate/suffix-slot/
identity visits as one conservative batch after the outer shape/order gate.
It selects at most three borrowed prefixes in fixed-size scratch before copying
anything. Fewer than two distinct eligible prefixes produce no allocation; valid
candidates admit one child list, each retained prefix's backing and copied values,
and its path box. Two or three nonempty flat prefix paths need no second list
flattening pass. The existing cap, selected-first order, physical index identity
and runtime metadata fallback remain unchanged. Early rejection or reaching the
cap may leave unused admitted visits. Field-name comparison and order/schema
validation internals are not qualified by these structural checks.
Selected-access child-redundancy proofs admit equality-prefix slot visits and
conservative literal/membership visits before checking them. Scalar lookup proofs
borrow identity values without constructing a list. IN/NOT IN proofs admit one
lookup-view list and any expression conversion through existing shared helpers;
unchanged values stay borrowed. Ordering and duplicate removal are unnecessary
for membership and are not performed. The filtered-index implication owner
receives the original child predicate, with no copied predicate shell. Proof
decisions and unsupported-input behavior are unchanged; current-budget exhaustion
propagates instead of silently treating a failed proof as absence. Payload
comparison, schema lookup and filtered implication traversal remain separately
owned work, not bounded by these construction checks. No new limit is introduced.
Access-choice AND equality-prefix evaluation admits constraint-list backing and
child visits before collection, then a conservative index-slot/constraint walk
before matching. Raw operands stay borrowed; expression output uses existing
lowercase admission. Only a prefix score survives, with no owned operand list.
Branch-set evaluation consumes that same equality result before its separately
owned branch-value checks. The evaluator propagates construction failures through
chosen-score lookup, explain projection, residual reranking and cardinality ties.
A semantic rejection can still use the established score hint; exhaustion cannot.
Reserving for every child/key slot can admit unused capacity/work after filtering
or early gaps. Comparison payloads, schema lookup, branch-value construction and
other evaluator families are not qualified by this equality-construction gate.
Access-choice branch-value scoring separately admits its child walk, lookup-view
list backing and a conservative literal/equal-set walk before construction.
Canonicalization borrows raw operands; expression outputs use shared conversion
admission. No owned operand list escapes the scorer. Every matching IN clause is
checked before applying the existing distinct-branch cap, preserving later error
precedence even when an earlier set is oversized or singleton. Exhaustion remains
an error through the evaluator's existing fallible callers. Sorting, payload
comparison and schema lookup internals are outside these structural allowances;
this does not qualify all branch evaluation as bounded or raise a limit.
Access-choice AND range evaluation admits child visits before validating the
original predicate slice; it does not allocate a comparison-reference list.
Whole-input validation still precedes per-key classification, preserving later
unsupported-clause rejection over earlier key conflicts. A conservative
key-slot/comparison batch precedes scoring. Equality conflict scratch borrows raw
operands and admits expression conversion; only equality/range flags and bound
strength leave classification. Construction failures propagate separately from
semantic rejection. Early gaps may leave unused admitted visits; payload
comparison and schema internals remain outside these structural checks. Single
comparison eligibility and ordinary score/cursor semantics are unchanged.
Primary-key equality/IN construction separately admits operand copies, the IN
destination and the path box before each allocation. The existing literal gate
runs first, including every IN slot; unsupported literals remain absence while
construction exhaustion propagates. Comparison selection consumes the completed
route without another copy. Key types, ordering and deduplication are unchanged.
This does not qualify semantic validation, secondary-index operands or later
normalization as fully budgeted.
Single-comparison secondary equality/IN planning admits outer candidate visits
and the winning value-list/path backing. Candidate ranking borrows index
identities and converts only the winner's literals; IN no longer retains a
compatibility list or converted values for losing indexes. Scalar copies and
expression conversion for the selected equality/IN operands now use the shared
copy helper and existing lowercase allowance. Unchanged values are copied once;
derived results move into the destination. Lowercase capacity/work reservations
are conservative and can reject earlier than exact-output charging. AND/range
payloads and score/proof internals remain separate work. Failure remains a typed
construction error, not missing candidate evidence.
Single ordered comparisons likewise rank borrowed identities before copying or
converting the winning operand. Existing admission covers outer candidate visits,
the selected bound, range-slot backing and the retained path. All four operators
keep their original inclusive/exclusive bounds and ranking. Single starts-with
comparisons admit candidate visits and build one shared prefix/bound pair before
ranking by its actual bound count. For one predicate, the accepted coercion gate
admits either raw keys or LOWER keys, not mixed conversion semantics. Existing
copy/lowercase, semantic-prefix, slot-list and path-box admission applies before
construction. Empty or unsupported prefixes stay absent without operand copies;
exhaustion propagates. AND-family bound construction, scoring/proofs and
normalization remain separate
owners; these checks are not a complete range-planning boundedness verdict.
Unsupported compilation remains successful absence, while construction failures
propagate without publishing a completed execution-preparation resident or
changing its retained weight. Warm same-policy results skip compilation; this
does not exempt any copies they still perform. Scalar load explain uses
capabilities without building an unused program; aggregate/grouped route
preparation still compiles where route selection depends on the result.
Accepted validation, expression conversion, semantic prefix-bound output,
program containers and remaining copies are separately owned work, not fully
bounded by scalar-output admission. Prefix successor construction walks borrowed
UTF-8 without a character-vector scratch allocation. Optional predicate
compilation separately admits its semantic prefix strings and scan/copy work
before construction. For a nonempty strict prefix of `n` UTF-8 bytes, the shared
owner charges `2n + 1` temporary bytes and `3n + 1` byte-work units: successor
width grows by at most one byte. Lower-only construction needs `n` of each;
empty unsupported prefixes need neither. These are conservative construction
allowances, not measured IC instructions or allocator telemetry. Planner
candidate-bound construction and prior expression conversion are not covered by
this prefix boundary. Optional expression-index predicate compilation separately
admits its supported lowercase conversion before running it. The text owner
provides a conservative pinned-Rust-1.98.1 allowance for cumulative requested
backing and byte-work units, without a sizing scan or a different Unicode codec.
Identity operands and unsupported source/target pairs do not charge conversion.
Planner candidate conversion remains outside this compiler check; no new replay
limit is installed. Toolchain upgrades require requalifying the lowercase
allocation/expansion allowance, not merely preserving output spelling.

## Read Surface Inventory

| Surface | Lane | Contract |
| --- | --- | --- |
| `DbSession::query::<E>()?.execute_live_page(...)` | `PublicRead` | Generated binding and decode around an authenticated bounded live page. |
| `execute_live_page` | `PublicRead` | Entity/field names resolve against accepted schema; built-in bounded admission and explicit continuation apply. |
| `advance_live_page` | `PublicRead` | Advanced adapters execute one bounded public page while IcyDB owns uncommitted continuation validation and explicit post-processing commit. |
| `DbSession::query::<E>()?.execute_exhaustive_page(...)` | `PublicRead` | Generated binding and decode around a revision-strict page; resume requires its complete source proof. |
| `execute_exhaustive_page` | `PublicRead` | Bounded scalar execution plus pre/post comparison of the canonical participating-store proof. |
| `DbSession::query::<E>()?.execute_grouped()` | `PublicRead` | Generated binding selects accepted entity identity; the engine-neutral grouped result remains structural. |
| `execute_public_dynamic_grouped_query` | `PublicRead` | Grouped dynamic execution requires explicit limits and exposes an opaque continuation cursor. |
| `execute_trusted_live_page` | trusted bypass | Explicit maintenance/admin dynamic page. |
| `advance_trusted_live_page` | trusted bypass | The same adapter-oriented bounded step over the explicitly authorized trusted lane. |
| `execute_trusted_exhaustive_page` | trusted bypass | Authorized maintenance page with the same revision-strict proof contract. |
| `execute_trusted_dynamic_grouped_query` | trusted bypass | Explicit grouped maintenance/admin read with caller-owned authorization and explicit engine limits. |
| `execute_trusted_sql_query` | trusted bypass | Trusted/admin SQL; caller-controlled SQL is not public-safe. |
| generated `icydb_query` | trusted bypass | Controller-gated by default, or protected by one declared synchronous application guard, then returns the canonical `SqlQueryResult`. |
| SQL `EXPLAIN` | `DiagnosticExplain` | Observational planning only on its diagnostic route. |

Public scalar and grouped callers may provide only the opaque cursor issued by
the preceding page of the same accepted plan. They cannot provide offsets or
admission-policy controls.

## Which API should I use?

- Known generated row type: `query::<E>()`; runtime adapters are automatic.
- Runtime entity/field names: `DynamicQuery` plus
  `execute_live_page`.
- Framework or generated-adapter traversal: `advance_live_page` or
  `advance_trusted_live_page`; decode each returned page before advancing.
- Complete unchanged-set traversal: typed or dynamic
  `execute_exhaustive_page`, retaining both continuation and proof.
- Grouped typed/dynamic rows: ordered `.group_by(...)` and `.aggregate(...)`
  declarations, explicit `.grouped_limits(...)`, and the grouped terminal.
- Controller/admin maintenance: `execute_trusted_live_page`.
- Authorized SQL tooling: `execute_trusted_sql_query`.
- Planner inspection: trusted SQL `EXPLAIN`.

Ordinary typed and dynamic pages require a safe selected route and return at
most 100 rows. A query `LIMIT`, when supplied, is the total traversal limit,
not a per-page limit. Do not emulate continuation with hidden offsets.

Live pages tolerate source mutations and provide forward keyset progress, not
snapshot completeness. Exhaustive pages compare the complete bounded physical
store proof before and after every page. A non-null continuation means only
that exhaustion has not been proved; completion requires a null continuation
under one unchanged proof.

The `advance_*` methods are page drivers for framework adapters, not
caller-facing collect-all terminals. They return the current page with its
continuation intact, reject a repeated non-null token, and leave caller-owned
state unchanged until the consumer explicitly commits the successfully
projected or decoded step. Commit moves the token into one caller-owned
`Option<String>` or clears that state on exhaustion. Consumers must not commit
a page whose projection or decoding failed.

Grouped calls additionally require positive `max_groups` and
`max_group_bytes` values. Public calls must remain within the frozen ceilings;
trusted calls bypass public admission but not their declared hard limits.
Group keys and aggregates define grouped output, so `.select(...)` is rejected
for grouped execution.

## Generated SQL Query Surface

Generated `icydb_query` remains controller-gated when its declaration omits an
authorization choice. A declaration may instead specify
`authorization = guard(path)`. Guarded mode rejects anonymous callers and
invokes the exact synchronous function once over caller plus `Sql`, before
request-root construction, startup admission, parsing, or dispatch. `Allow`
continues into the generated plain-result trusted SQL dispatcher;
`Deny` returns the typed SQL policy diagnostic. Guard authority replaces
controller authority and never forms an implicit union.

Both declaration forms remain trusted SQL surfaces, not public endpoint
templates. Authorization does not weaken the maintained read-only statement
dispatcher or bypass startup admission.

## Generated Accepted-Schema Surface

`icydb_schema` retains its explicit `authorization = public | controller`
forms and additionally accepts `authorization = guard(path)`. Guarded schema
rejects anonymous callers and invokes the same exact synchronous guard type
once with the `Schema` discriminator, before request-root construction,
startup admission, accepted-schema observation, or handler dispatch. `Deny`
returns the typed schema-policy diagnostic. Guard authority
replaces controller authority and never forms an implicit union.

The dedicated method is not a second spelling for SQL introspection. SQL
`SHOW`, `DESCRIBE`, and `EXPLAIN` remain under the complete `icydb_query` guard,
while the schema guard protects only `icydb_schema`.

## Public Endpoint Guidance

Authorize the caller before entering IcyDB. Use the ordinary typed or dynamic
lane and shape a bounded response; add a small query limit only when the whole
logical traversal needs a smaller cap than the built-in page envelope:

Generated IcyDB endpoints establish the request scope automatically. A manual
IC-CDK, Canic, lifecycle, or timer entry uses
`#[icydb::request_execution]` and keeps using `db!()` in nested helpers. The
same root is installed per poll across async suspension. The explicit root
argument is reserved for low-level framework integration that already owns
the root; it is never shared with the called canister and cannot replace a
different active root.

```rust
let page = db!()?
    .query::<User>()?
    .filter(User::ACTIVE.eq(true))
    .order_by(asc(User::ID))
    .execute_live_page(continuation.as_deref())?;
```

Filtering and ordering must map to accepted indexed access. A public endpoint
must return the opaque continuation and enforce its final encoded-response
budget after IcyDB returns.

See [the read-intent guide](../guides/read-intent.md) for maintained examples.

## Common Rejections And Fixes

| Diagnostic | Meaning | Correction |
| --- | --- | --- |
| `QueryReadAdmissionCode::PublicQueryRequiresLimit` | No proven finite returned-row bound. | Add a positive limit or use exact selected primary-key access. |
| `QueryReadAdmissionCode::PublicQueryRequiresIndex` | The selected route is not index-backed/bounded. | Add or select an accepted index, or move authorized maintenance to a trusted lane. |
| `QueryReadAdmissionCode::UnboundedFullScanRejected` | Planning selected a full entity scan. | Use indexed filtering or an explicit trusted lane. |
| `QueryReadAdmissionCode::SortRequiresMaterialization` | Ordering would materialize rows. | Use accepted index order or a trusted lane with its own budget. |
| `QueryReadAdmissionCode::GroupedQueryRequiresLimits` | Grouped execution lacks hard budgets. | Use a supported surface with explicit group and memory bounds. |
| `QueryReadAdmissionCode::GroupedQueryExceedsBudget` | Group limits exceed the built-in public policy. | Reduce the bounds or use authorized trusted execution. |
| `QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute` | An explain-only lane was asked to execute. | Execute through a row-owning lane. |
| `QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy` | The row bound exceeds 100. | Reduce the public response bound. |
| `QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy` | Primary-key input count or bytes exceed policy. | Split the request or use authorized bounded maintenance. |
| `QueryReadAdmissionCode::InputDepthExceeded` | Authored expression/value nesting exceeds 128 levels. | Simplify nesting; use flat boolean or membership inputs where equivalent. Trusted execution does not bypass this limit. |
| `QueryReadAdmissionCode::InputNodesExceeded` | Counted input nodes across the request exceed 4,096. | Reduce query components or batch inputs with application-owned combination semantics. Trusted execution does not bypass this limit. |
| `QueryReadAdmissionCode::InputBytesExceeded` | Counted variable input payload exceeds 2 MiB. | Reduce literal/name payload or effective repeated bindings. Trusted execution does not bypass this limit. |
| `QueryReadAdmissionCode::ExplainDoesNotAcceptCursor` | Logical explain received a cursor, including an empty string. | Remove the cursor; explain describes the request before pagination rewrites. |

## Regression Guard

`scripts/ci/check-read-admission-invariants.sh` verifies:

- every plan-admission rejection has a public diagnostic counterpart; public
  diagnostics may also describe input rejection before planning;
- every public rejection identifier has documentation in this contract;
- default budgets remain synchronized with this contract;
- typed execution enters the identity-bound
  live or exhaustive page boundary;
- trusted SQL documentation and generated-controller ownership remain intact;
- public reads enter through the maintained typed or dynamic admission
  boundaries.
