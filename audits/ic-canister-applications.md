# IC canister application audit addendum

Use this with a selected [shared audit method](README.md). It supplies application
boundary questions, not product policy or a new automatic gate. Consumers own
deployed service identities, storage, lifecycle and network/version limits.

## Public service and lifecycle surface

Inventory all callable exports and their payload formats, including IC Wire
method contracts, required Candid services and raw entrypoints. Include
update/query/composite-query methods, timers, init/upgrade hooks and generated
bindings/codecs alongside Rust visibility; a `.did` file alone is not the inventory.
Rust `pub` is not the complete external boundary. Record endpoint names, caller
classes, authorization, mutation/effect authority and retained-state obligations.

Apply the [IC application encoding policy](../DRAGGINZGAME.md#ic-application-encoding).
Trace each Candid dependency, derive and codec helper to its remaining required
boundary. Include public, third-party callers when checking owned application
endpoints for Wire adoption; caller location is not a Candid requirement. Check
actual frames and generated bindings, including unit requests and lifecycle
arguments; explicit codec hooks alone do not establish conformance. Identify
internal derives/helpers with no remaining codec caller. Keep independently
defined Candid interfaces, migration gaps and retained checkpoint bytes explicit;
adopting a codec does not itself qualify transport, authentication or recovery.

Before calling an endpoint unused, trace frontend bindings, other canisters'
method-name calls, ops/CLI scripts, generated clients and known external callers
or published interfaces. No in-repository call site is insufficient evidence for
removal. An unknown consumer is a proof gap. Retirement needs an explicit owner
decision, service/deployment scope and disposition for stored data and external
obligations; an optional package or pre-1.0 version does not erase them.

## Wasm size and installation evidence

Record raw and compressed artifact bytes, code-section bytes, defined and
imported function counts separately, and the exact artifact digest. Reuse the
owning Wasm inspector; do not recreate its parser in shell. Inspection facts are
distinct from installation admission. Source the applicable limits and semantics
from the selected network/runtime version and its consumer qualification; record
that source and date. Do not copy a limit from an older audit or treat a local
tool's constant as proof of deployed network policy.

Compare before/after artifacts with the same compiler, lockfile, target, features,
link options and optimizer. Attribute changes through focused feature/endpoint
ablations or the owning size tools, and retain both artifacts and measurements.
Source LOC is not Wasm size. Link-time elimination can already remove unused
private functions; deleting them is hygiene unless measured artifacts shrink.
Code-section and total bytes, function counts and compressed transport sizes
answer different questions and must not substitute for one another.

An evidenced install-limit rejection that prevents a required deployment can be
HIGH availability impact. Proximity to a qualified limit is a risk requiring
measured headroom and an owner, not automatic proof of an outage. Distinguish
current rejection, forecast growth and an unqualified policy assumption using
the common severity scale. Gate selection and thresholds stay consumer-owned.

## Structural and lint evidence

For large-file triage, split maintained runtime code from inline test modules,
external tests and generated code before attributing production complexity.
Report the classification method and its limitations. Test volume may deserve
its own review but must not inflate runtime ownership or size claims.

When relying on a repository lint, inspect its actual resolution boundary.
Probe cross-file calls, reexports and aliases relevant to the claimed rule; a
same-file call search cannot prove whole-service write batching or authorization.
Tool/parser failures must surface as failures or explicit gaps, not empty PASS
results. Avoid building a new whole-program analyzer merely for the audit.

A machine-consumed allowlist can record an approved scoped exception with its
reason, owner, exact identity and linked evidence/issue. It is not a parallel
backlog: unresolved `unfixed` records belong in the owning GitHub issue. Historical
reports and frozen exceptions remain evidence; current triage and adoption status
stay on GitHub under the [baseline](../DRAGGINZGAME.md#feedback-and-handoff).
