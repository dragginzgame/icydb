# IcyDB 0.308 — Storage adoption

The maintainer authorized this minor after the delivered 0.307 closeout.

## Landing slice

1. Adopt published Storage 0.72.0 in the root graph; qualify runtime opening,
   explicit allocation diagnostics, bootstrap, persistence and facade consumers.
   Preserve unrelated dependency selections and the deferred Canic stack.
   Storage retains its stored format but opening a handle now trusts the admitted
   directory; explicit diagnostics retain integrity verification.

2. Apply the maintainer-selected cleanup findings: share numeric CHECK schema
   fixtures, converge filter integer conversion on strict range-check helpers,
   and remove duplicate allocation snapshots/assertions. Retain separate input
   admission contracts and all behavioral assertions.

3. Apply the next selected cleanup findings: check predicate canonical form
   once, render partial-index SQL directly from accepted metadata with shared
   literal/comparison formatting, and share signed/unsigned width selection.
   Preserve current SQL spelling, error categories and literal admission.

4. Resolve SQL write fields once per admission operation and reuse the
   accepted field for policy checks, literal conversion and patch placement.
5. Prepare RETURNING column selection before mutation and reuse the same
   projection for precommit byte checks and final response shaping, including
   validated DELETE entrypoints.

6. Share SQL cache resident admission across literal commands and parameter
   templates, preserving root authority, retained-size limits and execution on
   admission refusal. Classify INSERT omissions without collecting field names,
   preserving missing-primary-key versus missing-required-field priority.

7. Adopt Toko Tooling 0.39.3 through its authenticated exporter and index native
   versus installed documentation. Respect the policy-only namespace boundary;
   retain receipt-owned paths until the producer qualifies that migration.

8. Adopt the requested Toko Tooling 0.39.6 and execute the online-reference
   migration. Retire managed reference copies with the exporter, pin upstream
   links, organize native top-level docs by topic and qualify moved-doc coverage.

## Status

Ready for user-owned integration after focused qualification. Package versions
and publication remain unchanged. The three audit cleanups were subsequently selected by the maintainer; focused
qualification for that second landing slice has passed.

The non-yanked published archive checksum is
`c868b5e1ae7d5056333140ce86ddfa6de65666b71f1eb7a659f21868041ce6ad`;
its source is `4de520b4706b1f76e4900d1acae3aa637bc26157`. Cargo changed only the
Storage version/checksum record, from 0.71.2 to 0.72.0. No Rust adaptation was
needed. Existing allocation-report and integrity-inspection boundaries already
call explicit allocation diagnostics. Pending technical notes were carried into 0.308 without changing the independent CLI ledger.

All 124 focused tests passed: four facade bootstrap/cold-reopen, two runtime
binding, three allocation diagnostic, 24 startup, 80 commit/recovery and 11
registry tests. Strict Core/facade all-feature library/test lint, all-feature
facade Wasm compilation, CLI all-target compilation, formatting, documentation,
dependency policy and diff checks passed. Evidence is under
`target/evidence/storage-072/`.

Full workspace/release gates, macOS and installed IC qualification were not
requested and remain skipped user-owned validation. Canic remains deferred.
No network lifecycle changes, package version changes or delivery occurred.
IC cycles, instruction counts and total/code-section Wasm size deltas are
unmeasured. This dependency adoption adds no local execution flow or behavior
axis; Storage retains ownership of admission and backing verification.


## Selected cleanup closeout

Completed the three maintainer-selected findings. Integer and float CHECK
round-trips now share schema construction while retaining their nominal names
and separate assertions. Filter integer text parsing delegates to the same typed
range checks as strict SQL literals; strict admission still rejects text.
Removed duplicate allocation snapshots and equivalent assertions from runtime
and registry tests. The shorter integer test no longer needs a line-count lint
exception.

All 111 selected tests passed: 79 CHECK, 19 schema-type, two runtime and 11
registry cases. The new numeric boundary case covers narrow limits, signed and
unsigned overflow, malformed text and strict text rejection. Strict all-feature
Core library/test lint, no-default-feature compilation, formatting,
documentation and whitespace checks passed. Evidence:
`target/evidence/cleanup-0308/`. Broad gates remain skipped user-owned validation.
Performance deltas remain unmeasured. No behavior axes were added; numeric
range checks and fixture construction each have one shared owner. No delivery
occurred; this slice is ready for integration.


## Predicate and numeric cleanup closeout

Completed the next three maintainer-selected cleanups. Accepted index predicates
check canonical form once before a field-reference walk. SQL metadata now renders
from accepted nodes directly, without constructing and rewriting a runtime tree.
Comparison spelling is shared with parser round-trip tests; enum/ULID display
literal resolution is shared with CHECK rendering while each caller retains its
error category. Signed/unsigned width selection has one owner for filter and
strict literal conversion; only filter admission parses text.

All 179 selected tests passed: 30 accepted-index predicate, 79 CHECK, 19 schema
literal, 31 predicate parser and 20 SQL lexical/rendering cases. Strict Core
all-feature library/test lint, no-default-feature compilation, formatting,
documentation and whitespace checks passed. The first new display fixture used
unsupported reduced-parser constants; it was corrected to test accepted constants
directly and use supported ordered casefold comparisons. The initial failure
and final passing logs are retained in `target/evidence/predicate-cleanup-0308/`.
An incoming Entropy 0.1.3 lock selection was preserved and used for qualification;
that dependency update is not attributed to these cleanups.

No new behavior axes or public APIs were introduced. The display path loses an
intermediate executable AST and repeated canonicalization is removed. Full
workspace/release and macOS checks remain skipped user-owned validation; cycle,
instruction and total/code-section Wasm size deltas are unmeasured. No delivery
occurred. This slice is ready for integration.


## SQL write authority and RETURNING closeout

Completed both maintainer-selected follow-ups. INSERT/UPDATE literal admission
now resolves one borrowed accepted field and reuses it for generated/managed
checks, nullability and type conversion, and physical patch placement. Expression
results reuse their resolved field too. Whole-column INSERT prevalidation and
UPDATE primary-key refusal remain at their original boundaries.

One prepared RETURNING plan carries accepted columns and the existing selected
field projection from early validation through precommit response-size checks
and final statement shaping. Validated DELETE reuses that descriptor/plan rather
than re-entering admission. Count-only writes construct no projection labels.
The selected-payload regression now reuses the same plan for borrowed byte
checks and owned response shaping, retaining reordered columns and ignoring the
unreturned large blob.

All 94 focused tests passed: 72 SQL owner tests, 12 field-admission/alias cases,
five expression-update cases, two write-scan bounds cases, and the count-only,
update-returning and delete-returning boundary cases. Strict Core all-feature
library/test lint, all-feature facade Wasm compilation, formatting,
documentation and whitespace checks passed. Initial propagation and test-clone
lint failures plus the final passing logs remain under
`target/evidence/write-cleanup-0308/`.

No public API, input mode, or behavior axis was added. A local prepared projection
replaces repeated field-name resolution and the accepted write field remains the
policy/type/slot authority. Existing unrelated edits and dependency selections
are preserved. Full workspace/release and macOS checks remain skipped user-owned
validation. IC cycle/instruction and total/code-section Wasm size deltas remain
unmeasured. These slices are ready for integration; no delivery occurred.


## Cache admission and INSERT omission closeout

Completed both selected follow-ups. Literal command and parameter-template cache
misses share resident construction, accepted-root binding, entity-label capture,
retained-size admission and weighted insertion. Compilation, binding and hit
handling remain at their existing boundaries. Refused cache residents still
execute without retention. INSERT omission classification now carries only
whether a required primary key is missing, returning MissingRequiredFields for
any missing non-key requirement without collecting field names.

All 26 focused tests passed: nine cache-accounting cases, four text-cache and
schema/template invalidation cases, and 13 SQL write-field cases. The added
regression distinguishes missing-key-only, missing-non-key and combined omissions,
verifies unchanged rows after each refusal, and admits nullable omitted fields.
Strict Core all-feature library/test lint, all-feature facade Wasm compilation,
formatting, documentation and whitespace checks passed. Initial callback lifetime
failure and corrected passing evidence are retained under
`target/evidence/cache-omission-cleanup-0308/`.

No public API, cache policy or behavior axis changed. Shared cache admission has
one owner and omission diagnostics lose their temporary collection. Existing
pending work and dependency selections were preserved. Broad workspace/release
and macOS gates remain skipped user-owned validation. Cycle/instruction and total
uncompressed/code-section Wasm deltas are unmeasured. These changes are ready for
integration; no commit or deployment occurred.


## Tooling 0.39.3 and documentation closeout

Adopted the non-yanked published 0.39.3 archive with SHA-256
`9286bc781005e242e90234024a41416e92f1cf8a3098033864affab7dbf61197`, source
`db66c9e4275eedafb308045ff7199c8a688c66ca`. Cargo changed only the tooling record
from this task's input; the approved nested tooling-only repair preserves its
other records. The exporter retains all 183 installed files at their canonical
paths. Executable scripts and tool pins are unchanged from qualified 0.39.2;
the refresh therefore used snapshot-only mode and reuses that tool/caller evidence.

Added a consumer-owned documentation index and root README link, distinguishing
product topics, product release history and installed tooling references. Updated
current tooling provenance and documented the published namespace boundary.
The 0.39.3 migration is policy-only: it explicitly forbids managed-file relocation
or a namespace gate until the producer releases and qualifies those changes.
No product wrapper or consumer-specific remapping was introduced.

Root/nested tooling admission, dependency policy, canonical-file integrity,
1127 local references across 138 documents and whitespace checks passed.
Evidence: `target/evidence/toko-tooling-0393/`. No Rust behavior changed; broad
workspace/release and macOS gates remain skipped user-owned validation. Canic
runtime resolution remains deferred. Performance deltas are unmeasured.
No commit or deployment occurred.


## Tooling 0.39.6 and native topic migration closeout

The maintainer selected 0.39.6 while the requested 0.39.5 migration was underway.
The authenticated archive checksum is
`00fcdf9fa866007e0d8e802f711257cd23e3081a467daf69225b3844f91dbc1b`, source
`5806e6e4943f6be600cd53385498ce08a5f804c4`. This supersedes the earlier namespace
migration deferral: shared reference documents now stay upstream at pinned URLs.
The exporter retired unchanged managed references and unused optional maintenance
and producer-test entrypoints. Local host maintenance selects another checkout;
no host schedule was modified. Directly called validation-evidence and release-
receipt helpers are explicit retained entrypoints. The final receipt owns 107
files; its only Markdown files are the root baseline and two canonical files.

Native Foundations moved under architecture, the feature/readiness plans under
roadmap, and release metadata under governance. Relative links and native source
path references were updated, with saved HTML review evidence untouched. The
consumer documentation checker includes architecture, roadmap and governance
roots; six new fixture assertions prove relocated docs are checked. The only
remaining top-level docs file is the native index.

All 25 documentation-adapter fixture cases and 680 local references across 79
current documents pass. Root/nested tooling admission, dependency policy,
installed-tool checks, release-adapter substitutes and whitespace checks pass.
Canonical source inputs and package inheritance were checked. The initial .5
check exposed references pulled in by unused entrypoints; the final .6 selection
retires them. An incoming Entropy 0.1.4 lock edit was preserved; its verified
archive was cached after offline metadata initially refused. No runtime adoption
qualification is attributed to this documentation task.

Evidence is under `target/evidence/toko-tooling-0396/`, with the intermediate
migration previews/checks under `target/evidence/toko-tooling-0395/`. No product
behavior axis changed. Broad Rust/workspace/release and macOS gates were not
requested and remain skipped user-owned validation. Cycle/instruction and total/
code-section Wasm deltas are unmeasured. Changes remain local; no delivery ran.


## Native package layout closeout

The maintainer approved the native workspace split and the narrow Core dev-only
support exception. The [layout record](../0.308-package-layout/status.md) records
all sixteen moves, focused qualification, release multi-lock propagation and the
remaining deferred Canic boundary. Product behavior, package versions and
supplier selections remain unchanged by this migration. Changes are local.


## Toko Tooling 0.39.7 closeout

The maintainer requested the published tooling refresh. All three workspace pins
and locks select 0.39.7, archive SHA-256
`fd795f6ef878496ac27a990a9208b87fa786a9148c18580338ef8ac79df501d7`, source
`fcbbcd147678fdfe5ab22c36710e017fb2c018e4`. The authenticated exporter retains the
107-file executable/baseline selection; reference docs remain upstream. Every
unrelated lock record is unchanged. The approved tooling-only Canic lock repair
was reused after offline Cargo could not resolve its older dependency graph.

The consumer release fixture now substitutes native qualification admission in
its disposable dispatch copies and retains both native fetch selections. Real
root/native/Canic tooling admission, installed-tool checks, dependency policy,
release dispatch/preparation substitutes, selected shell lint, formatting and
current documentation links pass. The notice link repair points to an existing
source file rather than restoring an absent file. No product versions, commits,
network lifecycle or deployment changed. Full product/release and macOS gates
remain skipped user-owned validation; IC cycles/instructions and total/code-section
Wasm deltas are unmeasured. Evidence: `target/evidence/toko-tooling-license-scan/`.
