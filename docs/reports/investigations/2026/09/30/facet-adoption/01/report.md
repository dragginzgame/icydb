# Facet Rust library audit for IcyDB

Facet could help IcyDB build better host diagnostics, inspect application types,
and share traversal code at typed conversion boundaries. Its strongest potential
is outside the canister runtime. This audit recommends keeping the current
schema, value, storage, and execution authorities, and evaluating Facet only
against a concrete tooling need. A general replacement of IcyDB's macros or
Serde is not justified by the evidence collected here.

The most consequential blockers are Facet's experimental security support,
unstable reflection identities, the audited prerelease's Rust 1.92 requirement,
domain-type integration work, and unmeasured canister costs. One concrete
generator mismatch is verified in source: the audited TypeScript generator maps
64-bit and 128-bit Rust integers to JavaScript `number`.

## Scope and evidence

Requested scope: a full adoption audit of the Rust reflection library and its
relevance to IcyDB. This investigation covers reflection architecture, derives,
reading and construction, invariant preservation, formats, dynamic values,
diagnostics, code generation, dependency boundaries, portability, maintenance,
and the IcyDB integration points those capabilities could affect.

The evidence is source inspection and published API documentation. This is an
adoption assessment, not an exhaustive memory-safety certification of every
Facet crate. No exploit reproduction, dependency advisory scan, build, behavioral
test, fuzz campaign, or IC measurement was executed. Unrelated ecosystem
projects were assessed for relevance rather than audited internally.

| Input | Exact evidence boundary |
| --- | --- |
| IcyDB | Workspace `0.261.19`, HEAD `c16083c1da5ebe7c12b974d07b4f30374e287ca1`, including the worktree observed during this audit |
| Facet core source | Commit `65bae5c31a7ce401bc44630fb96250ea884cfd3e`, dated 2026-08-20; principal packages identify themselves as `0.50.0-rc.7` |
| Facet format source | Commit `4279debff780ae1cd5b028201f446c26594b1120`, dated 2026-08-20; principal packages identify themselves as `0.50.0-rc.7` |
| Published documentation | docs.rs serves Facet and facet-reflect `0.46.5`; its release list also includes `0.50.0-rc.7` |
| Working source copies | Temporary checkouts under `/tmp/icydb-facet-audit` and `/tmp/icydb-facet-format-audit` |

The published release list dates `0.46.5` to 2026-05-26 and `0.50.0-rc.7` to
2026-08-20. Those are release dates, not the audit date. Prerelease source findings
must not be silently treated as properties of `0.46.5`.
[Published release list](https://docs.rs/crate/facet/latest).

Source and documentation locations disagree about some repository organization:
the browsed facet-format landing page says its workspace has moved into the
Facet monorepo, while the retrieved commits preserve a separate format workspace.
This report pins source findings to the retrieved commits rather than claiming
they represent every subsequent upstream change.
[Repository relocation notice](https://github.com/facet-rs/facet-format).

Existing changes in the root and detailed changelogs, page coordinator, entity
rename status tracker, and another investigation were left untouched. Audit
authorization permits this new report; it does not authorize dependency adoption,
implementation, release edits, or a new minor line.

## What Facet provides

`#[derive(Facet)]` generates a type description exposed as `T::SHAPE`. The shape
describes fields, variants, memory layout, attributes, documentation, and
type-specific operations. `Type` describes Rust structure, while `Def` describes
how consumers interact with a value: for example, a collection can be a Rust
struct while behaving as a list. Generic consumers traverse this metadata.
[Shape API](https://docs.rs/facet/0.46.5/facet/struct.Shape.html).

This can replace repeated structural traversal in several tools. It does not
automatically generate database semantics or make all consumers share the same
wire representation. Each format or integration must implement its own meaning.

| Component | Verified capability | IcyDB relevance and assessment |
| --- | --- | --- |
| `facet` and `facet-core` | Derive, static metadata, erased operations, container and scalar definitions | Potential application-type introspection; substantial overlap with existing generated descriptors |
| `facet-reflect::Peek` | Read fields, variants, collections, and scalars through reflection | Candidate for generic host inspection or typed input traversal |
| `facet-reflect::Partial` | Construct values incrementally, track initialization, finish with fallible APIs | Candidate for a typed output decoder; adds construction machinery |
| `facet-reflect::Poke` | Replace values and mutate supported fields | Limited tooling value; database updates must still pass IcyDB admission |
| `facet-json` and other format crates | Format parsers and serializers; JSON has borrowing entrypoints and structured errors | Potential host import/export and configuration tooling |
| `facet-pretty` | Structured printing; audited source redacts fields marked sensitive | Candidate for readable diagnostic artifacts |
| `rediff` | Structural differences and assertions over reflected values | Candidate for host comparison and richer test failures |
| `facet-json-schema`, `facet-typescript`, `facet-zod` | Generation crates are present in the audited format workspace | Potential JSON-facing tool output; mappings need qualification |
| `facet-value` | Tagged-pointer dynamic representation with numbers, strings, bytes, collections, and other kinds | Useful format intermediary; unsuitable as an IcyDB Value replacement |
| Postcard and related binary formats | Compact format support | No demonstrated need to change IcyDB's canonical persistence grammar |

The reading and construction capabilities are documented in the
[reflection API](https://docs.rs/facet-reflect/0.46.5/facet_reflect/) and
[Partial API](https://docs.rs/facet-reflect/0.46.5/facet_reflect/struct.Partial.html).
JSON's owned and borrowing entrypoints are documented in the
[JSON API](https://docs.rs/facet-json/0.46.1/facet_json/).
Structural comparison is documented in the
[rediff API](https://docs.rs/rediff/0.46.1/rediff/).

`Facet` itself is not dyn compatible. Type erasure is provided by reflection
wrappers and shapes, rather than a general `dyn Facet` interface. The lifetime
parameter permits describing types that borrow data. These features do not
provide automatic zero-copy decoding of IcyDB's current persisted rows.
[Facet trait](https://docs.rs/facet/0.46.5/facet/trait.Facet.html).

## Where it could help IcyDB

### Host diagnostics and artifact comparison

This is the best candidate for a bounded experiment. IcyDB already exposes live
accepted-schema descriptions and exports diagnostic artifacts. A reflected host
projection could print nested differences, show paths to changed fields, and
reuse display code across diagnostic DTOs. The appropriate owners are the CLI
diagnostic and observability modules.

Evidence:
[schema observability](/home/adam/projects/icydb/crates/icydb-cli/src/observability/schema.rs),
[diagnostic artifacts](/home/adam/projects/icydb/crates/icydb-cli/src/diagnostic/artifact.rs).

This opportunity is conditional. The current artifact already uses Serde JSON;
formatting or comparing that existing representation may solve the problem with
less integration work. Facet deserves adoption only if a representative report
shows a useful improvement that the simpler alternative cannot supply.

A structural diff also cannot decide whether a schema change is admissible.
IcyDB must interpret entity, field, member, variant, and index identities through
accepted metadata. A display tool may highlight a rename without establishing
that the corresponding migration is legal or complete.

### Shared typed value conversion

This is the most interesting runtime research direction, with higher cost and
risk. IcyDB generates entity-specific write lowering and row decoding. Facet
could potentially supply one traversal that reads application DTOs into IcyDB
inputs, or constructs typed rows from accepted output values.

The existing seam is concrete:
[typed adapter traits and write cells](/home/adam/projects/icydb/crates/icydb/src/db/session/write.rs),
[generated entity adapters](/home/adam/projects/icydb/crates/icydb-model-macros/src/node/entity.rs),
[typed descriptors](/home/adam/projects/icydb/crates/icydb-core/src/db/dynamic_write.rs).

The required flow would remain:

```text
Application type plus IcyDB source descriptor
    -> current accepted typed binding
    -> reflected conversion at the application boundary
    -> existing IcyDB inputs or accepted outputs
    -> existing admission, execution, and storage
```

Reflection would replace traversal at this seam. It would not replace binding,
accepted policy, request accounting, or database identity. Generated source
descriptors remain necessary unless an explicitly designed replacement carries
the same IcyDB-owned contract.

Writes must preserve all four `WriteCell` intents: `Omitted`, `Default`, `Null`,
and `Value`. Ordinary optional-field deserialization is not an equivalent
contract. Reads must preserve exact scalar domains, nested accepted member and
variant binding, and failure behavior before exposing the completed typed row.

The possible benefit is less repeated generated conversion code. The possible
cost is more metadata, indirect calls, heap construction, planning, and error
handling. No Wasm or instruction evidence currently establishes the net effect.

### Application type inspection and documentation

Facet could describe application-facing DTOs for developer tools: field lists,
documentation, enum choices, and forms for explicit JSON-facing inputs. This is
useful when a tool needs the Rust application shape itself.

Live database inspection already has accepted-native owners:
[accepted snapshots](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/snapshot.rs),
[schema descriptions](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/describe.rs),
[inspection plans](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/inspection_plan.rs).

A tool that describes the deployed database should consume those existing
outputs. Reconstructing deployed schema from a reflected entity would give it
the wrong authority after catalog mutation or a rename.

### Import export and client generation

Facet's format ecosystem could support a host tool that accepts application
documents with structured errors. It could also generate definitions for a
deliberately specified JSON interface. Both require a demonstrated product need;
neither warrants replacing current Candid endpoints or persistence.

The audited TypeScript generator converts `u64`, `u128`, `i64`, and `i128` to
`number`. That cannot preserve the entire source integer domain. It also tracks
generated types by `type_identifier`, so distinct types with the same short name
need explicit qualification before use. These are findings about the audited
generator, not claims about all Facet-based generators.
[Audited TypeScript generator](https://github.com/facet-rs/facet-format/blob/4279debff780ae1cd5b028201f446c26594b1120/facet-typescript/src/lib.rs).

For IcyDB, a generated client must explicitly define large-integer, decimal,
principal, account, binary, enum, null, and write-intent representations. Its
types must describe the actual transport. A generator for a Rust shape does not
automatically describe Candid's encoding of that shape.

### Test diagnostics

Reflected assertions can produce more readable nested failure output without
requiring `PartialEq`. This could help a particular complex fixture. It does not
replace tests of IcyDB equality, canonical encoding, numeric ordering, or typed
error classification. Structural sameness is a different question from those
database contracts. Do not convert the existing test suite merely to standardize
assertion style.

## Architecture and type compatibility

IcyDB already has database-specific reflection in the form of accepted schema,
field kinds, value catalogs, typed descriptors, and compiled row contracts.
Facet adds information about a compiled Rust value. Those descriptions have
different owners and lifetimes.

| Boundary | Required treatment |
| --- | --- |
| Accepted schema authority | Preserve accepted snapshots as runtime authority; Facet can describe a proposal or an application value |
| Durable identities | Retain IcyDB source bindings and accepted IDs; never persist shape pointers, compiler type IDs, field offsets, or Facet declaration IDs |
| Renames and migrations | Bind reflected fields through current IcyDB contracts; Rust names and serialization renames cannot establish accepted identity |
| Query and index semantics | Keep accepted capabilities, comparison, ordering, hashing, and key encoders under existing owners |
| Persistence | Keep bounded fallible codecs and the current version-1 grammar; reflection describes memory layout, not a portable storage format |
| Recovery | Keep journal and catalog publication semantics; a reflected value constructor supplies no recovery protocol |
| Canister endpoints | Retain Candid support and `icydb_*` endpoint names; no Candid implementation was identified in the audited core and format integration surfaces |

Facet explicitly documents `DeclId` as unstable across compilations, refactors,
and reformatting, and unsuitable for persistence. Its compiler type identity is
for in-process type operations, not a database identity.
[Declaration identity contract](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-core/src/types/decl_id.rs),
[compiler type identity implementation](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-core/src/types/const_typeid.rs).

The current IcyDB identity bridge is maintained in
[source bindings](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/source_binding.rs).
Replacing it with structural reflection would lose the distinction between
authorship identity, accepted identity, editable names, and schema incarnation.

Type coverage also needs deliberate adaptation:

| IcyDB type family | Integration implication |
| --- | --- |
| Ordinary Rust scalars and collections | Broad Facet coverage exists; IcyDB bounds, map canonicalization, and set semantics still apply |
| IcyDB Decimal | IcyDB owns an i128 mantissa and scale domain; Facet's `rust_decimal` feature does not implement this local type |
| Principal, Account, Subaccount, Id | Need local reflected representations or fallible proxies that preserve domain construction and identity |
| IcyDB Ulid | Local wrapper needs qualification; audited Facet enables upstream `ulid` 1.x while IcyDB uses 3.x |
| IntBig, NatBig, U256 | Preserve exact numeric domains and byte limits; generic numeric conversion is insufficient |
| Float32 and Float64 | Preserve existing validation and canonical comparison rules |
| Named records and enums | Preserve accepted catalog identity and member/variant bindings independently of Rust field order |
| WriteCell | Preserve all four authored intents; do not collapse into Option |

Type evidence:
[IcyDB Value](/home/adam/projects/icydb/crates/icydb-core/src/value/mod.rs),
[IcyDB Decimal](/home/adam/projects/icydb/crates/icydb-schema/src/decimal/mod.rs),
[Facet optional implementations](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-core/Cargo.toml),
[Facet upstream dependency versions](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/Cargo.toml).

`facet-value` is not a substitute for IcyDB Value. Its compact representation is
useful for format data, but it does not carry IcyDB's accepted enum identity,
account and principal semantics, fixed-point decimal contract, or canonical
database map and key rules. The audited source supports 128-bit numbers;
rejecting it solely on the assumption that it only supports 64-bit integers
would be inaccurate. The mismatch is semantic coverage and ownership.
[Dynamic value kinds](https://github.com/facet-rs/facet-format/blob/4279debff780ae1cd5b028201f446c26594b1120/facet-value/src/value.rs),
[numeric representation](https://github.com/facet-rs/facet-format/blob/4279debff780ae1cd5b028201f446c26594b1120/facet-value/src/number.rs).

## Safety and boundedness

`Facet` is an unsafe trait. Its safety contract requires correct layout and
invariant descriptions; otherwise safe reflection consumers can become unsound.
Prefer supported derives and safe APIs. A hand-written implementation or a shape
assembled from external catalog data would require a separate safety review.
Never turn an accepted schema snapshot into an arbitrary native memory shape.
[Unsafe trait contract](https://docs.rs/facet/0.46.5/facet/trait.Facet.html).

`Partial` tracks initialized fields and provides fallible completion. Audited
construction code also checks declared invariants recursively and caches
information about invariant-bearing subtrees. This is useful, but it cannot
infer undeclared application invariants or enforce database constraints.
[Construction invariant checks](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-reflect/src/partial/partial_api/build.rs).

There is an important distinction for mutation: `Poke::new` permits wholesale
replacement; individual struct-field mutation requires a compatible POD
declaration. POD means field combinations have no additional invariants. It is
not an appropriate blanket annotation for entities with semantic constraints.
Even a valid in-memory replacement must still pass IcyDB write admission.
[Mutation API](https://docs.rs/facet-reflect/0.46.5/facet_reflect/struct.Poke.html).

The audited derive rejects ordinary reflected enums without an explicit representation.
Adding Facet to generated enums therefore is not necessarily a derive-only
edit. Representation choices and their effects need qualification for the exact
adopted release.
[Enum derive implementation](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-macros-impl/src/process_enum.rs).

Production reflection and format internals contain `unwrap` and `expect` paths.
Their presence is not proof that malformed input reaches a panic. It means a
runtime adopter must examine the reachable paths and prove typed rejection for
supported invalid inputs. Example locations include Partial frame/layout
assumptions and format error conversion requiring an active span guard.
[Partial internals](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-reflect/src/partial/partial_api/internal.rs),
[format error conversion](https://github.com/facet-rs/facet-format/blob/4279debff780ae1cd5b028201f446c26594b1120/facet-format/src/deserializer/error.rs).

The inspected JSON and format entrypoints did not establish a comprehensive
IcyDB-style budget for input bytes, nesting, collection construction, temporary
allocation, and conversion steps. This is a verification gap, not a demonstrated
unbounded-input exploit. An input byte cap alone would not prove all of those
obligations. Reflection caches and allocations also need cold and warm
accounting if they become reachable in a canister request.

IcyDB's existing bounded decode and conversion owners provide the contracts an
adapter must preserve:
[recursive decode](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/cursor.rs),
[bounded byte reader](/home/adam/projects/icydb/crates/icydb-core/src/db/codec/reader.rs),
[conversion accounting evidence](/home/adam/projects/icydb/docs/design/0.257-typed-query-explain/0.257-value-conversion-perf.md).
The historical measurements in that last document are not Facet measurements.

## Portability dependencies and maintenance

The audited prerelease declares Rust 1.92. IcyDB's public dependency path promises
Rust 1.88.0, although its internal development toolchain is newer. A general
dependency on this prerelease would violate that public floor. A separate host
tool could use a newer compiler if its own support contract allows it. The exact
`0.46.5` MSRV was not verified, so this report does not present the stable release
as a solution to that incompatibility.
[Audited compiler requirement](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/Cargo.toml),
[IcyDB support contract](/home/adam/projects/icydb/README.md).

Core reflection has `no_std` and allocation features. That is encouraging for
portability, but does not establish IC readiness. Selected format and diagnostic
crates have their own transitive feature choices. The audited core and format
workflows include targeted Miri runs, but no Wasm or PocketIC job was identified
in those inspected workflows. A Wasm-specific terminal dependency gate in
facet-pretty shows some platform care; it is not evidence of a qualified canister
integration.
[Core CI](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/.github/workflows/test.yml),
[format CI](https://github.com/facet-rs/facet-format/blob/4279debff780ae1cd5b028201f446c26594b1120/.github/workflows/test.yml),
[pretty dependency gating](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet-pretty/Cargo.toml).

Facet core does not require adopting its entire ecosystem or a JIT. Related
native execution projects provide no established solution to IcyDB's Wasm cost
question. A canister experiment should use an explicitly selected interpreter
and reflection dependency set, with no assumption that native code generation
will be available on the IC.

The audited umbrella defaults include documentation metadata; derive output can
retain field/type docs and source locations. That is useful for host tools and
potentially costly or undesirable in deployed binaries. Audited options include
disabling the doc feature or stripping generated documentation with
`facet_no_doc`. Evaluate the exact unified feature graph, because another
dependency can re-enable features.
[Facet feature definitions](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/facet/Cargo.toml).

Format adoption is not a blanket Serde replacement. IcyDB still needs its
maintained Candid and Serde contracts, domain encoders, and generated operations.
Keeping both systems for the same DTO would add derives and attributes until a
specific consumer can actually be removed. The current IcyDB field-walk generator
already centralizes visit, normalize, and validate traversal, reducing the
obvious benefit of replacing that slice with generic reflection.
[Existing shared field traversal](/home/adam/projects/icydb/crates/icydb-model-macros/src/imp/field_walk.rs).

Companion crates do not all expose matching release lines. For example, the
browsed legacy facet-diff API uses the `0.43.2` family, while rediff documents a
`0.46.1` release. Do not assume arbitrary Facet-derived types can be passed to
companions compiled against another Facet version. Verify one coherent lockfile
for the chosen use case.
[Legacy diff dependencies](https://docs.rs/facet-diff/0.43.2/facet_diff/),
[rediff release API](https://docs.rs/rediff/0.46.1/rediff/).

Licensing of the inspected core and format workspaces is MIT OR Apache-2.0,
matching IcyDB's overall licensing choices. No complete transitive license
inventory was generated. More materially, the audited core security policy says
no version currently receives supported security updates and describes the
project as experimental. Miri and fuzz inputs provide useful evidence of safety
work, but this audit neither executed those checks nor establishes their complete
coverage.
[Security policy](https://github.com/facet-rs/facet/blob/65bae5c31a7ce401bc44630fb96250ea884cfd3e/SECURITY.md).

## Adoption findings

These severities describe risk to an IcyDB adoption proposal. They are not
confirmed vulnerabilities in Facet.

| Finding | Risk | Evidence and disposition |
| --- | --- | --- |
| F1 Runtime authority and durable identity mismatch | HIGH | Shape layout and DeclId cannot own accepted schema or durable IDs. Reject that integration pattern; preserve current bindings |
| F2 Experimental security support and unsafe foundation | HIGH | Unsafe trait contract and explicit security policy. Defer production persistence or untrusted-input adoption pending a bounded safety review |
| F3 Prerelease compiler requirement | HIGH | Audited Rust 1.92 exceeds public Rust 1.88.0. Keep any experiment outside that dependency path; stable-release MSRV remains unverified |
| F4 Generic TypeScript numeric and naming mappings | HIGH | Large integers become number; types are tracked by short identifier. Do not publish stock generated IcyDB client types |
| F5 Domain types and authored write intent require adapters | MEDIUM | IcyDB scalar domains, accepted named values, and four WriteCell intents are not supplied by generic reflection. Qualify exact conversions before proposing replacement |
| F6 Bounds panic reachability and cache accounting unqualified | HIGH | Inspected internals and caches add obligations absent from a source-only check. No production runtime recommendation without focused rejection and budget proofs |
| F7 Wasm benefit and compatibility unmeasured | MEDIUM | No IC integration was built or measured. Record raw Wasm, instructions, and cycles before judging a runtime change |
| F8 Companion version and repository churn | MEDIUM | Documentation and retrieved source organization differ; companion release lines vary. Pin one coherent dependency graph and exact source before experimentation |
| F9 Host diagnostic opportunity | LOW | Existing accepted artifacts provide a safe integration seam. Compare against existing Serde-based rendering before adding a dependency |

## Recommended follow up

The current decision is to keep implementation unchanged. The audit found
plausible uses, but no demonstrated unmet requirement or measured runtime benefit
that justifies a general adoption.

If richer artifact diagnostics are wanted, the smallest useful experiment is a
host-only comparison of one existing diagnostic artifact. Preserve its accepted
identity and current JSON contract, compare Facet pretty/diff output with a
renderer over the existing Serde representation, and choose the simpler option
that solves the concrete problem. Keep any mirror projection temporary; a
permanent duplicate DTO would weaken the proposed simplification.

No-build gate for that candidate: the need is a specific unreadable or difficult
comparison; the existing artifact remains canonical; the alternative is a small
renderer over existing data; the owner is host diagnostics; the only proposed
new mechanism is reflection for display. It should add no canister execution
route, persisted state, wire format, or schema authority. If the simpler renderer
is sufficient, do not add Facet.

A runtime conversion experiment is a separate decision. Use temporary isolated
variants, not a permanent selectable backend. Demonstrate:

1. Exact scalar and named-value conversion, including full-width integers,
   decimals, principals, account bytes, enum binding, and all write intents.
2. Preserved current binding failures and rejection before partial typed output
   is published; constructors must enforce domain invariants.
3. Byte, depth, collection, conversion-step, and allocation limits for supported
   inputs, plus a review of reachable dependency panic paths.
4. The public compiler floor and a minimal coherent dependency/feature graph.
5. Functional equivalence on the focused typed fixture in Wasm and PocketIC.
6. Raw non-gzipped Wasm bytes, IC instructions, and cycles against the same
   baseline, with both first-use and reused reflection plans.
7. Complexity change: generated code actually removed, adapters added, files
   touched, approximate line delta, and any additional maintained states.

The most relevant existing footprint fixtures are
[one entity typed query](/home/adam/projects/icydb/canisters/audit/one_entity_typed_query/Cargo.toml),
[ten entity typed query](/home/adam/projects/icydb/canisters/audit/ten_entity_typed_query/Cargo.toml),
and the maintained
[typed facade fixture](/home/adam/projects/icydb/testing/model-facade-only/Cargo.toml).
Those provide candidates for a bounded future comparison; no measurement claim
is made from their mere existence.

If reflection reduces generated code but increases runtime cost or the number
of authorities and adapters, keep the current implementation. If it reduces
both supported complexity and acceptable measured cost, replace the selected
conversion path directly under IcyDB's pre-1.0 hard-cut rules. Do not retain two
production implementations as a compatibility mechanism.

## Validation and change footprint

Source-level evidence review is complete within the declared scope. Behavioral,
security, compiler-floor, and IC compatibility certification remains unperformed.
Raw Wasm, IC-cycle, and instruction deltas are **unmeasured**. No native timing
benchmark was run or used as a substitute.

Direct crates.io metadata/download access and a subsequent remote tag fetch
failed because shell DNS access was unavailable. Published API/release evidence
was obtained through browsing; exact prerelease sources were inspected from the
successful temporary clones. Stable package source/MSRV and a complete latest
upstream graph therefore remain verification gaps.

Report checks passed: all 19 local file references resolve, code fences are
balanced, and no trailing whitespace was found. No rendered preview was available.

The only repository change made by this audit is this new report: one Markdown
file, approximately 450 added lines, zero production-code lines. Implementation complexity and runtime
state space are unchanged. No dependencies, package versions, changelogs, design
trackers, services, networks, commits, or pushes were changed by this audit.
