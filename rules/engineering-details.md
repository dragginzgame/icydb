# Engineering contracts by task
<!-- Shared companions: docs/candid-boundary.md docs/principles/rust-code-hygiene.md docs/tool-store-maintenance.md rules/agent-maintenance.md rules/change-reporting.md rules/contributions.md rules/rust-workspaces.md tasks/README.md tasks/cargo-tool-versions.md -->

These contracts supplement the [engineering baseline](../DRAGGINZGAME.md).
Read the applicable sections when the task touches their subject. This file is
not a second mandatory startup read. The core owns validation cadence; detailed
product, release and recovery obligations remain binding when applicable.

## Adoption and local authority

- Every consumer must adopt a reviewed revision through a recorded local snapshot
  or explicit revision-bound reference. Their AGENTS.md must identify that baseline
  and the local overlay. A moving sibling checkout is not hidden inheritance.
- Shared rules govern engineering practice. Product architecture, identities,
  numeric limits, validation gates and release identities stay local. Release
  target names and workflow follow the common contract below. Do not copy one
  consumer's architecture into the common baseline.
- Give each rule one detailed owner. The baseline summarizes it and links there;
  overlays list local choices and commands. Update the owning text when policy
  changes instead of appending repeated exceptions. Keep detailed reference
  procedures task-specific rather than adding them to every startup read.
- Shared Tooling owns consistency across the repository fleet: repository/sibling
  counts, shared-snapshot and pin drift, CI/issue overviews, cross-repository LOC
  and tooling inventories, and common maintenance/audit procedures. Run central
  fleet reports here; their existence does not require every consumer to vendor
  them or add sibling scans/dashboard tests to its CI. Consumers still adopt
  shared tooling needed for their own development and validation.
- Keep repository-fleet inventory and governance out of IC Observability's product
  scope. Its `ic-metrics` and `ic-logging` crates own reusable measurement and log
  engines; a repository count is a Shared Tooling report, not a new Observability
  API, dependency or application.
  Product libraries do not become fleet-management owners because a report
  contains counts or is called a metric.
- Local overlays may strengthen the baseline or define product-specific choices
  within its delegated scope. A change to an explicit common rule, including
  command authority or delivery cadence, requires a maintainer-approved exception
  with scope and reason; calling it stricter does not bypass that requirement.
- Apply the shared baseline and the local overlay within its delegated scope.
  Local instructions do not silently override the common baseline.
  Read the existing [current handoff](#repository-handoff-files) for accepted
  work and implementation status. Treat historical documents as evidence, not
  current authority. Verify documentation against implementation; distinguish
  supported behavior from unfinished plans.
- Every repository must have a concise, accurate GitHub description of its
  current purpose and scope, consistent with its README and implementation.
  Review it during baseline adoption and whenever the repository's purpose,
  scope or maintenance status changes. Correct missing, stale or misleading
  descriptions; do not present retired behavior or planned features as current.

## Scope and authorization

The [contribution rules](contributions.md) own implementation, PR, checkout and
delivery authority. Use their [authorization table](contributions.md#agent-authorization)
and [delegated delivery](contributions.md#delegated-delivery) contract rather than
adding local approval gates. Direct edits are the default when a PR is optional;
an unqualified deploy has the defined fleet scope.

- Task-relevant sibling inspection is read-only and needs no separate permission.
  Source contributions require destination/change scope; fleet delivery permits
  release preparation in included normal checkouts after coordinating active work.
  Shared ownership or issue permission alone does not authorize sibling edits.
- Carry established authorization forward. Complete necessary preparation before
  asking about a genuinely missing decision; do not require magic phrases or
  repeated approval of already-authorized substeps. Inspection alone is not repair.
- Issue actions follow [agent maintenance](agent-maintenance.md#issue-review-and-repair):
  verify the GitHub owner, use current evidence and distinguish issue authority
  from source/delivery authority. Other external messages retain their own scope.
- Preserve unrelated work, explicit exclusions and runtime approval restrictions.
  An unavailable checkout or rejected action does not authorize an alternate route.

## Ownership and simplification

- Prefer deleting an obsolete mechanism, reusing its canonical owner or narrowing
  an existing contract before adding a concept. More abstraction, more tests and
  fewer lines are not goals in themselves.
- Give each state, identity, policy and invariant one authoritative owner. Carry
  decisions structurally; consumers project rather than reconstruct them. Keep
  independent validation at real trust, corruption and recovery boundaries.
- New modes, flags, caches, registries, traits and state machines need a
  demonstrated requirement, a canonical owner and a simpler alternative check.
  Do not build a framework to remove a little repetition or support hypothetical
  consumers. No change is a valid outcome when the need is unproven.
- Trace callers and failure contracts before merging similar flows. Preserve
  ordering, authorization, atomicity, recovery and platform constraints. Generated
  code is changed at its generator, then regenerated by the authorized workflow.
- Dependencies own their established contracts; do not reproduce them downstream.
  Libraries must not silently claim consumer endpoints or lifecycle ownership.

## DFINITY dependencies: own the solution

- **Do not submit PRs to DFINITY crates or repositories, or make our work depend
  on DFINITY accepting, implementing, maintaining or publishing changes.** Plan
  against the capabilities of already available, qualified dependencies.
- When those capabilities are insufficient, maintain the necessary implementation
  ourselves at its canonical owner or change our code so the dependency is no
  longer needed. Apply the [maintenance and provenance rules](dependency-pinning.md#maintaining-upstream-code)
  and the [IC Platform boundary](cargo-dependencies.md#external-ic-dependency-boundary).
  This does not require removing working dependencies or rewriting cryptography.
- Retire issues whose proposed outcome requires future DFINITY work as not
  planned. Preserve unresolved local defects in their owning issues with a
  self-owned solution; closing an upstream plan does not mean the defect is fixed.

<a id="repository-files-no-symlinks"></a>

## Repository files: self-contained source and packages

- **Keep source and distributions self-contained; prefer regular files.** This
  is a portability and reproducibility default, not a claim that symlinks are
  unidiomatic. Use explicit dependencies/configuration paths or the canonical
  package interface instead of repository links into sibling checkouts or
  machine-specific external locations.
- A narrow internal relative link is acceptable when it removes real maintenance
  work and stays within the owning checkout. Document its purpose and qualify
  checkout, tooling and packaging behavior on supported hosts. Avoid dangling
  links, cycles and implicit external dependencies. Prefer an ordinary reference
  when it serves the same purpose; do not introduce copied files and a syncing
  mechanism merely to avoid a useful internal link.
- Shared snapshots and shipped package assets remain regular, self-contained
  files. Materialize packaging copies at their generator and verify them against
  their canonical owner. Package-local `LICENSE` files contain the applicable
  complete terms and notices as regular files. An internal source link does not
  authorize shipping an unresolved link or omitting its target from an archive.
- Generated selection pointers in the verified executable store and deliberate
  symlinks in isolated security/rejection fixtures remain valid tooling uses.
  Normal operating-system/toolchain layouts and aliased checkout paths should
  work where practical; a repository-file preference alone is not a reason to
  reject them. Preserve each tool's actual path and custody requirements.
- Concentrate strict link/escape checks at packaging, installation and
  security-sensitive read/write boundaries. Review Git modes, working paths and
  package inventories when relevant, but do not build a general symlink scanner
  into every command. The optional `check-no-symlinks.pl` helper enforces a
  stricter all-regular inventory for repositories that choose it; it cannot
  assess justified internal links. Such repositories should select appropriate
  boundary checks rather than add a parallel exception framework merely to keep
  that gate. Shared Tooling itself retains this stricter inventory check.
- Preserve existing installations, evidence and link targets. Replacing an
  obsolete repository link must not modify its target. Exceptions to the
  self-contained dependency or shipped-file requirements still need scoped
  maintainer approval under the baseline's local-authority rules.

## CLI applications

- **Use Clap for as much Rust CLI behavior as its supported APIs can own.** This
  includes developer tools, generators, benchmark runners and maintained example
  or utility binaries. Use its derive or builder API; adding Clap only for flag
  recognition does not justify a second parser or validation layer.
- Declare typed values, paths, nonzero/ranged numbers, enumerated choices,
  defaults and environment sources in Clap. Use its argument actions, arity,
  groups, conflicts and requirements for supported argument relationships, and
  its help, version, usage, styling and parse-error handling. Check the selected
  version's capabilities before writing equivalent custom logic.
- Compose owned subcommands into the Clap command tree and pass parsed values to
  handlers. Reserve raw argument forwarding for actual external command
  boundaries. Do not maintain argv scanners, hidden flag injection, repeated
  parsing or separate help/option catalogs to connect our own commands when
  Clap's composition and propagation can carry the same contract.
- Integrate application-specific syntax through Clap's value parsers and typed
  extension points, reusing the existing domain parser. Keep custom validation
  for constraints Clap cannot express, runtime/configuration-dependent policy
  and independent trust or effect boundaries; document the reason locally.
  Merely using custom validation today is not evidence of a capability gap.
- When adopting a Clap capability, remove the superseded CLI mechanics and
  their duplicate declarations. Preserve intended syntax, defaults/precedence,
  native path bytes, output/exit behavior and sensitive-value redaction. Keep
  domain admission at its owner; do not weaken it or add a generic CLI framework
  merely to replace a local parser.

## Metrics and logging

- **`ic-metrics` serves only operational instrumentation of the Canic application
  tree**, including Canic, IcyDB, Backup and Blob Storage. Participating crates
  record what they are doing; Canic's metrics adapter owns their shared reporting
  endpoint. Library instrumentation must not introduce a dependency back on Canic.
  Use `ic-logging` for logging.
  [IC Observability](https://github.com/dragginzgame/ic-observability) owns both
  canonical crates. Select the capabilities a crate uses; this does not require
  otherwise unused dependencies or instrumentation in every crate.
- Outside those canonical owners, do not introduce or maintain custom metrics
  instrumentation, metric accumulators or histograms, recording engines, log
  stores, retention or paging engines, or competing metrics/logging frameworks
  for facilities we own. Reuse the canonical
  crates rather than copying their implementations or hiding duplicates behind
  wrappers. Existing bespoke implementations are migration work, not precedent
  for new ones.
- Metrics is not a general math, statistics, numeric-validation or benchmark
  utility library. Do not use it for averages, rounding, ratios, checked
  arithmetic, unrelated profiler reports or domain/DTO validation merely because
  similar calculations exist inside its recorder. Do not export or re-export
  those implementation helpers as reusable mathematical APIs. Keep ordinary
  calculations with their semantic owner; do not create another published crate
  for one formula. Preserve numeric/error contracts when removing accidental
  coupling. Tests of actual application instrumentation may use Metrics; generic
  tests and benchmarks must not acquire it just to calculate results.
- If a required Metrics or Logging capability is missing or unsuitable, search for an existing
  issue or open one in
  [IC Observability](https://github.com/dragginzgame/ic-observability/issues).
  Describe the concrete consumer requirement and extend the canonical crate
  there before adopting the improvement. Do not solve the gap with a private
  fork, local copy or parallel fallback engine. Adopt a compatible, available
  dependency through the normal dependency workflow; an issue alone does not
  establish that the capability exists.
- Consumers retain measurement attribution, units, labels, platform sampling,
  clocks, budgets and lifecycle. Metrics owns recording and its internal
  arithmetic; Canic's metrics adapter owns endpoint authorization, transport DTOs,
  Wire/TypeScript projection and aggregation across the application tree.
  A report consumer must not depend on the recording engine merely to decode or
  validate a report. Metrics itself must not depend on Canic or Wire for export.
  Governed persistence belongs to Storage's `ic-metrics-storage` adapter. Preserve
  supported consumer behavior and stored formats when replacing an existing
  engine, then delete the replaced copy.

## IC application encoding

- Enforce the lexical application-type boundary with the shared
  [Candid source check](../docs/candid-boundary.md), using reviewed declaration
  allowances for named external protocols and codec comparisons. Review macro
  expansion and cross-module aliases separately; the check is not type resolution.
- **Use `ic-wire` for all application endpoints we own**, including public
  endpoints called by third parties, owned canister RPC and frontend/client
  payloads. Caller location or endpoint visibility does not select the codec.
  Reuse Wire's encoding, decoding and generated bindings.
- **Do not derive or implement `CandidType` on owned application types.** This
  includes domain/state records, storage models, reports, and owned request/reply
  DTOs. Conditional derives, optional Candid features, generic trait bounds and
  Platform re-exports do not make an owned type an external protocol. Use Wire
  and its normal Serde contracts for our payloads; do not maintain dual-codec
  application types merely for convenience.
- Candid adapts independently defined interfaces that require it, such as
  management/system canisters, NNS/SNS and ledger protocols. A third-party caller
  of our application is not itself an external Candid protocol. Record the
  required interface; existing derives or a Candid-first example are not reasons
  to retain it. Required external Candid dependencies still follow the
  [IC Platform boundary](cargo-dependencies.md#external-ic-dependency-boundary).
- Reuse qualified external DTOs supplied by Platform before defining another
  Candid projection. When a required external interface needs a local Rust shape,
  keep it in that external adapter and explicitly convert to owned types. Record
  the named protocol and actual encoder/decoder that requires it. A Rust struct
  written in our repository can mirror an external contract; that narrow role
  is different from giving an application model `CandidType`. Missing Platform
  exports belong in an owning capability issue, not a duplicate public facade.
- A missing released Wire capability requires an owning issue and qualification
  before adoption, or an expressly approved scoped exception. Do not introduce
  a local codec or a Candid fallback to bypass that work.
- **CBOR belongs to IC Platform's typed external-protocol boundary.** Consumers
  use its certificate, hash-tree, ingress-envelope, controller-leaf and other
  named protocol APIs. Browser consumers use the corresponding explicit
  `@tokostudios/ic-platform` exports. Do not add direct CBOR dependencies,
  generic CBOR imports/re-exports, arbitrary-value serializers, local parsers or
  copied protocol codecs outside that owner. This includes general-purpose
  packages such as `serde_cbor`, Ciborium and Minicbor, even though they are not
  IC-specific dependencies. Missing typed operations belong in a Platform
  capability issue; a generic serialization facade is not the replacement.
- Owned application CBOR payloads, persisted records, release plans and canonical
  hash inputs migrate to bounded Wire encoding in their product owner; do not
  move application models or commitment policy into Platform. Treat changes to
  encoded records, digests, signatures and retry identities as an explicit
  format/identity cut. Trace both producers and readers, retained artifacts and
  uncertain effects; define admission or retirement of old records before
  changing bytes. Preserve active recovery inputs and never silently reinterpret
  or regenerate them. Existing retained-byte obligations are migration debt,
  not permission for new CBOR formats or an implicit fallback reader. This rule
  does not replace human-authored configuration formats or external protocol bytes.
- CBOR ownership applies to production, tests, examples, benchmarks, generators
  and development tooling, including Rust and JavaScript/TypeScript. Use typed
  Platform fixtures for external IC packets and Wire for owned payload fixtures.
  Consumer code retains authentication, trust roots, budgets, semantic validation
  and effect/retry policy; decoding a packet does not authenticate it. Preserve
  exact external bytes, tags and certified-leaf commitments when adopting APIs.
  Codec-comparison or other temporary exceptions require explicit maintainer
  approval with a named scope, reason and retirement/review condition; fixture
  privacy is not an exemption.
- Consumer validation must reject forbidden direct codec dependencies and imports
  across normal, build, development, optional and target-specific declarations,
  independent fixture workspaces and browser roots. Include aliases, generated
  entrypoints and re-exports in coverage; review dynamic/generated uses beyond
  the checker's capabilities. Reuse shared dependency/import checks and improve
  their shared owner for missing coverage rather than copying scanners locally.
  Record narrow approved exceptions and reject stale allowances. Legitimate
  transitive codecs behind Platform and the specifically enumerated native
  Testkit exception remain permitted; that exception grants no direct CBOR use.
  Use the shared Cargo common inventory, optional Rust `--cbor` source mode and
  browser SDK/CBOR check together. Default Candid-only checking does not enforce
  CBOR ownership; none of these lexical checks proves external DTO provenance or
  byte compatibility. Passing checks and complete migration are
  separate claims; report uncovered roots and remaining source exceptions.
- Remove Candid derives, conversions and codec helpers from internal types when
  no remaining Candid contract needs them. Do not wrap an unchanged Candid payload
  in Wire merely to claim adoption. Storage layouts and canonical hash inputs
  retain their owning contracts. Existing persisted Candid checkpoint bytes are
  a compatibility obligation, not permission for new Candid storage formats.
  Isolate any still-required historical Candid schema at its legacy boundary;
  record its retained-byte obligation and retirement condition. Do not silently
  change existing stored bytes or erase uncertain effects to remove a derive.
- Tests, examples and generators follow the same owned-type rule. Dedicated
  external-provider fixtures and explicit Candid-versus-Wire benchmark controls
  may retain the Candid shapes needed for that protocol or comparison, isolated
  from production application types. A fixture's privacy is not an exemption;
  identify the external contract or measurement that requires the Candid shape.
- IC Wire owns codecs, resource admission and codec generation. Consumers retain
  endpoint exports, transport, authentication, method identities, semantic
  validation, rollback and retry policy. Qualify the selected released APIs and
  both ends of the actual integration, including zero-argument RPCs and lifecycle
  argument decoding. Explicit codec hooks alone do not prove Wire framing or
  binding conformance, endpoint behavior or instruction/size savings.
- Generated cross-runtime RPC clients derive byte, node and depth budgets from
  one product policy owner. Follow the [RPC budget qualification rule](rpc-budgets.md)
  for intentional asymmetry, generated-policy drift checks and actual runtime
  boundaries; equal numbers alone do not establish equivalent admission.
- Changing existing message bytes requires an explicit consumer migration with
  affected callers, generated bindings and retained retry bytes accounted for.
  Follow the [pre-1.0 contract rules](#pre-10-contracts); do not silently change a
  public interface or introduce parallel codecs/fallback readers for adoption.

## Wasm optimization

- Shared Tooling owns the common decision process and evidence requirements.
  IC Tooling's `ic-build` owns reusable Cargo/Wasm optimization mechanics and
  the canonical recommendations, recipe definitions and qualification evidence.
  Improve that owner when shared support is missing; do not establish competing
  general-purpose optimizer policies in consumers. Existing local adapters stay
  explicit until a qualified shared replacement is available.
- A dependency owner's required target/backend flags must be explicit inputs to
  the consuming build recipe, not assumed to propagate from that dependency's
  Cargo configuration. The build owner must compose existing flags, resolve
  environment/config precedence, reject contradictory selections and record the
  effective command. Qualify actual caller wiring and bind those settings to
  artifact provenance/cache identity; recommendation text alone is not adoption.
- Consumers own deployment limits, representative workloads, required Wasm
  features, interface/upgrade compatibility and release admission. They select
  a qualified recipe and record product-specific deviations with their reason
  and evidence. Cargo profile settings still belong in the consuming workspace;
  depending on `ic-build` does not inherit or enforce a profile. Shared recipes
  must expose their effective settings so local projections can be checked.
- Define the objective before selecting settings: raw Wasm/code-section limits,
  runtime instruction or cycle budgets, and the priority among them. A setting
  called `z`, `Oz` or `O3` is not proof of an optimum. Prefer a reproducible,
  qualified choice; do not silently search for and change the recipe on each
  release.
- Wasm footprint comparisons, benchmark summaries and optimization acceptance
  report two separate metrics from the same final uncompressed module: total
  module bytes (all sections) and code-section bytes. Show absolute before/after
  values and byte deltas for both; percentages may supplement them. Label units
  explicitly, using bytes and optionally MiB (1 MiB = 1,048,576 bytes). If either
  measurement is unavailable, say so; do not substitute compressed size or infer
  code-section size from total size. State the code-section measurement method
  when reporting headroom against the IC admission limit.
- The IC [resource limits](https://docs.internetcomputer.org/references/resource-limits/)
  currently list a 12 MiB (12,582,912-byte) code-section limit per canister
  (checked 2026-11-01). This is distinct from total module size. Verify the target
  network/runtime limit when making admission claims; compression cannot reduce
  the code-section size checked by the runtime.
- Do not use gzip, zip or other compressed byte counts to rank Wasm footprint,
  claim Wasm size savings or establish code-section headroom. Omit compressed
  sizes from Wasm comparison tables, progress updates and final summaries unless
  transport/compression measurement is explicitly requested. A requested transport
  comparison must be labelled separately and cannot replace either raw metric.
  This reporting rule does not remove compression or required upload admission
  checks from deployment tooling.
- Treat compiler optimization and post-link `wasm-opt` as separate stages of
  one recipe. Record compiler/target and optimizer identities, Cargo profile,
  LTO/codegen settings, ordered optimizer arguments, features and input identity.
  Compare final deployable artifacts under matched inputs; a post-link pass
  does not erase the effect of the compiler profile. Preserve required module
  contracts and run the relevant runtime/upgrade checks when settings change.
- Routine performance work uses the actual deployment recipe as its primary
  measurement. Library benchmarks name the consumer recipe they represent and
  state any missing deployment stages. Keep correctness controls for matched
  logical values; additional profiles answer a named comparison question.
  Do not require an exhaustive optimization matrix in ordinary consumer CI.
- Reconsider recommendations when compiler/optimizer changes, workload changes
  or observed regressions justify it. Use a small explicit candidate comparison,
  widening it only when the result needs it. Retain absolute final sizes and
  runtime costs, tradeoffs and limitations at the owning recipe. A codec beating
  another codec under two profiles does not choose between those profiles.
  Tool upgrades or changed recommendations require consumer qualification before
  adoption; they do not silently rewrite deployment settings.

## Gzip encoding

- IC Tooling's `ic-host-artifacts::artifact::encode_gzip`, behind its `gzip`
  feature, is the canonical gzip encoder. General gzip compression belongs in
  Tooling; it does not move into IC Platform merely because IC artifacts use it.
- Reuse that API in production, tests, fixtures, examples and generators rather
  than constructing another `flate2` encoder or copying header/compression logic.
  A local adapter is justified when it supplies product policy or necessary
  translation; it must delegate encoding to the canonical helper.
- Consumers own the explicit compression level, compressed-byte allowance,
  input custody, staging and publication. Ordinary fixtures use the fastest
  setting that preserves the tested property, normally level 1. Do not run a
  production level such as Canic's level 9 merely to construct valid gzip input.
  Use the product's adapter/settings when testing exact compressed bytes/hashes,
  release parity or compression-sensitive limits; keep explicit level/backend
  coverage at the codec owner. A stored member is suitable only when the test
  does not require actual size reduction.
- Keep independent encoder controls only for a named backend/interoperability or
  noncanonical-format test. Document the property they prove and isolate them
  from ordinary fixture construction. System archive tools used for bootstrap,
  packaging or evidence remain infrastructure dependencies; do not use them as
  alternate application or Wasm-artifact encoders.
- Zero timestamps alone do not make compressed bytes canonical across backends,
  feature selections or dependency versions. Before changing compression inputs,
  check frozen hashes, retained artifacts and external compressed-byte contracts.
  Preserve obligated bytes or explicitly qualify their migration; do not silently
  rewrite expected hashes to make a test pass. Exact emitted-byte hashes remain
  valid artifact identities; they are not interchangeable with raw-payload hashes.

## Complete, reviewable changes

- Ordinary continuation resumes the accepted coherent in-repository batch through
  its directly required implementation, focused checks, propagation and cleanup.
  Do not impose a one-slice-per-turn stop. Stop at new independent scope or a
  release boundary; continuation does not authorize starting another release line.
- Deliver one coherent outcome with its directly required implementation,
  rejection/recovery evidence, callers, fixtures, documentation and cleanup.
  Split independent outcomes, not compiler fallout or each proof of one change.
- Scale the handoff to the change: lead a small fix with its practical effect,
  focused verification and actionable blocker, linking detailed evidence at its
  owner. Keep independent dependency updates separate even within one release.
  Inspect newly discovered concerns before expanding the accepted repair scope.
- Contract changes must trace producers, consumers, codecs, generated artifacts,
  persisted data and installation/recovery helpers. Reuse the canonical encoder
  rather than reconstructing its payload in another path.
- Remove superseded paths completely when their obligations permit retirement.
  Name any remaining consumer or deployment blocker and the evidence needed to
  close it. Do not leave replacement and old implementation indefinitely active.
- Keep completed reports proportional under the [change-reporting rules](change-reporting.md).
  Lead with the outcome, relevant verification and actionable blockers. Detailed
  line metrics and symbol inventories are opt-in; routine work and inspection
  do not require them. Public API removals and consumer actions still belong
  in their normal documentation.
- Complete changelog maintenance under the [common changelog rules](changelogs.md)
  as part of the coherent batch, including automatically selecting and maintaining
  the numbered next release at the top. Do not wait for a separate notes request.

## Pre-1.0 contracts

- Pre-1.0 is a hard cut: maintain one current contract. No deprecated aliases,
  compatibility shims, dual readers/writers, legacy fallbacks, staged deprecations
  or migration engines for superseded repository-owned models.
- Repository-owned models are unversioned or V1 before 1.0. No V2 or higher,
  parallel aliases or hidden versions such as Next/New/Legacy/Compat or numbered
  modules. This does not prohibit package versions or external standard versions.
- Breaking public API or semantic changes require a minor release before 1.0;
  a hard cut does not make an incompatible patch acceptable. Select the pending
  changelog version automatically under the common rules; changing package
  versions or executing a release remains an explicitly authorized consumer action.
- Classify compatibility at the released package's own consumer boundary. An
  upstream minor/hard cut does not automatically require downstream minor bumps.
  Contributor-only CI, hooks, formatting, tool installation and local PATH changes
  are compatible for a library whose published API, behavior and supported build
  requirements remain unchanged. Shared Tooling's commands are its product
  interface; adopting their changes does not turn them into a library API break.
  Apply the [package-impact rules](changelogs.md#classify-the-released-packages-own-impact),
  including exposed dependency types and coordinated workspace versions.
- Never reuse a wire/storage discriminator for an incompatible layout or interpret
  retained bytes under a new layout merely because both contracts use V1.
  A hard cut identifies the frozen format,
  producers, consumers and retained installations, and coordinates their update
  and explicit retirement/reset disposition before replacing the current contract.
  Pre-1.0 is not proof that stored data or external consumers are disposable.
- Hard cuts do not remove same-contract interruption recovery, backup/restore,
  uncertainty or external obligations. Never discard the only record of effects,
  assets, balances or liabilities merely to remove old source. Product-specific
  transition and retirement procedures remain local; do not invent a migration
  engine or another reader to avoid resolving their prerequisites.

## Validation and evidence

The [core validation policy](../DRAGGINZGAME.md#validation-and-evidence) owns
selection, execution cadence, evidence reuse and asynchronous CI. Read this
section when changing release validation, dependency preparation or evidence
contracts; ordinary development does not require rereading these mechanics.

- **Rust release and deployment entrypoints must run Clippy early and stop on
  failure.** After source/recovery admission and required compiler/dependency/tool
  preparation, run the consumer's existing Clippy gate before expensive product
  builds, test suites, packaging or new publication/deployment effects. Preserve
  its selected packages, targets, features and lint policy; a narrower ad hoc
  `cargo clippy` invocation does not replace that gate. Establish ordering with
  sequential recipes or dependency edges that also hold under parallel Make.
  A failed lint must prevent later stages, and a passing lint does not replace
  the remaining complete gate or existing recovery obligations.
- Give each validation obligation one execution owner per admitted attempt and
  configuration. Aggregate CI/release recipes must not repeatedly invoke full
  lint, setup or build gates through nested product targets. Keep standalone
  commands safe by sharing prerequisite dependencies or delegating to admitted
  leaf stages; parallel Make must preserve ordering and avoid duplicate work.
  Prepare shared test artifacts once for their exact inputs, then let applicable
  suites consume them. Different features, targets, hosts, archives and installed
  contracts remain distinct obligations.
- Do not follow Clippy with `cargo check` merely to repeat the same compilation.
  Compare package, target, feature, profile, compiler/configuration and generated
  inputs, and remove the duplicate stage only after qualifying that it owns no
  distinct obligation. Default-feature, Wasm, separate host and archive checks
  remain when they establish different contracts. Keep early fail-closed lint.
- A later release or publication step must freshly admit its inputs, but need
  not automatically repeat every earlier check. Reuse only through the qualified
  [stage evidence contract](../docs/release-validation-reuse.md); missing or
  changed evidence requires execution. Neither a successful earlier group nor
  a version-only diff proves reuse is safe. Preserve actual archive verification,
  registry/source identity and recovery checks at their owning boundaries.
- Run tooling regression fixtures when their implementation, callers or covered
  contracts change, or when explicitly qualifying them. Do not attach a tool's
  complete self-test suite to every ordinary use of that tool. For example,
  verifying each release archive remains required where selected, while rerunning
  the archive verifier's synthetic regression matrix needs its own applicability.
  Keep those regressions in their owning CI roster and qualify any new selector.
- Check for active builds before compilation or source mutation that could affect their inputs. Do not change
  source under active validation or compete for its build lock. Use the owning
  repository's build directory; preserve unrelated artifacts.
- Prepare locked dependency sources under the
  [Cargo network policy](cargo-dependencies.md#cargo-network-policy). Authorized
  preparation and dependency updates need no repeated network permission;
  explicit offline settings and execution-environment restrictions still apply.
  Prepare selected executable tools separately through the
  [release/setup contract](../docs/releases.md), before dependent validation.
  Offline checks and standalone read helpers must not install or fetch implicitly.
- Test maintained observable behavior, typed failures and genuine architectural
  boundaries. Delete tests that only prohibit a removed name or remember an old
  implementation. Source inspection can enforce a live architectural invariant;
  private layout, error prose and fixed test counts are not behavior contracts.
- Bind evidence to its actual source, inputs, dependencies, artifacts and commands.
  Distinguish source review, mocks/substitutes, local execution and live observations.
  Retain failed/inconclusive attempts and limitations; a rebuild or new release
  never relabels old evidence. State measured results separately from expectations.
- Where external effects need interruption recovery or reconciliation, persist
  intent and exact identity before dispatch; define precise retry/replay semantics.
  A lost reply is not proof of failure and does not authorize repetition. Preserve
  required recovery without adding journals to tools that do not need them.
- Enforce structured facts, schemas, hashes and executable behavior in guards.
  Do not freeze explanatory prose or require manual status-marker rotation.

## Release commands

- Grouped Cargo increment commands capture frozen inputs by default; an explicit
  `RELEASE_GROUP` selects one group, and multiple groups require an explicit roster.
  Keep publication on that captured source so concurrent
  normal-checkout lock updates belong to the next batch. Use Shared's
  [batch setup, recovery and custody interfaces](../docs/release-batches.md),
  rather than adding local orchestration wrappers. Qualify the consumer's real
  hooks before activation; an imported helper alone does not complete adoption.
- Every `dragginzgame` repository, including Shared Tooling, must expose
  `make release-patch`, `make release-minor` and `make release-major`.
  All three use the same [release contract and Makefile pattern](../docs/releases.md):
  preflight, validate, bump and finalize the changelog, stage and commit, then
  deliver and tag through the repository's explicitly selected direct or PR policy.
  Only the selected semantic-version component differs between the three targets.
- Keep one release workflow per repository. The three entry points must delegate
  to it; do not give patch releases an implicit fast lane or different effects.
  Consumer-owned validation gates, metadata files, branches, remotes and delivery
  policy are explicit inputs. Direct delivery remains the
  default; PR delivery prepares a review branch and validates the admitted merged
  commit afresh before tagging. Never select another policy to bypass a rejected
  push or branch protection.
- Independent release groups may select a complete, consumer-owned gate under
  the [group validation contract](../docs/release-group-validation.md). Account
  for every stage, including affected consumers, public identities, generated
  inputs, native hosts and installed behavior. Shared changes broaden the plan;
  unknown scope refuses or uses the existing full gate. Keep full repository CI,
  source/archive admission and recovery intact. Group selection alone grants no
  omission; qualify the mapping before enabling it in a release adapter.
- Keep group validation modular under the
  [shared-mechanics boundary](../docs/release-group-validation.md#shared-mechanics-and-local-policy).
  Consumers own product mappings and one full-stage roster; Shared owns reusable
  dispatch, checkpoint and evidence mechanics. Raise missing shared capabilities
  at their owner instead of multiplying local frameworks. Full CI must not loop
  over group gates and repeat common checks; full-versus-selected qualification
  comparisons must have an explicit lifecycle rather than run indefinitely on
  every ordinary change. Preserve distinct host/feature obligations and existing
  admission until an equivalent replacement is qualified.
- Standard release entrypoints must select their required prepared executables
  through the consumer's offline admission interface; do not require maintainers
  to export tool paths in each shell. Fresh preflight prepares missing selections
  only after source/recovery admission. Recovery admits the saved source's
  selection without installing or resolving partially prepared live metadata.
  Low-level helpers may require explicit paths supplied by their calling adapter.
- Agents may run these commands under a release/ship/deploy request for the scoped
  repository and established destination, choosing affected groups and semantic
  increments under [delegated delivery](contributions.md#delegated-delivery).
  That request includes the documented gate, version preparation and selected
  delivery effects. Direct delivery includes the release commit, tag and atomic
  branch/tag push; PR delivery includes the release branch/PR and, after an
  authorized merge, fresh merged-source validation and its exact tag push.
  The runner never merges or approves the PR. A full delivery request may cover
  the agent's merge and subsequent publication; invoking only a release command
  does not expand its effects. Fix, commit, PR, preparation and inspection requests
  retain their narrower scope.
- Use standard semantic-version increments: patch increments the patch; minor
  increments the minor and resets the patch; major increments the major and
  resets both lower components. Apply the pre-1.0 compatibility rule above.
- Stop on failure and preserve build and evidence artifacts. **Normal
  `release-patch`, `release-minor` and `release-major` always start fresh from
  current clean, committed source and canonical metadata, with full preflight
  and validation. Never scan historical plans, reuse their validation or replay
  their commit/tag/push effects as part of a normal release.** A prior plan's
  phase, source, destination or malformed contents cannot block a fresh release.
  The new candidate's stale local plan/staging/PR receipts may be discarded;
  old plans are not a required historical archive. Keep build and validation
  evidence attributable to its original attempt.
  Persist exact intent before preparation may begin. Recovery of that specific
  attempt is explicit through `release-resume VERSION=X.Y.Z`, bound to its saved
  source, version, payload and destination. PR recovery remains pending/nonzero
  until merge and fresh complete validation of the admitted merged commit; late
  evidence checks use `RELEASE_COMMIT`, which may precede HEAD. Current dirty
  source/index, occupied locks, candidate ref/payload, destination and branch
  protection conflicts still stop the new release. Missing older tags or
  registry/Git delivery disagreement do not establish such a conflict.
  Do not force-push, overwrite tags, silently bump again or add implicit package
  publication, deployment or post-release cleanup.

## Changelogs and artifact preservation

- **Git retains decisions and compact results, not an archive of agent runs.**
  Routine validation needs no new evidence directory or report: use the owning
  changelog/issue and existing CI result. For a useful performance study or design
  investigation, maintain one current summary per topic, normally at most 150
  lines, recording the conclusion, limitations, source/tool identities, commands
  and selected absolute results. Update it instead of making a directory for
  each run, dependency adoption or patch release; Git history preserves revisions.
- Default tracked experimental evidence budget: one summary plus at most two
  small text/data files per topic, **100 KiB combined per topic and 5 MiB across
  the repository**. These are ceilings, not targets. Do not commit raw run logs,
  bulk measurement tables, source-tree snapshots, build outputs, Wasm binaries,
  or compressed evidence bundles (`zip`, `tar.gz`, `xz`, etc.). Compression does
  not make bulk evidence appropriate for Git. Maintained test vectors and source
  fixtures belong with tests and require a concrete regression purpose; do not
  rename historical output as a fixture to bypass this rule.
- New raw experimental output belongs under `target/evidence/`, removable by
  explicit `cargo clean`. Retain it while investigating a failure or preparing
  the compact result; it is not a permanent archive. No automatic copying into
  docs, another directory, Git history or remote storage. A claim requiring
  durable raw evidence needs an explicit exception naming the claim, minimal
  payload, measured size, owner, destination and retention/review period. Small
  release receipts and active deployment/recovery records retain their separate
  contracts; this experimental budget does not discard them.
- For existing evidence excess, inventory and compact it under an authorized
  cleanup. Preserve useful conclusions and repair references. Retire obsolete
  claims or state that raw measurements are no longer retained; never present a
  summary/hash as proof that missing artifacts remain independently verifiable.
  Superseded exploratory output does not need another permanent archive merely
  because an agent once produced it. Ordinary Git deletion leaves history intact;
  history rewriting, remote archival and deleting active recovery evidence need
  their own explicit scope. A new rule alone does not authorize sibling cleanup.
- Before editing a changelog, read and apply [the changelog rules](changelogs.md).
  They are part of this mandatory baseline and own release-note content, issue
  links, draft/history preservation and detailed-note structure.
- Releases and deployments preserve consumer-owned build and evidence artifacts
  on success, failure and retry. Never append cargo clean or equivalent cleanup.
  Cleanup requires explicit scope and authority. Tools may clean temporary files
  they own; they must not erase consumer artifacts or required evidence outside
  the explicit cleanup scope. Preservation during delivery does not require
  committing raw artifacts or retaining every completed experiment forever.
- Managed tool setup retains current declared requirements across consumers,
  selected deliverables, small provenance receipts and explicitly needed recovery
  evidence. Successful builds discard their private compiler scratch after byte
  admission, while installation custody is still held. Historical selectors and
  automatic previous-version fallbacks are not permanent liveness roots. Use the
  producer's [store maintenance contract](../docs/tool-store-maintenance.md),
  including expiring failed attempts, recovery pins, preview, physical-byte
  accounting and explicit quiescence where executable users cannot be leased.
  Updating one consumer must not upgrade or reclaim another consumer's selection.
- Product owners declare native cache roots and age/size budgets, separate final
  deliverables and receipts from incremental/compiler output, and enforce those
  budgets at a serialized maintenance boundary. An active build or recovery pin
  prevents reclamation, not an unlimited retention exemption after work finishes.
  Adopt shared mechanics rather than adding consumer-specific deletion scripts.
  Legacy stores, unknown ownership and unrelated files require explicit migration;
  a policy update alone does not authorize a one-off machine cleanup.
- **Git administrative directories are not build, scratch or checkout-archive
  storage.** This covers both the Git directory and common directory, including
  `.git/release-state` and `.git/publication-state`. Keep compact recovery journals,
  receipts and necessary metadata there; put reproducible source extracts and
  compiler outputs under the owning workspace's `target/`. A copied source tree
  must not accidentally build into a nested `target/` beneath `.git`.
- Retiring a checkout must reclaim its redundant source/build storage, not move
  it into `.git/retired-checkouts` or an equivalent hidden archive. Preserve only
  verified unique work and required evidence through the durable-work contract.
  If a full archive is genuinely required, disclose its measured size, approved
  destination, owner and expiry before creating it; cleanup authority does not
  implicitly authorize a new hidden bulk archive.
- Writers of agent/tool-owned Git-side state must define and enforce an aggregate
  byte budget and retention lifetime, report retained size and reclaimable bytes,
  and refuse additional bulk retention when the budget would be exceeded. Keep
  active recovery pins explicit and bounded by an owner/review date. Failed or
  completed attempts must not accumulate complete source/build copies forever.
  Missing retention controls are an implementation gap, not a reason to add
  another bespoke cleanup script or silently delete evidence.
- Git storage audits distinguish Git objects from tool-owned files, inspect the
  actual Git/common directories and report large retained roots. `git gc` does
  not reclaim arbitrary release directories, and `cargo clean` does not clean
  `.git`. Never prune refs/reflogs, rewrite history or delete active recovery
  state merely to reduce a reported size. Legacy cleanup requires scoped review
  of active users, unique source and evidence before removal.
- Explicit workspace cleanup is the disk-reclamation boundary. With no operator
  overrides or package/profile/target selection, `cargo clean` must remove the
  owning workspace's entire `target/`, including nested compiler outputs, generated
  build inputs, Wasm caches and retained evidence. Do not redirect the repository's
  default Cargo target to a child such as `target/cargo`, to a shared sibling
  directory, or outside the workspace. Prefer Cargo's built-in default over a
  `build.target-dir` setting that changes inherited nested-workspace behavior.
- Build commands may select isolated outputs beneath `target/` where required,
  but a repository's general `make clean` must still clean the whole workspace
  `target/`; inherited build settings must not silently narrow it. Use Cargo's
  existing target selection, not a wrapper or replacement for `cargo clean`.
  Clearly named selective cleanup commands may keep their documented scope.
  Operator-selected Cargo environment variables and CLI filters retain their
  normal meanings; do not override the user's Cargo installation or global config.
- Workspace-owned scratch belongs beneath the owning workspace's `target/`,
  normally `target/tmp/`, including generated fixture projects, temporary build
  trees and intermediate downloads. Create unique per-run directories there so
  explicit workspace cleanup reclaims them along with compiler outputs. Do not
  default to repository `.tmp/`, `tmp/` or system `/tmp` merely for convenience.
- Scratch is not source custody. Lasting implementation, unique patches and
  contribution checkouts follow the [durable work contract](contributions.md#durable-work-and-checkout-location)
  and must survive routine deletion of `/tmp`, caches and `target/`. Keep actual
  changes in the requested checkout or an authorized durable contribution; ask
  before choosing another handoff location. Scratch exceptions below do not
  authorize leaving unique code changes there.
- Use outside-target scratch only when the operation needs it, such as an atomic
  replacement on the destination filesystem, a fixture that removes `target/`
  itself, or evidence explicitly required to survive cleanup. Document the reason,
  ownership and cleanup/retention boundary alongside the caller. A `.tmp` filename
  used for an atomic write is not a general scratch directory. Keep overrides
  local to the command; do not globally change `TMPDIR` or another workspace's
  output location. Repositories without Cargo use their documented artifact root.
  Existing outside-target artifacts still require scoped cleanup authority;
  adopting this rule does not authorize deleting or relocating retained evidence.
- Consumer artifact preservation does not prohibit user-requested cleanup or the
  admitted managed-cache lifecycle above. Do not run
  cleanup concurrently with builds. Evidence that must survive an explicit clean
  belongs outside `target/`; document any separate tool stores or caches rather
  than implying Cargo cleans them. Qualify cleanup changes with disposable
  fixtures, never by deleting a working checkout's artifacts as a test.

## IC stable-memory governance

- `ic-storage` owns allocation governance, backing custody, exclusive leases and
  governed collections. Production applications obtain checked allocation handles
  only after admission and durable commitment. Application schemas, lifecycle
  and recovery decisions remain with their owning products.
- Applications must not assign allocation IDs, create a second manager/runtime
  over the same backing or retain an unmanaged store-access path. The host supplies
  backing at its owned Storage integration boundary; raw backing access belongs
  there and inside Storage, not in ordinary application stores.
- Storage integration must preserve validation and commitment before opening
  stores. A generic engine accepting a `Memory` trait still needs checked
  Storage-owned handles at its production integration. Inspect actual construction
  and access, including re-exports; imports or dependency bans alone prove nothing.
- Isolated raw-memory tests, benchmarks and infrastructure internals may use
  scoped unmanaged backing with a documented purpose. They do not qualify a
  production integration. Other production bypasses require an approved exception
  naming the scope, reason and qualification.
- `ic-memory` is the retired allocation owner. This rule describes the current
  Storage boundary, not permission to upgrade dependencies, reset backing or
  migrate retained data. Existing deployments keep their explicit recovery and
  migration obligations; adopting documentation does not establish code adoption.

## Rust workspaces and portable tooling

- Governed tools target latest stable during authorized maintenance, while
  builds keep exact reviewed versions and checksums. Retaining an older version
  needs a documented compatibility blocker and owning follow-up. Use the
  [tool freshness workflow](../tasks/cargo-tool-versions.md), including optional
  sccache; never resolve latest dynamically in builds or create a second installer.
- Directory names for repositories, workspaces, tooling and operational inputs
  must not contain line-feed (LF) or carriage-return (CR) characters, including
  ancestor directories and resolved symlink destinations. Do not create or select
  such directories for normal work. Reject invalid paths before trimming or
  normalizing them; never silently select a different path. Deliberate negative
  test fixtures may contain forbidden names. Preserve existing artifacts and
  evidence with those names; this rule does not authorize renaming or deletion.
- Provide [local developer setup](../docs/local-setup.md) through explicit
  `make install-tools` and offline `make tools-check`, following the
  [required tool inventory](../docs/local-setup.md#required-tool-inventory).
  Every repository prepares the same common host and Cargo-tool sets. IC
  executables are product selections through the existing ordered local target
  lists; repositories without an IC executable caller need not install them.
  The host set includes pinned jq,
  Mike Farah yq, ripgrep with PCRE2 and cloc under `.tools/host/bin`; the Cargo
  set includes cargo-sort and cargo-sort-derives under
  `.tools/rust/common/bin`. Common and selected Cargo tools share versioned-slot
  receipt and byte admission; common PATH activation requires the complete set.
  Candid extractor selection belongs to products that extract declarations, using
  the selected Cargo-tool interface and publishing its admitted path explicitly.
  Aggregates run host and Rust steps, then selected product tools, stopping on
  failure. Consumer-specific setup/check targets extend the ordered lists in
  `make/tools.mk`, not unordered aggregate prerequisites. Make/CI callers select
  their own checkout's local paths; document interactive shell PATH setup,
  prepared Rust toolchains and system bootstrap packages separately.
  Adopt the [consumer Make interface](../docs/make-interface.md) from the reviewed
  snapshot for common commands and LOC reporting, replacing copied recipes;
  CI calls the same targets. Keep product inputs and adapters local. Use
  declarative Rust formatter inputs for extra workspaces, derive sorting and
  manifest order. Use paired custom commands for other policies/frontends
  instead of replacing shared recipes; preserve all checks during adoption.
- Shared Tooling owns common executable storage as well as installation commands.
  Its reviewed snapshot also owns the default common tool versions and digests.
  Adopt those catalogs with the installers; do not maintain independent default
  copies. A consumer exception requires an explicitly selected catalog and a
  documented reason and native qualification. Host, IC and Rust exceptions are
  independent. Product tools and PocketIC selection stay local; compiler setup
  implements the shared [support policy](cargo-dependencies.md#minimum-supported-rust-version-msrv).
  Host, IC and Cargo installers share verified bundles per user across checkouts
  while retaining each repository's pinned selection. Cargo tools use the standard
  tool-build environment and exact build identities; application compiler flags
  and wrappers remain repository-owned and do not configure these tool builds. Consumers
  must not create independent stores or new bespoke `.tools` installers. Follow
  the [shared storage adoption boundary](../docs/local-setup.md#shared-executable-storage)
  through reviewed canonical tooling; `.tools/host` and `.tools/ic` select shared
  bundles without storing new executable copies in the checkout. This does not
  authorize deleting
  existing installations or replacing them with a mutable sibling PATH.
- Repositories using IC executables select the [local IC setup](../docs/ic-tools.md) through
  `make install-ic-tools` and offline `make ic-tools-check`, declaring the needed
  executable names in `IC_TOOLS` before including the shared Make interface.
  Standalone callers supply repeated `--tool NAME` arguments; no complete bundle
  is selected implicitly. Keep selected common tool names available under the
  checkout's `.tools/ic/bin`, with one reviewed pin
  matrix and explicit installation. Consumers own version qualification and
  scoped pin exceptions; ordinary validation never downloads tools implicitly.
  PocketIC-specific selection, provisioning, offline admission and lifecycle
  belong to IC Testkit. Consumers use its selected setup/check contract rather
  than a separate server pin catalog or Shared Tooling's IC executable bundle.
- Apply the [dependency pinning rules](dependency-pinning.md): immutable
  Git/action identities, compatible registry requirements with locked builds,
  verified tool downloads, and explicitly qualified sibling or moving inputs.
  Run the declaration checker in CI and release gates; consumers own the chosen
  versions, approved exceptions and runtime qualification evidence.
- Follow the [upstream maintenance and recovery rules](dependency-pinning.md#maintaining-upstream-code)
  for imported runtime and cryptographic implementations. Preserve provenance,
  review applicable fixes, and distinguish source availability from demonstrated
  recovery. Product-specific pins, fixtures and recovery procedures stay local.
- Use the [standard Rust workspace layout](rust-workspaces.md): a virtual
  repository-root workspace, including single-package repositories. Maintained
  packages use `crates/<package-name>/` or application-owned `apps/<app-name>/`
  trees, including component packages grouped beneath an App. Both inherit from
  the same root. Approved independent workspaces use those shapes relative to
  their own roots; other layouts need explicit exceptions.
  The rule explicitly approves IcyDB's existing `crates/`, `canisters/`, `schema/`
  and `testing/` layout; adoption does not require relocating those packages.
  Repositories without Rust packages do not need a Cargo workspace.
- Keep related crates in one development workspace; release groups own their
  coordinated versions under the
  [workspace versioning guidance](rust-workspaces.md#coordinated-versions-and-workspace-boundaries).
  Independent groups all use literal package versions; omit the root
  `workspace.package.version`. Require explicit group selection for release,
  resume and publication. Document the group map in root `Cargo.toml` comments and use
  [group changelogs and retained history](changelogs.md#release-group-changelogs-and-shared-history).
  Do not move packages or synchronize unrelated releases merely for versioning.
  Apply the [core and integration graph boundary](rust-workspaces.md#core-and-integration-graph-boundary)
  when adapter or qualification dependencies block an otherwise valid core upgrade;
  independent release versions do not isolate Cargo resolution or override that boundary.
  Apply the
  [Cargo dependency rules](cargo-dependencies.md): every direct dependency
  is declared in root `[workspace.dependencies]`, and every child manifest uses
  `workspace = true`, including development, build and target-specific tables.
  Keep version/source selections in the root; children select target conditions,
  features and publication policy. Keep the
  [minimum supported Rust version](cargo-dependencies.md#minimum-supported-rust-version-msrv)
  aligned with the approved Rust 1.99.1 development/release compiler. Retire older
  compiler support and consolidate duplicate qualification under that policy,
  retaining distinct target, host, feature and consumer proofs. Future compiler
  changes require a maintainer decision and appropriate validation.
- Select the [Platform package owner](cargo-dependencies.md#platform-package-owners)
  matching the capability: Types for identities, Runtime for IC0/execution, and
  the Platform facade for crypto/external adapters. Keep externally maintained
  IC-specific dependencies, including DFINITY packages, behind the reviewed
  Platform boundary. Direct Runtime usage grants no raw SDK exception. Apply the
  [external IC dependency boundary](cargo-dependencies.md#external-ic-dependency-boundary)
  to production, build, test and tooling code, optional features and all targets.
  JavaScript/TypeScript SDK Core consumers use the separate npm package
  **`@tokostudios/ic-platform`**, through its explicit subpaths, under the
  [npm IC boundary](dependency-pinning.md#ic-javascript-and-typescript-dependencies).
  Rust-only consumers do not need an npm dependency; the two packages have
  independent release identities.
  Missing Platform support requires an owning Platform capability issue, not a new direct
  dependency or a local replacement facade.
- Follow the [Rust hygiene baseline](../docs/principles/rust-code-hygiene.md): narrow
  visibility, documented APIs/invariants, ordinary module discovery, bounded
  fallible decoding, typed errors and `std` paths in code that requires `std`,
  preserving documented `no_std` support. Exact edition, internal module layout and
  lint choices stay local within the workspace rules. Do not fake platform
  behavior with production cfg(test) paths.
- Rust repositories adopt the [standard formatting hook](git-hooks.md):
  `make install-hooks` enables a reviewed repository-local pre-commit hook that
  auto-formats and refreshes only selected files, rejects partial staging and
  preserves unrelated working edits. Formatting includes `cargo sort --workspace --grouped`
  for root and child manifests before Rust formatting. Keep matching `fmt-check`
  sorting and Rust checks in CI and release gates;
  hook activation and independent formatting qualification are separate checks.
- Keep shell wrappers small and declare dependencies and effects. Use the
  project's maintained implementation language for substantial tooling. Do not
  add Python tooling. Fail on invalid inputs rather than hiding broken assumptions.
- Do not use directory paths containing carriage returns or newlines (CR/LF)
  for checkouts, tooling, state or artifacts. Reject those inputs before trimming
  or normalizing them, and check resolved directory aliases as well. Never repair
  an invalid path by stripping characters: that can select a different directory.
  This restriction does not prohibit ordinary spaces in paths.
- CI/release tools use [reviewed snapshots](../docs/consuming-snapshots.md), not symlinks
  or mutable sibling/network sources. Do not patch vendored copies; fix upstream
  or keep an explicit consumer-owned adapter outside the snapshot.
- Portable scripts use Bash strict mode and preserve Bash 3.2 compatibility unless
  the support matrix is deliberately changed. Downloaded executables need exact
  reviewed snapshot versions or explicit consumer exceptions, pinned digests,
  HTTPS, verification before extraction
  and a version check before installation. No implicit credentials or targets.

## Host support

- Every `dragginzgame` package must work on macOS. This includes its applicable
  dependency setup, native tools, build, test, CI and deployment workflows.
  For canister and frontend packages, support includes running their host
  workflows on macOS; product runtime targets remain locally owned.
- Consumers declare supported macOS versions and architectures in their local
  host matrix. Missing coverage or a macOS failure is a support gap to fix,
  not permission to classify macOS as unsupported. Exceptions require explicit
  maintainer approval with scope and reason.
- Host-specific dependency installation, CI setup and deployment commands may
  differ while preserving the same product contracts, validation obligations,
  authorization, recovery and artifact preservation. Document prerequisites and
  isolate host differences at their owning boundary; do not assume Linux tools,
  paths, package managers or GNU behavior are available on macOS.
- Qualify supported hosts through native CI or recorded native execution of the
  relevant workflows. Linux passes, cross-compilation and installer branches
  alone do not prove macOS behavior. Align evidence with
  [the host guidance](../docs/supported-hosts.md) and the consumer's matrix; report
  outstanding qualification without weakening the support requirement.
- Routine CI keeps only the newest run per workflow and branch/PR ref. Use a
  stable workflow/ref concurrency group with `cancel-in-progress: true`, without
  source SHA or run ID, so newer revisions cancel queued and running older checks.
  Preserve all required hosts on the retained run. Cancelled checks do not qualify
  their source; release, publication and deployment workflows retain separate
  effect/recovery obligations. See [the CI policy](../docs/supported-hosts.md).

## Feedback and handoff

- Use the [repeatable task catalog](../tasks/README.md) for named maintenance checks
  and bounded recurring audits. Shared Tooling owns the procedures; consumers
  own local inputs and validation contracts. Scheduled execution requires explicit
  activation and retains the task's inspection/repair boundaries.
- Follow the [user-triggered agent maintenance rules](agent-maintenance.md)
  when asked to check CI, review issues or inspect for work after completing a
  task. Session activation carries forward within its scope; inspection and
  repair requests retain their distinct authority.
- GitHub issues in the owning repository are the sole tracker for bugs, feature
  requests, review findings, reusable gaps and follow-up work. Search existing
  issues before filing; update matching evidence rather than creating duplicates.
  Do not create or maintain local upstream issue files, feedback ledgers,
  project-prefixed issue IDs or parallel tracking queues. Handoffs and design
  documents link to GitHub issues without duplicating triage or issue status.
  Supporting evidence stays with its existing owner; it is not another tracker.
- A reusable-gap issue records the reviewed upstream revision, affected owner,
  symptom, focused evidence, smallest proposal and disposition. Identify dirty
  source separately from a committed revision. Include affected callers/hosts
  and the smallest useful verification.
- Track upstream acceptance separately from verified consumer adoption. Close
  producer issues when their own delivered implementation and required checks
  are complete; keep concrete, independently actionable adoption work in linked
  consumer-owned issues. Shared waiting findings follow the rule below.
  Do not keep or reopen a completed producer issue solely for fleet adoption,
  or create a second open tracker for the same consumer obligations. The
  [issue maintenance procedure](agent-maintenance.md#issue-review-and-repair)
  defines closure evidence and retirement of redundant coordination trackers.
  Resolve product-specific feedback locally instead of promoting it to universal
  policy.
  For another repository's finding, search its issues and file or update the
  matching issue under the issue authorization above. Include a concrete fix or
  patch where feasible and its actual validation results. Keep shared snapshots
  intact; repair at the source owner and adopt a reviewed committed revision.
  Source repairs and PRs require explicit scope under the isolated contribution
  workflow; routine adoption stays with the receiving owner and sibling working
  checkouts remain untouched.
  If issue access or a remote is unavailable, report the finding and blocker to
  the maintainer without inventing an issue URL or creating a local tracker.
- Report the outcome, changed files, relevant verification, skipped checks,
  remaining risks and open consumer actions. State compatibility and host impact
  when relevant. Do not claim a release proves publication, adoption or deployment.

### Shared dependency findings

- A pending sibling release, registry publication, tag or deployment is a user
  coordination matter, not a new engineering issue. Do not create or reopen
  supplier, consumer or Shared issues, or post repeated release requests, solely
  because a needed version is not yet available. This includes "publish the fix",
  "release group X" and "blocked until sibling Y deploys" trackers, even when
  the dependency genuinely blocks the current task.
- Report that blocker directly to the user: name the repository and release
  group/package, the needed version or source commit, observed delivery state,
  and the affected work. Distinguish committed code, pushed tags, registry
  availability and deployed applications. Do not invent an unpublished version
  or start sibling delivery. Continue independent authorized work where possible.
- Actual source defects, missing APIs and substantive consumer migrations still
  belong in their owning issues. A code fix's existing implementation and required
  acceptance criteria remain valid; this rule does not close unfinished fixes or
  waive qualification. Routine version/lock/snapshot updates and publication
  requests alone do not justify another issue.
- Keep **one active remediation issue per shared dependency root cause**, at the
  repository that owns the supplying dependency or capability. A matching
  advisory or inherited defect in multiple consumer graphs does not create
  multiple implementation outcomes. IC Platform owns findings in its external
  IC boundary, including the crypto/Agent dependencies supplied through it;
  other capabilities retain their established owners.
- Search open and closed issues before filing. Reuse the canonical issue and
  respect closed consumer redirects. Attach materially new affected-consumer
  source/lock identities, dependency paths and operation/target evidence there;
  do not recreate mirrors, repeated unchanged comments or a Shared umbrella.
- Open a separate consumer issue only for **distinct, independently actionable
  consumer-owned work**, such as a direct boundary bypass, an integration defect
  or adoption of an available qualified correction requiring local changes.
  State that local action and link the supplying issue. Merely waiting for the
  same supplier or observing the same advisory in another lock is insufficient.
- Consolidate existing mirrors only after linking their unique evidence and
  remaining migration requirements from the canonical issue. Close them as
  superseded/not planned, with an explicit redirect. Preserve historical
  discussions; do not describe the dependency as fixed or adoption as complete.
- Tracking consolidation changes no advisory/release gate, scoped acceptance,
  expiry date or qualification obligation. The supplier closes its issue on its
  own correction or reviewed decision and required evidence; this neither
  certifies fleet adoption nor makes each consumer an acknowledgement gate.

### Repository handoff files

- A current handoff, such as `docs/status/current.md`, is a replacement summary
  of the state needed to resume work, not an append-only session journal. Update
  or replace existing sections; remove completed or superseded narratives rather
  than prepending another session report. This rule does not require creating a
  handoff file where none is needed.
- Aim for **50–100 physical lines**. Above **150 lines**, review and trim the
  handoff during its next update. These are maintenance targets, not tool limits
  or CI/release gates; preserve essential unresolved recovery and safety facts.
- Keep accepted scope, relevant source identity and pending local work, active
  blockers and next actions, and the latest relevant validation with its limits.
  Link canonical issues, migration guides and detailed evidence instead of
  copying their contents or maintaining another issue-status ledger.
- Move completed release/PR narratives, superseded qualification and long test
  inventories out of the required startup read. Preserve still-needed historical
  evidence at its existing owner or a durable Git revision before removing its
  narrative; uncommitted content is not preserved by a Git-history link. Keep
  unresolved uncertainty and the location of recovery evidence visible. Trimming
  prose does not authorize deleting build artifacts or the only retained evidence.
- Do not paste final agent reports, symbol inventories, routine metric blocks or
  command logs into the repository handoff. Keep detailed evidence with its owner;
  link it only when relevant to resuming current work. Reuse existing evidence
  locations and Git history rather than creating a parallel archive framework.
