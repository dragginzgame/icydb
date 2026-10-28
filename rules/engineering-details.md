# Engineering contracts by task

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
- Shared Tooling owns consistency across the repository fleet: repository/sibling
  counts, shared-snapshot and pin drift, CI/issue overviews, cross-repository LOC
  and tooling inventories, and common maintenance/audit procedures. Run central
  fleet reports here; their existence does not require every consumer to vendor
  them or add sibling scans/dashboard tests to its CI. Consumers still adopt
  shared tooling needed for their own development and validation.
- Keep repository-fleet inventory and governance out of IC Metrics' product
  scope. IC Metrics owns reusable measurement arithmetic; a repository count is
  a Shared Tooling report, not a new IC Metrics API, dependency or application.
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

- **Task-relevant read-only inspection of sibling repositories is authorized by
  default and requires no separate permission.** Read source, manifests, history,
  instructions and existing evidence as needed to verify consumers, ownership or
  compatibility. Consumer `AGENTS.md` files must not add an "ask before reading
  siblings" gate. Honour explicit user exclusions and access restrictions. This
  authorizes inspection, not edits or commands that write in a sibling checkout;
  those follow the contribution and validation rules below.
- Work within the authorized repositories and preserve unrelated dirty work.
  Re-read dirty files before editing. Agents working in one repository must not
  edit another contributor's sibling checkout in place. Relevant cross-repository
  fixes and adoption work in `dragginzgame/*` have standing authorization through
  the [isolated PR workflow](contributions.md#cross-repository-pull-requests):
  use a separate worktree or clone, topic branch, scoped edits and checks, commits,
  branch push and PR. Do not ask for separate permission for those steps.
  Preserve explicit read-only instructions, repository exclusions and active
  ownership. Contributions under other GitHub owners require explicit
  target/action authorization.
  Direct sibling-checkout edits require an express exception naming that checkout
  and change; shared ownership or a PR contribution never supplies that exception.
- Staging, commits and release execution are user-owned by default. During
  ordinary implementation, leave the real checkout's index, package/application
  release versions and Git refs alone. Do not run `git add .`, `git add -A`,
  `git commit`, version-bump helpers, `git tag`, pushes, `make release-*` or
  mutating release adapters to finish a coding task. This includes release
  recovery and indirect calls from scripts or Make targets. "Keep going",
  "prepare the release", "work on 0.4" and "ready to push?" authorize no such
  effects. Prepare source, numbered changelog notes and validation, then hand
  them to the user. Explicit delegation and the standing isolated PR exception
  follow the contribution rules below;
  a maintainer-only local release boundary must not be bypassed by inference.
- Follow the [contribution rules](contributions.md): ordinary contributions
  from people and agents use branches and pull requests. Agents may create
  scoped commits when asked to commit or deliver a PR. A PR request includes the
  branch, commits, branch push and PR creation/update needed to deliver it;
  do not require the maintainer to commit first or approve each substep again.
  The standing cross-repository PR workflow supplies the same scoped authority
  without a separate PR request. Neither route authorizes release versions or tags.
- A PR request does not authorize merging, pushing directly to `main`, `master`
  or another integration/release branch, or rewriting shared history. Those
  effects require explicit authorization. Preserve branch protections and
  required reviews/checks. Ordinary repair or continuation requests authorize
  local work in the current repository; the standing cross-repository PR workflow
  separately authorizes only its isolated contribution commits and branch pushes.
- Agents may execute package/manifest version changes, tags, pushes, publication,
  deployment and paid effects when explicitly authorized for their target and effect.
  Authorization includes the necessary documented substeps of the requested
  operation. Inspect helpers before invoking them; a contribution PR is not a
  release request. Continuation and readiness alone do not authorize a release.
- An explicit instruction is sufficient for its named action. Carry established
  authorization forward; do not require magic phrases or repeated confirmation.
  Complete authorized preparation so any required approval concerns a concrete
  result. Apply authorized current-repository fixes directly to the working tree
  and run the appropriate focused checks; a detached patch alone does not complete
  a local repair. Inspection remains distinct from repair authorization.
  Relevant GitHub issue work has standing authorization only in repositories
  owned by `dragginzgame`: create, comment, update, assign, close or reopen issues
  as warranted by the evidence, following the feedback rules below. Verify the
  actual GitHub owner before writing. Issue actions in any other GitHub repository
  require explicit maintainer authorization for that destination and action;
  searching and reading issues remain permitted. Prepare external issue content
  for review before requesting authorization. This does not authorize
  unrelated messages or release effects. Cross-repository source contributions
  use the separate isolated PR authority above, not issue-posting authority.

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

## Repository files: no symlinks

- **Do not create or commit symlinks for maintained repository files or
  directories.** This includes source, manifests, documentation, scripts, shared
  snapshots, license files and shipped package assets. Generated source and
  package outputs follow the same rule; fix their generator when necessary.
- Use explicit dependency/configuration paths or the canonical package interface.
  When packaging needs a local file, use a regular copy and verify it against
  its owner. In particular, package-local `LICENSE` files must be regular files
  containing the applicable complete terms and notices, never symlinks.
- The permitted repository-tooling uses are generated selection pointers owned
  and admitted by Shared Tooling's verified executable store, and deliberately
  created symlinks in isolated rejection/security fixtures. Keep these outside
  maintained source and shipped package assets; they do not authorize general
  file aliases, package-license links or links into sibling checkouts.
- Operating-system/toolchain layouts and directory aliases outside the maintained
  repository tree remain external inputs, subject to each tool's path admission.
  This rule does not authorize deleting existing installations, evidence or link
  targets. Replace an obsolete repository link without modifying its target.
- Review tracked Git modes, working-tree paths and actual package file lists
  when adopting the rule. A clean index alone does not prove generated or packaged
  files comply. Any other repository-owned use needs a scoped maintainer-approved
  exception under the baseline's local-authority rules.

## CLI applications

- **Use Clap for Rust CLI applications**, including developer tools and maintained
  utility binaries. Use its derive or builder API rather than handwritten
  `std::env::args` loops, custom flag/subcommand dispatch or a parallel parser.
- Let Clap own argument/value parsing, typed values, defaults, required arguments,
  allowed values, argument groups/conflicts/dependencies, help, version, usage and
  parse-error diagnostics. Before adding custom CLI logic, check whether Clap
  already provides the behavior and use that capability.
- Prefer Clap's extension points when application-specific parsing is needed.
  Keep custom validation limited to domain constraints Clap cannot express;
  document that gap and keep domain semantics with their existing owner. Do not
  reimplement generic CLI mechanics behind wrappers or duplicate manual checks
  that Clap already enforces.

## IC application encoding

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
- Check for active builds before compilation or source mutation that could affect their inputs. Do not change
  source under active validation or compete for its build lock. Use the owning
  repository's build directory; preserve unrelated artifacts.
- Authorized development, tests and validation include `cargo fetch --locked`
  for missing dependency sources in the affected workspace's existing lockfile;
  no separate maintainer approval is needed. Preserve lock selection and report
  preparation failures. A missing or inconsistent lockfile does not authorize
  an unlocked update. Honour explicit offline settings and actual environment
  network restrictions. For offline gates, prepare caches before validation.
  An authorized dependency update includes the registry/Git
  access needed for that selected update; do not require separate network approval
  or add `--offline`, `--frozen` or `CARGO_NET_OFFLINE=true` by habit. Offline
  validation is not a blanket policy for dependency preparation. Follow the
  [Cargo network policy](cargo-dependencies.md#cargo-network-policy);
  do not silently override a caller's explicit offline setting or select
  unrequested dependency versions.
  A selected standard release includes its documented locked cache and selected
  executable-tool preparation under the [release contract](../docs/releases.md),
  including required cache preparation before compiled
  adapter startup. Locked source preparation does not authorize executable-tool
  installation, runtime network access, new credentials/targets, paid operations
  or release effects. Keep offline checks and standalone helper reads offline;
  prepare missing sources separately rather than adding implicit fetches to them.
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
- Standard release entrypoints must select their required prepared executables
  through the consumer's offline admission interface; do not require maintainers
  to export tool paths in each shell. Fresh preflight prepares missing selections
  only after source/recovery admission. Recovery admits the saved source's
  selection without installing or resolving partially prepared live metadata.
  Low-level helpers may require explicit paths supplied by their calling adapter.
- These one-shot commands are user-operated by default. An agent needs explicit
  delegation to run the selected release for the selected repository and
  destination. That request authorizes the
  documented gate, version preparation and selected delivery effects. Direct
  delivery includes the release commit, tag and atomic branch/tag push; PR
  delivery includes the release branch/PR and, after separately authorized merge,
  fresh merged-source validation and its exact tag push. The runner never merges
  or approves the PR. A request for a fix, commit or PR does not authorize a release;
  separate preparation and inspection retain their own authorized scope.
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

- Before editing a changelog, read and apply [the changelog rules](changelogs.md).
  They are part of this mandatory baseline and own release-note content, issue
  links, draft/history preservation and detailed-note structure.
- Releases and deployments preserve consumer-owned build and evidence artifacts
  on success, failure and retry. Never append cargo clean or equivalent cleanup.
  Cleanup requires explicit scope and authority. Tools may clean temporary files
  they own; they must not erase consumer artifacts or the only retained evidence.

## IC stable-memory governance

- Production stable stores in `dragginzgame` IC applications and components must
  use memory handles issued by `ic-memory` after successful admission and durable
  allocation commitment. Stable keys and their retained memory-ID assignments
  belong to `ic-memory`; store formats, schema admission and data semantics stay
  with their storage owners.
- Application and component code must not independently assign memory IDs,
  construct `MemoryManager` instances, or read/write unmanaged stable memory.
  The host integration supplies raw backing memory to its owned `ic-memory`
  runtime; it must not retain an alternate store-access path or create another
  manager over that backing. Raw backing access and manager construction belong
  to that integration boundary and `ic-memory` internals.
- Production storage integration entry points must require checked runtime
  handles and preserve validation and persistence before opening stores.
  Generic storage engines may accept the upstream `Memory` trait, but accepting
  that trait alone does not enforce allocation governance. Their production
  callers must supply `ic-memory::RuntimeMemory` through the checked integration.
- `ic-storage` is a collection layer; it does not replace allocation governance.
  Its production integrations must use `ic-memory` handles. Selecting it as the
  standard collection implementation is a separate product decision, qualified
  at the affected consumers. An upstream collection using checked runtime memory
  can preserve allocation safety; a replacement collection using unmanaged
  memory cannot claim that protection.
- Review actual handle construction and access, including dependency re-exports.
  Importing `Memory`, `Storable` or an upstream collection is not itself a bypass;
  neither an import-path rule nor a direct-dependency ban proves this invariant.
- Raw-memory infrastructure internals, isolated tests and benchmarks may use
  scoped unmanaged memory. Document the owning boundary and purpose; such
  evidence does not qualify a production integration. Other production bypasses
  require a maintainer-approved exception naming the scope, reason and safety
  qualification. Adopt this rule through a reviewed snapshot or revision-bound
  reference; policy adoption alone does not prove consumer implementation.

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
  independent. Product tools, PocketIC selection and compiler policy stay local.
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
- Cargo workspace members inherit package versions from the root. Preserve
  established groupings and coordinated versions under the
  [workspace versioning guidance](rust-workspaces.md#coordinated-versions-and-workspace-boundaries);
  prefer release batching and verified consumer adoption over another split.
  Apply the
  [Cargo dependency rules](cargo-dependencies.md): every direct dependency
  is declared in root `[workspace.dependencies]`, and every child manifest uses
  `workspace = true`, including development, build and target-specific tables.
  Keep version/source selections in the root; children select target conditions,
  features and publication policy. Keep the
  [minimum supported Rust version](cargo-dependencies.md#minimum-supported-rust-version-msrv)
  as low as the qualified package dependency path permits, independently of the
  development toolchain. CI must explicitly check each advertised floor;
  documented package groups may have different floors. Do not upgrade a toolchain
  or raise MSRV without an established need and appropriate validation.
- Consume every externally maintained IC-specific dependency through `ic-platform`,
  including DFINITY packages. Apply the
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
  Relevant repairs may proceed through the standing isolated PR workflow;
  sibling working checkouts remain untouched.
  If issue access or a remote is unavailable, report the finding and blocker to
  the maintainer without inventing an issue URL or creating a local tracker.
- Report the outcome, changed files, relevant verification, skipped checks,
  remaining risks and open consumer actions. State compatibility and host impact
  when relevant. Do not claim a release proves publication, adoption or deployment.

### Shared dependency findings

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
