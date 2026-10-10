# Dragginzgame Engineering Baseline

Shared Tooling is the authoritative source of mandatory common engineering rules
and canonical shared tooling for all `dragginzgame` repositories. This file is
normative for automated contributors. Keep it compact; the
[shared principles](docs/principles/README.md) explain the decision tests.
Consumer choices described in those guides remain subject to this baseline.

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
  Read the current handoff for accepted work and implementation status. Treat
  historical documents as evidence, not current authority. Verify documentation
  against implementation; distinguish supported behavior from unfinished plans.
- Every repository must have a concise, accurate GitHub description of its
  current purpose and scope, consistent with its README and implementation.
  Review it during baseline adoption and whenever the repository's purpose,
  scope or maintenance status changes. Correct missing, stale or misleading
  descriptions; do not present retired behavior or planned features as current.

## Scope and authorization

- Work within the authorized repositories and preserve unrelated dirty work.
  Re-read dirty files before editing. Agents working in one repository must not
  edit code or other files in any other repository unless the maintainer expressly
  authorizes that target repository and the intended change. This includes Shared
  Tooling, upstream dependencies and downstream consumers, whether edits are made
  directly, by delegated agents, or through scripts, formatters and generators.
  Changing directories, shared ownership, dependency fixes, inspection requests
  and issue-reporting authority do not grant cross-repository edit permission.
  Without that authorization, keep other repositories read-only and report the
  proposed fix under the GitHub issue authorization below.
  Prefer owning-repository issues for sibling findings. Explicit scoped
  authorization is an exception to this read-only default, and remains valid
  through the necessary implementation and qualification of that change.
- Follow the [contribution rules](rules/contributions.md): ordinary contributions
  from people and agents use branches and pull requests. Agents may create
  scoped commits when asked to commit or deliver a PR. A PR request includes the
  branch, commits, branch push and PR creation/update needed to deliver it;
  do not require the maintainer to commit first or approve each substep again.
- A PR request does not authorize merging, pushing directly to `main`, `master`
  or another integration/release branch, or rewriting shared history. Those
  effects require explicit authorization. Preserve branch protections and
  required reviews/checks. Ordinary repair or continuation requests authorize
  local work, not an unsolicited commit or push.
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
  cross-repository file edits, unrelated messages or release effects.

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
- When cleaning up code, list every removed function, method and type only in the
  final response to the maintainer in the Codex app, including private symbols
  and those inside deleted files.
  Give exact names, their former file or module, why each was removed and its
  replacement when applicable. Distinguish deletions from moves or renames.
  Shared reasons may be grouped, but every removed name must still be listed.
  Do not copy this inventory into repository files, changelogs, reports, commit
  messages, PRs or issues. Continue documenting meaningful public API removals
  and required consumer actions at their normal owners.
- Complete changelog maintenance under the [common changelog rules](rules/changelogs.md)
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

- Use the [shared audit methods](audits/README.md) for reusable code-hygiene,
  structural and module reviews. Keep product invariants and validation commands
  in consumer overlays. Audit adoption adds no automatic broad gate or schedule;
  findings do not supply repair authority. Preserve historical reports locally.
- Run the smallest relevant checks automatically during authorized development.
  Before delivering a completed code change as ready, run the repository's
  documented full validation suite. This is standing validation authority within
  the authorized repository; no separate request is needed. Local command lists
  must distinguish focused development checks, the full delivery suite and release
  commands. Reuse passing evidence for unchanged inputs; rerun affected checks
  after further changes. Documentation-only changes need link, consistency and
  diff checks, unless the repository documents additional relevant validation.
  Inspection-only tasks do not authorize a full suite. Release execution,
  publication and deployment retain their separate explicit authorization.
- Check for active builds before compilation or source mutation. Do not change
  source under active validation or compete for its build lock. Use the owning
  repository's build directory; preserve unrelated artifacts.
- For offline validation, prepare caches for the selected lockfile explicitly
  before validation or release mutation. Preserve lock selection and report
  preparation failures. An authorized dependency update includes the registry/Git
  access needed for that selected update; do not require separate network approval
  or add `--offline`, `--frozen` or `CARGO_NET_OFFLINE=true` by habit. Offline
  validation is not a blanket policy for dependency preparation. Follow the
  [Cargo network policy](rules/cargo-dependencies.md#cargo-network-policy);
  do not silently override a caller's explicit offline setting or select
  unrequested dependency versions.
  A selected standard release includes its documented locked cache and selected
  executable-tool preparation under the [release contract](docs/releases.md),
  including required cache preparation before compiled
  adapter startup. Explicit offline settings remain authoritative; ordinary
  checks and standalone helper reads do not gain network permission.
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
  All three use the same [release contract and Makefile pattern](docs/releases.md):
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
- These one-shot commands require an explicit request to run the selected release
  for the selected repository and destination. That request authorizes the
  documented gate, version preparation and selected delivery effects. Direct
  delivery includes the release commit, tag and atomic branch/tag push; PR
  delivery includes the release branch/PR and, after separately authorized merge,
  fresh merged-source validation and its exact tag push. The runner never merges
  or approves the PR. A request for a fix, commit or PR does not authorize a release;
  separate preparation and inspection retain their own authorized scope.
- Use standard semantic-version increments: patch increments the patch; minor
  increments the minor and resets the patch; major increments the major and
  resets both lower components. Apply the pre-1.0 compatibility rule above.
- Stop on failure and preserve build and evidence artifacts. Preflight or
  validation-only failures restart through the normal target against current
  source with fresh preflight and complete validation; retain earlier evidence.
  Persist exact release intent before preparation may begin, then reconcile an
  interrupted release automatically when a normal target is rerun, at its saved
  version and commit. In direct delivery, if that release is committed and HEAD
  has newer fixes or a different increment is requested, reconcile it first,
  then run fresh preflight and complete validation for the requested increment
  from the actual local version. PR delivery finishes only the saved release; an open PR returns
  a pending, nonzero result so chained publication cannot start before merge.
  Squash/rebase merges require fresh complete validation of the exact merged
  commit with the unchanged prepared tree. Late evidence checks use
  `RELEASE_COMMIT`, which may precede HEAD. Select unfinished intent before
  computing another increment; stop for identity, payload, destination or concurrency conflicts.
  Do not force-push, overwrite tags, silently bump again or add implicit package
  publication, deployment or post-release cleanup.

## Changelogs and artifact preservation

- Before editing a changelog, read and apply [the changelog rules](rules/changelogs.md).
  They are part of this mandatory baseline and own release-note content, issue
  links, draft/history preservation and detailed-note structure.
- Releases and deployments preserve consumer-owned build and evidence artifacts
  on success, failure and retry. Never append cargo clean or equivalent cleanup.
  Cleanup requires explicit scope and authority. Tools may clean temporary files
  they own; they must not erase consumer artifacts or the only retained evidence.

## Rust workspaces and portable tooling

- Directory names for repositories, workspaces, tooling and operational inputs
  must not contain line-feed (LF) or carriage-return (CR) characters, including
  ancestor directories and resolved symlink destinations. Do not create or select
  such directories for normal work. Reject invalid paths before trimming or
  normalizing them; never silently select a different path. Deliberate negative
  test fixtures may contain forbidden names. Preserve existing artifacts and
  evidence with those names; this rule does not authorize renaming or deletion.
- Provide [local developer setup](docs/local-setup.md) through explicit
  `make install-tools` and offline `make tools-check`, following the
  [required tool inventory](docs/local-setup.md#required-tool-inventory).
  The host set includes pinned jq, Mike Farah yq, ripgrep with PCRE2 and cloc
  under `.tools/host/bin`. Make/CI installation and check callers select the
  complete set and their own checkout's local paths; document interactive shell
  PATH setup, system bootstrap packages and product toolchains separately.
  Adopt `make/tools.mk` from the reviewed snapshot for these common commands
  and LOC reporting, replacing copied recipes; CI calls the same targets.
- Provide the [common local IC executable setup](docs/ic-tools.md) through
  `make install-ic-tools` and offline `make ic-tools-check`. Keep the common tool
  names available under the checkout's `.tools/ic/bin`, with one reviewed pin
  matrix and explicit installation. Consumers own version qualification and
  scoped pin exceptions; ordinary validation never downloads tools implicitly.
  PocketIC-specific selection, provisioning, offline admission and lifecycle
  belong to IC Testkit. Consumers use its selected setup/check contract rather
  than a separate server pin catalog or Shared Tooling's IC executable bundle.
- Apply the [dependency pinning rules](rules/dependency-pinning.md): immutable
  Git/action identities, compatible registry requirements with locked builds,
  verified tool downloads, and explicitly qualified sibling or moving inputs.
  Run the declaration checker in CI and release gates; consumers own the chosen
  versions, approved exceptions and runtime qualification evidence.
- Use the [standard Rust workspace layout](rules/rust-workspaces.md): a virtual
  repository-root workspace, including single-package repositories. Maintained
  packages use `crates/<package-name>/` or application-owned `apps/<app-name>/`
  trees, including component packages grouped beneath an App. Both inherit from
  the same root. Approved independent workspaces use those shapes relative to
  their own roots; other layouts need explicit exceptions.
  The rule explicitly approves IcyDB's existing `crates/`, `canisters/`, `schema/`
  and `testing/` layout; adoption does not require relocating those packages.
  Repositories without Rust packages do not need a Cargo workspace.
- Cargo workspace members inherit package versions from the root. Apply the
  [Cargo dependency rules](rules/cargo-dependencies.md): every direct dependency
  is declared in root `[workspace.dependencies]`, and every child manifest uses
  `workspace = true`, including development, build and target-specific tables.
  Keep version/source selections in the root; children select target conditions,
  features and publication policy. Keep the
  [minimum supported Rust version](rules/cargo-dependencies.md#minimum-supported-rust-version-msrv)
  as low as the qualified package dependency path permits, independently of the
  development toolchain. CI must explicitly check each advertised floor;
  documented package groups may have different floors. Do not upgrade a toolchain
  or raise MSRV without an established need and appropriate validation.
- Follow the [Rust hygiene baseline](docs/principles/rust-code-hygiene.md): narrow
  visibility, documented APIs/invariants, ordinary module discovery, bounded
  fallible decoding, typed errors and `std` paths in code that requires `std`,
  preserving documented `no_std` support. Exact edition, internal module layout and
  lint choices stay local within the workspace rules. Do not fake platform
  behavior with production cfg(test) paths.
- Rust repositories adopt the [standard formatting hook](rules/git-hooks.md):
  `make install-hooks` enables a reviewed repository-local pre-commit hook that
  auto-formats and refreshes only selected files, rejects partial staging and
  preserves unrelated working edits. Formatting includes `cargo sort --workspace`
  for root and child manifests before Rust formatting. Keep matching `fmt-check`
  sorting and Rust checks in CI and release gates;
  hook activation and independent formatting qualification are separate checks.
- Keep shell wrappers small and declare dependencies and effects. Use the
  project's maintained implementation language for substantial tooling. Do not
  add Python tooling. Fail on invalid inputs rather than hiding broken assumptions.
- CI/release tools use [reviewed snapshots](docs/consuming-snapshots.md), not symlinks
  or mutable sibling/network sources. Do not patch vendored copies; fix upstream
  or keep an explicit consumer-owned adapter outside the snapshot.
- Portable scripts use Bash strict mode and preserve Bash 3.2 compatibility unless
  the support matrix is deliberately changed. Downloaded executables need exact
  consumer-owned versions, pinned digests, HTTPS, verification before extraction
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
  [the host guidance](docs/supported-hosts.md) and the consumer's matrix; report
  outstanding qualification without weakening the support requirement.

## Feedback and handoff

- Use the [repeatable task catalog](tasks/README.md) for named maintenance checks
  and bounded recurring audits. Shared Tooling owns the procedures; consumers
  own local inputs and validation contracts. Scheduled execution requires explicit
  activation and retains the task's inspection/repair boundaries.
- Follow the [user-triggered agent maintenance rules](rules/agent-maintenance.md)
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
- Track upstream acceptance separately from verified consumer adoption. Resolve
  product-specific feedback locally instead of promoting it to universal policy.
  For another repository's finding, search its issues and file or update the
  matching issue under the issue authorization above. Include a concrete fix or
  patch where feasible and its actual validation results. Keep shared snapshots
  intact; repair at the source owner and adopt a reviewed committed revision.
  Other cross-repository changes retain their separate authority.
  If issue access or a remote is unavailable, report the finding and blocker to
  the maintainer without inventing an issue URL or creating a local tracker.
- Report the outcome, changed files, relevant verification, skipped checks,
  remaining risks and open consumer actions. State compatibility and host impact
  when relevant. Do not claim a release proves publication, adoption or deployment.
