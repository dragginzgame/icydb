# Dependency and repository pinning

These rules are part of the [engineering baseline](../DRAGGINZGAME.md).
Use one consistent selection method across repositories; consumers still own
their reviewed versions. Different qualified versions are not automatically drift.
The [Cargo ownership rules](cargo-dependencies.md) define where declarations live.

## Selection and reproducibility

| Input | Required selection |
| --- | --- |
| Registry dependencies | Explicit compatible version requirements plus a tracked lockfile for each maintained build workspace. |
| Git dependencies | Repository URL and full commit ID in `rev`; no branch, tag, abbreviated SHA or implicit default branch as the pin. |
| External Actions and reusable workflows | Full commit SHA in `uses`; a version comment may explain it. Local actions belong to the checked-out source. |
| Additional repository checkouts | Full commit ID in `ref`, unless an approved moving-input exception applies. The workflow's own checkout may follow the selected event/branch/PR. |
| CI container images and Docker actions | `@sha256:` image digest. A repository-owned Dockerfile belongs to the source; review its base-image pins too. |
| Downloaded executables | Exact version and platform checksum, verified before extraction/execution, followed by a version check. |
| Shared Tooling snapshots | Reviewed source commit plus file digests under the [snapshot contract](../docs/consuming-snapshots.md). |

- A Cargo declaration such as `version = "0.49.2"` is a compatibility range,
  not an exact pin. Prefer Cargo's ordinary compatible requirements for registry
  libraries. Avoid wildcard requirements. Use `=X.Y.Z` only for a concrete
  compatibility constraint, such as a coupled macro/runtime API, a qualified
  protocol boundary or an upstream regression; record the reason and exact value.
  Do not exact-pin every library merely to reproduce a build.
- Track each build workspace's lockfile, including maintained test/integration
  workspaces. CI, release validation and artifact-producing Cargo commands use
  `--locked` (or `--frozen` when offline operation is intended). Explicit cache
  preparation preserves that lockfile. Do not regenerate a lockfile to make a
  gate pass. Published libraries still declare compatibility for their consumers;
  their own lockfile does not constrain a downstream application's resolution.
- Verify that a selected Git SHA belongs to the intended upstream and review its
  change before updating it. A full SHA fixes identity; it does not prove trust,
  compatibility or validation. A tag may be recorded as a human-readable label.
- Keep a pin with its existing authoritative owner: root Cargo catalog,
  workflow reference, tool-version/checksum file or snapshot manifest. Do not
  introduce a second global version catalog. Update coupled declarations,
  lockfiles, checksums and affected evidence together through an explicit review.
  Checker execution never upgrades, fetches, reformats or rewrites dependencies.

## Frontend and npm inputs

- Keep a committed `package-lock.json` for each independent npm build root.
  Use `npm ci` during explicit CI/developer dependency preparation; validation
  and hooks use the already prepared dependencies. A manifest/lock mismatch
  fails instead of regenerating the lock. Preserve any reviewed project `.npmrc`
  options needed to reproduce that lock. Compatible registry requirements are
  fine; the lock owns resolved versions and integrity. Git dependencies still
  need full commit identities, and external file links need the sibling-input
  qualification below.
- Record an exact Node version and the selected npm version with one consumer
  owner, such as `.nvmrc` plus the package-manager declaration or tool inventory.
  CI and local setup consume that selection. An `engines` compatibility range
  describes supported runtimes; a moving major alone does not freeze a release
  environment. Do not create another shared catalog of product toolchain pins.
- Configure intended registries and scoped package routing explicitly. Keep
  credentials outside tracked configuration and logs. Preparation may run package
  lifecycle scripts; review those effects and do not use `npx`/`npm exec` as an
  implicit tool installer in a formatter or gate.
- npm publication is a separate authorized command. Bind generated declaration
  packages to the selected source, generator/tool versions and lockfile; inspect
  the exact package payload, version and registry before dispatch. Keep immutable
  Action pins, least-privilege credentials or qualified trusted publishing, and
  supported registry provenance. A release tag or passing formatter is not proof
  that a package was published. Reconcile an uncertain publish response against
  that package/version before retrying.

The declaration checker below does not yet inspect npm manifests, lockfiles or
Node selections. Consumer gates must enforce these inputs; a checker PASS is not
npm qualification. See [npm ci](https://docs.npmjs.com/cli/v11/commands/npm-ci/)
and [npm provenance](https://docs.npmjs.com/generating-provenance-statements/).

## Sibling paths and moving inputs

- Paths inside the repository use that checkout's source identity. A dependency
  outside it, including a symlink into a sibling checkout, needs a documented
  development purpose and release qualification boundary. A `version` beside
  `path` does not freeze the sibling's bytes.
- Release qualification must select and verify the external repository's exact
  commit and clean state, or use a content-addressed immutable artifact. Bind
  its lockfile and build inputs to the resulting evidence. Reuse that identity
  during retry; do not silently read a newer sibling HEAD.
- A maintainer may approve following a named branch during development, as with
  a separately published asset repository. Record the repository, selector,
  owner/reason and the command or procedure that freezes and verifies its resolved
  identity before qualification. Resolve once for that attempt; retries use the
  recorded identity. Such an exception is not permission for floating release
  inputs or for a general exemption from pinning.
- Existing local exceptions must be deliberately adopted, not silently replaced.
  Merely finding a branch or sibling path in current code does not establish
  approval. Maintainer authorization remains governed by the baseline.

## Automated checks and exceptions

Run `bash scripts/ci/check-dependency-pins.sh` in CI and the complete release
gate. Use `--consumer /path/to/repository` for read-only local inspection. The
checker reads tracked and non-ignored new Cargo manifests, workflows and action
definitions; it parses YAML/TOML with Mike Farah `yq`, rather than treating
comments, shell strings or line layout as declarations. `jq` and Git are also
required; Cargo discovers actual workspace roots without downloading dependencies.
The selected Rust toolchain must already be installed; the checker disables
rustup's automatic toolchain installation as well as Cargo network access.

It checks Git/action/check-out/container selectors, Cargo wildcard/exact
requirements, paths outside the repository and the presence of tracked workspace
lockfiles. It covers workspace, development, build, target-specific and patch
dependency tables. It does not resolve registry graphs, prove that a shell command
uses `--locked`, inspect arbitrary download scripts/Dockerfile contents, or verify
live release evidence. Consumer gates enforce those execution boundaries with
locked commands and their existing qualification checks. A passing declaration
check alone is not release qualification.

Document approved exceptions in the local overlay or its linked policy, including
their qualification procedure. Machine-readable entries live in the optional
`ci/dependency-pinning-exceptions.json`, a JSON array with these exact fields:

```json
[
  {
    "rule": "cargo-exact",
    "file": "Cargo.toml",
    "subject": "example-runtime",
    "value": "=1.2.3",
    "reason": "Must match the macro crate's generated API.",
    "evidence": "AGENTS.md#dependency-exceptions"
  }
]
```

Allowed rule names are `cargo-exact`, `cargo-external-path` and `checkout-ref`.
For Cargo, `subject` is the declared dependency name/alias and `value` is the
literal version requirement or external path. For a checkout, `subject` is
`with.repository` and `value` is its explicit `with.ref`. `file` is the declaring
file, even when a Cargo dependency is inherited elsewhere. Evidence must name a
tracked local document, optionally with an anchor. No wildcards or blanket
suppressions: entries match all four identity fields exactly. Changed selectors
need reviewed reasons; remove exceptions when their maintained need ends. This
file records the approved decision, not a new source of versions or an issue ledger.
The checker validates structure and matching scope; human review establishes approval.

Consumers vendor the checker and `scripts/ci/dependency-pins.jq` together. Prepare
a reviewed `yq` version (4.47.2 or later in v4) before running checks; the shared
`install-yq.sh` accepts explicit version/digest inputs and uses the existing
checksum verifier. Shared Tooling's own selections live in `ci/tool-versions.env`.
Qualify adoption on each declared native host. Do not silently change dependency
versions, toolchains or release inputs while adopting this policy.

References: [Cargo version requirements and Git selections](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html),
[Cargo lockfiles](https://doc.rust-lang.org/cargo/guide/cargo-toml-vs-cargo-lock.html),
[GitHub Action pinning](https://docs.github.com/en/actions/reference/security/secure-use#using-third-party-actions).
