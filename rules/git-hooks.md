# Git formatting hooks

These rules are part of the [engineering baseline](../DRAGGINZGAME.md) for Rust
repositories. The standard pre-commit hook automatically formats the selected
commit payload. Repository-local Git hooks run before a commit; GitHub CI provides
the independent, non-mutating formatting gate.

## One formatting contract

- Vendor the reviewed Shared Tooling `.githooks/pre-commit` and
  `scripts/dev/install-git-hooks.sh`, with `scripts/ci/check-make-execution.sh`,
  unchanged in the [recorded snapshot](../docs/consuming-snapshots.md).
  Keep the hook and installer executable.
  Retire the superseded local formatter hook after reviewing its obligations.
- Every Rust repository provides `make fmt` and `make fmt-check`. Both cover the
  same maintained Rust workspaces, including standalone nested workspaces,
  examples or harnesses with their own manifests. Following IcyDB and Canic,
  `fmt` runs `cargo sort --workspace` before `cargo fmt --all` for each workspace;
  `fmt-check` uses `cargo sort --workspace --check` and
  `cargo fmt --all -- --check`. This sorts the root and every member's Cargo.toml,
  including members under `crates/`, `apps/` and approved layout trees, the root
  dependency catalog and inherited child tables. Explicitly cover
  standalone/excluded manifests outside that workspace's member set.
  Independent workspaces and their locations follow the
  [workspace layout rules](rust-workspaces.md); formatter coverage does not
  itself grant a layout exception.
- Use the `cargo-sort` tool for manifest ordering. Record an exact reviewed
  version in the consumer's developer and CI setup and use the same version in
  both; Shared Tooling's fixtures use `2.1.4`, matching Canic's reviewed selection.
  Install it during explicit setup, with `--version` and `--locked`, never during
  pre-commit or validation. Keep toolchain and rustfmt/tomlfmt configuration
  locally owned. Sorting preserves dependency identities, features, package
  versions, workspace inheritance, comments and selected lockfiles; it is not a
  dependency upgrade or release bump. See the
  [cargo-sort documentation](https://github.com/DevinR528/cargo-sort).
- Additional formatting steps, such as derive sorting, may
  belong in those targets. They must have matching non-mutating checks, preserve
  dependency/version selections and work from tracked inputs in an isolated
  index snapshot. Formatting must not build, test, publish, clean artifacts,
  install tools or implicitly fetch dependencies. Keep network/cache preparation
  and frontend dependency setup outside the hook. A mixed Rust/frontend repo
  must supply an explicit local formatting adapter when its formatter requires
  untracked dependencies; do not patch the vendored hook or implicitly install
  those dependencies in a commit hook. Such an adapter runs explicitly prepared
  formatter executables against snapshot inputs, never against the original
  working tree.
- Include `fmt-check` in CI and the complete release gate. Prepared release
  metadata must also be consistently formatted before staging. A release must
  not rely on a commit hook to repair its saved staged payload; retain the
  release runner's exact commit-tree check.
- Keep routine formatting output concise across all repositories. `make fmt-check`
  reports `Checking formatting... ok` on success; `make fmt` reports
  `Formatting... ok`. Use the shared `scripts/ci/run-formatting.sh` wrapper to
  capture formatter stdout/stderr. Failures produce a short failure line and a
  second line pointing to the complete retained log; preserve the command's
  failing exit status. Make may add its own error diagnostic. Do not hide
  failures, discard diffs or remove workspace/derive/frontend checks to reduce
  output. Retain failure logs in CI artifacts too.

For a single root workspace, prefer the optional shared `make/rust-format.mk`:

```make
include make/tools.mk
include make/rust-format.mk
```

It supplies `format-tools-check`, `fmt` and `fmt-check`, using the shared
[formatter prerequisite check](../docs/verification-helpers.md#formatter-prerequisites)
and the `HOST_TOOL_VERSIONS` pin file. Formatting uses prepared tools offline and
disables automatic rustup installation. `FORMAT_CARGO` selects one executable
name or path, not a command string; export `RUSTUP_TOOLCHAIN` to select a compiler.
The include preserves the default goal and never activates hooks or installs
tools. Keep setup explicit and retain stronger consumer admission checks.
Its `run-formatting.sh` companion owns the compact output and retained logs;
include it in snapshots and isolated hook/adoption fixture inputs.
The adjacent `make/execution.mk` companion uses the existing execution probe to
reject Make ignore-errors and non-executing modes before recipes run. Select its
declared companion when exporting; a failing prerequisite alone cannot enforce
failure propagation under ignore-errors mode.
Admission checks both `MAKEFLAGS` and Make's retained `MFLAGS`, including when
`MAKEFLAGS` is cleared or replaced. Do not assign `MFLAGS` on the command line or
in a Makefile; the guard requires GNU Make's generated invocation flags.

Keep local recipes for multiple independent workspaces, sort-derives, custom
manifest ordering or frontend formatting. Those recipes still use the shared
prerequisite checker; do not adopt the root-only include and accidentally drop
existing coverage. Qualify the actual formatting hook after either adoption.
Wrap the complete local formatter adapter once, for example
`bash scripts/ci/run-formatting.sh --check bash scripts/dev/format-workspaces.sh --check`,
where the consumer's existing adapter owns the actual commands. A fixed
`bash -ec '...'` sequence works too; do not create another workspace registry or
replace custom policy with the root-only include. The wrapper accepts one command
and its literal arguments, runs it once, and preserves its exit status. Logs are
created beneath `RUNNER_TEMP`, otherwise `TMPDIR` or `/tmp`; only its own successful
temporary log is removed. The shared failure collector includes `formatting.*`.

When a recipe calls a tool available only through a Makefile-exported `PATH`,
use `env tool ...` (for example, `env cargo sort --workspace`) or an explicit
executable path. Apple's system Make can resolve a bare recipe command using
its original process PATH, even though the recipe receives the updated PATH.
Do not rely on an unrelated global installation to make the recipe work.

For a separate `testing/` workspace, also run `cargo sort --workspace testing`
before `cargo fmt --manifest-path testing/Cargo.toml --all`, with their `--check`
equivalents in `fmt-check`. Pass one workspace root per `cargo sort --workspace`
invocation. `cargo-sort` owns its ordering and TOML rendering; do not add a second
hand-written sort or rewrite dependency declarations to enforce ordering.

## Selected files and working edits

### Mixed Rust and frontend reference

Vendor `scripts/dev/format-frontend.sh` with the hook for a frontend using
Prettier's built-in parsers. Explicit setup prepares Node and dependencies with
the [npm pinning rules](dependency-pinning.md#frontend-and-npm-inputs). Before
committing, the caller exports `PRETTIER_BIN` as the absolute prepared executable
path (for example, `$PWD/frontend/node_modules/.bin/prettier`). An isolated index
export has no `node_modules`; never resolve that path relative to the export or
install packages inside it. The consumer's Makefile reads the version from the
selected tracked lockfile, for example:

```make
PRETTIER_VERSION = $(shell jq -er '.packages["node_modules/prettier"].version' frontend/package-lock.json)

fmt:
	@PRETTIER_VERSION="$(PRETTIER_VERSION)" bash scripts/ci/run-formatting.sh --write \
		bash -ec 'cargo sort --workspace; cargo fmt --all; bash scripts/dev/format-frontend.sh --write frontend'

fmt-check:
	@PRETTIER_VERSION="$(PRETTIER_VERSION)" bash scripts/ci/run-formatting.sh --check \
		bash -ec 'cargo sort --workspace --check; cargo fmt --all -- --check; bash scripts/dev/format-frontend.sh --check frontend'
```

Retain the Rust prerequisite checks and all actual workspace roots described
above. The hook supplies `SHARED_TOOLING_FORMAT_FILES`, a NUL-delimited selection
outside the exported worktree. The helper formats matching selected frontend
paths inside that export; the hook alone refreshes the real index. Outside the
hook, `--write` and `--check` cover tracked frontend inputs, with `--check` always
checking the complete tracked scope. JavaScript/TypeScript, JSON, CSS/SCSS, HTML,
Markdown and YAML are selected; Prettier owns configuration and ignore behavior.
Generated files may be ignored only when the consumer's generation checks own
their correctness. Symlink inputs are refused. No Git staging or tool install
occurs in the helper, and formatter failures propagate.
Explicit checks/writes reject a missing scope directory, and a failed version
probe is rejected even if it prints the expected version. A hook selection with
no remaining frontend files is a valid no-op, including deletion of the last
frontend file; it does not require the prepared executable.

This is a reference for prepared executables and built-in parsers. A consumer
needing plugins or additional extensions owns their explicit prepared resolution
and focused adapter checks. Preserve config, version and lock selections from
the snapshot; do not make the formatter read unstaged configuration in the real
checkout. Qualify real Prettier on the declared native hosts before claiming
adoption; shared command-substitute tests prove wiring and selection only.
Shared Tooling additionally runs `scripts/ci/test-frontend-formatting.sh` with
locked real Prettier and Rust formatters in its native CI matrix. It exercises
selected-file refresh, snapshot configuration and ignore behavior, parse-failure
isolation, partial staging, and preservation of unrelated files and the npm lock.
Prepare its test-only dependencies with `npm ci --prefix ci/frontend`, using the
Node/npm selections in `ci/frontend/package.json`; validation never installs
them. A matching successful CI run qualifies this reference's built-in parsers,
not a consumer's plugins, configuration or adoption.
See the [Prettier CLI contract](https://prettier.io/docs/cli).

### Index ownership

- The hook records the selected added/modified/renamed files using NUL-delimited
  paths. It rejects a selected file with unstaged changes before formatting,
  including a partial selection of configuration or formatter inputs. Preserve
  the unstaged edits separately or finish staging that file, then retry.
- Formatting runs in a temporary export of the exact index, using its source,
  manifests, configuration and Make targets. It does not use unstaged working
  versions. It preserves executable lookup through the original checkout's
  prepared `.tools/host/bin`, `.tools/ic/bin` and `.tools/rust/bin`, in that order
  before the inherited PATH; an interactive shell export is not required for
  those tools. Prerequisite checks still enforce the selected versions, and
  formatting never installs missing tools. It clears inherited Git
  repository/index variables for formatters,
  while preserving the original commit index for its own checks and staging.
  It rejects inherited Make ignore-errors, dry-run, question, touch and version-only modes
  before dispatch, so a skipped or failed formatter cannot refresh the index.
- After successful formatting, the hook checks that the index and selected
  working files have not changed, copies formatting back to those selected files
  and refreshes only that selection. Unselected tracked files, untracked files
  and unrelated working edits remain untouched. Never use `git add -A`, add newly
  discovered formatting changes or stash/reset working edits inside the hook.
- If formatting fails, the real index and working files remain unchanged. A
  detected concurrent edit or staging change stops the hook; preserve that change
  and retry. Do not edit or stage concurrently with an active hook. If copying or
  staging itself fails, review the selected formatted files before retrying;
  no rollback may discard working edits.
- Snapshot formatting supports regular tracked files. Reject symlinks,
  submodules and unresolved index entries rather than following content outside
  the saved snapshot. A repository needing a different model requires an
  explicit maintainer-approved exception and an owned adapter outside the
  snapshot. Disposable formatter snapshots are helper-owned scratch, never a
  consumer build/evidence directory.

## Installation and adoption

Expose one setup command in every Rust consumer:

```make
.PHONY: install-hooks
install-hooks:
	bash scripts/dev/install-git-hooks.sh
```

Run `make install-hooks` once per clone and call it from the documented developer
setup/update flow. It sets only repository-local `core.hooksPath=.githooks`.
The installer refuses to replace another effective hook path, including inherited
or deliberately disabled settings, or to disable an executable private hook in
the default Git hooks directory. Reconcile existing hook obligations explicitly
before changing their location. Setup never changes global Git configuration or
silently chmods tracked files.
Hook paths are compared literally, including trailing newlines. A failed Git
configuration read must stop setup rather than count as an absent setting.

Adding tracked hooks does not activate them in an existing clone. Verify the
effective `git config --get core.hooksPath`, executable mode, snapshot integrity,
formatter prerequisites and the consumer's actual `fmt`/`fmt-check` targets.
Exercise automatic refresh, partial-stage rejection, failed formatter isolation
and unrelated working-edit preservation on the declared Linux/macOS hosts.
Report source/snapshot adoption and local activation separately. Shared Tooling
distributes and tests the Rust hook; its own shell-only checkout does not activate
it.

Consumers may replace repeated mechanical adoption fixtures with
`scripts/ci/check-formatting-hooks.sh`, retaining their selected manifests,
formatter inputs and product-specific cases. Its [documented contract](../docs/verification-helpers.md#consumer-formatting-hook-adoption)
requires already-prepared tools and retains failed exports. A committed snapshot
and focused native consumer qualification are still required before deleting
superseded checks.

Hooks are developer convenience, not CI or release evidence. Git permits bypassing
pre-commit hooks; CI must still enforce formatting independently. See the official
[Git hook documentation](https://git-scm.com/docs/githooks) and
[rustfmt usage](https://github.com/rust-lang/rustfmt#running).
