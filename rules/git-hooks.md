# Git formatting hooks

These rules are part of the [engineering baseline](../DRAGGINZGAME.md) for Rust
repositories. The standard pre-commit hook automatically formats the selected
commit payload. Repository-local Git hooks run before a commit; GitHub CI provides
the independent, non-mutating formatting gate.

## One formatting contract

- Vendor the reviewed Shared Tooling `.githooks/pre-commit` and
  `scripts/dev/install-git-hooks.sh` unchanged in the
  [recorded snapshot](../docs/consuming-snapshots.md). Keep both executable.
  Retire the superseded local formatter hook after reviewing its obligations.
- Every Rust repository provides `make fmt` and `make fmt-check`. Both cover the
  same maintained Rust workspaces, including standalone nested workspaces,
  examples or harnesses with their own manifests. Following IcyDB and Canic,
  `fmt` runs `cargo sort --workspace` before `cargo fmt --all` for each workspace;
  `fmt-check` uses `cargo sort --workspace --check` and
  `cargo fmt --all -- --check`. This sorts the root and every member's Cargo.toml,
  including the root dependency catalog and inherited child tables. Explicitly
  cover standalone/excluded manifests outside that workspace's member set.
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

For a single workspace, the minimum Make targets are:

```make
.PHONY: fmt fmt-check
fmt:
	cargo sort --workspace
	cargo fmt --all

fmt-check:
	cargo sort --workspace --check
	cargo fmt --all -- --check
```

For a separate `testing/` workspace, also run `cargo sort --workspace testing`
before `cargo fmt --manifest-path testing/Cargo.toml --all`, with their `--check`
equivalents in `fmt-check`. Pass one workspace root per `cargo sort --workspace`
invocation. `cargo-sort` owns its ordering and TOML rendering; do not add a second
hand-written sort or rewrite dependency declarations to enforce ordering.

## Selected files and working edits

- The hook records the selected added/modified/renamed files using NUL-delimited
  paths. It rejects a selected file with unstaged changes before formatting,
  including a partial selection of configuration or formatter inputs. Preserve
  the unstaged edits separately or finish staging that file, then retry.
- Formatting runs in a temporary export of the exact index, using its source,
  manifests, configuration and Make targets. It does not use unstaged working
  versions. It clears inherited Git repository/index variables for formatters,
  while preserving the original commit index for its own checks and staging.
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
