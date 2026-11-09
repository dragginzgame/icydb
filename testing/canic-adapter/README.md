# Canic adapter qualification

This unpublished workspace owns IcyDB's Canic-dependent qualification
([#596](https://github.com/dragginzgame/icydb/issues/596)). Its own
`Cargo.lock` keeps Canic's aligned SDK cohort out of the root package resolver.
Existing actor, schema and integration package directories have not moved; their
`package.workspace` selects this manifest explicitly. The maintainer approved
this exception on 2026-10-31; [#310](https://github.com/dragginzgame/icydb/issues/310)
retains further placement decisions.

This workspace inherits Rust 1.99.1 and the root's exact toolchain pin.
The existing workspace CI lane runs `scripts/ci/check-canic-adapter-compiler.sh`
for external native/Wasm consumers under both supported dependency spellings.
The retained lock does not qualify current repository path dependencies.
Direct fixture Wire remains 0.14.2; current root Wire has a different identity.
Full graph, generated endpoint and installed acceptance remain blocked under
[#373](https://github.com/dragginzgame/icydb/issues/373). Root compiler and
tooling-selection qualification remain independent.

The root workspace owns the database implementation, public libraries, CLI and
framework-neutral build/fixture helpers. It contains no Canic dependency, including
development dependencies. This workspace owns fixture serving, host macros,
endpoint compile contracts, lifecycle and installed tests. Canic supplies the
exports and host lifecycle; IC Testkit supplies simulator admission/lifecycle;
IC Wire supplies encoding and admission. No supplier checkout overrides are used.

The retained forward-qualification candidate selects Canic 0.113.0, Platform
0.8.3, Storage 0.64.0, Wire 0.14.2, Timers 0.32.4 and Testkit 0.75.0. Runtime
alignment remains deferred. A maintainer-approved tooling-only repair adds the root's
Cargo-selected Toko Tooling 0.36.0 record and 31 workspace dev-dependency edges,
preserving every existing version and checksum. Current root path dependencies
select Storage 0.70, Wire 0.18, Platform 0.27.1, Runtime 0.20.3 and Types 0.18.3.
This retained candidate still has its
recorded executor-links conflict and does not qualify current-source composition.

The earlier verified **canic-icydb 0.115.0**
archive selects Platform 0.22 but still requires Wire 0.16.1 for the endpoint
limits it exchanges with IcyDB. Root Wire 0.18 is a different type identity.
That publication does not establish a matching composed stack; do not substitute
supplier paths or bridge those limits locally. Follow
[IcyDB #373](https://github.com/dragginzgame/icydb/issues/373) and
[Canic #590](https://github.com/dragginzgame/canic/issues/590).

The server runner follows this workspace's locked Testkit, matching its native
integration clients. Root Testkit 0.95 does not silently select another server
for this retained graph.

## Tooling admission

`make check-adapter-tooling` admits the installed root snapshot, then uses
Shared's offline helper to admit both workspaces' tooling declarations and
lock selections and require the same version/checksum. Adapter fetching,
formatting, lint/test entrypoints and CI run this gate before later work.
Missing or mismatched nested selection fails even when root admission passes.
This checks tooling custody; it does not claim full locked Cargo resolution.
Cargo's attempted full refresh still fails on the existing executor-links
conflict. The one-off tooling-only repair was explicitly approved instead of
changing the deferred runtime cohort; ordinary lock refreshes remain Cargo-owned.

## Historical candidate evidence

The observations below predate the root cohort above. They qualify only their
recorded source and dependency selections, not current-source composition.

Fixture-serving strict lint and native/Wasm compilation pass, as do both
non-metrics endpoint-fixture tests. Metrics-enabled generated endpoints still
fail four Wire-limit type checks; earlier integration lifecycle encoding failures
remain unqualified. Earlier installed passes qualify only
the source and dependency identities they tested. No compatibility bridge or
retired endpoint wrapper is supplied.

Extended native qualification passes all 11 active facade-only tests and the
participant re-entry/retry test; the allocation-attribution test stays ignored.
The facade fixture selects Memory 0.63.0 for types exchanged with current IcyDB.
Eleven compiler rejection fixtures pass, but five have extra Wire-version
diagnostics and lifecycle declaration compilation fails at the same boundary.
Their expected diagnostic snapshots remain unchanged.

This is an adapter qualification limitation, not a product upgrade or release
blocker. An adapter may retain an older qualified matching IcyDB/SDK stack.
This workspace currently uses repository path dependencies to test pending IcyDB
source; advancing those paths requires new qualification before deploying that
composition. An older installed pass cannot qualify the new core automatically.

Native clients use the internal `icydb::__macro::RpcMethod` contract, without
loading Canic. The server still uses Canic's published method contract. The
`rpc_contract` test compares all 12 identities and native frames; lifecycle
arguments remain Canic-owned. Neither the client projection nor fixture serving
implements another codec or exports production endpoints.

Prepare dependencies with `make fetch-adapter`. Select this manifest explicitly:

```sh
cargo test --locked --manifest-path testing/canic-adapter/Cargo.toml \
  -p icydb-testing-integration --test rpc_contract
cargo test --locked --manifest-path testing/canic-adapter/Cargo.toml \
  -p icydb-testing-canic-adapter --test compile --features sql,metrics,migration
```

Installed tests additionally require the owner-selected Testkit setup documented
in [INSTALLING.md](../../INSTALLING.md). Product `test`, `clippy`, `validate`, `fmt-check` and release preflight select
only the root workspace. Explicit `test-adapter`, `clippy-adapter`,
`validate-adapter` and `fmt-adapter-check` retain composition coverage. Prepare
its server with `make install-testkit testkit-check`. The path-scoped
`Canic qualification` workflow owns installed browser/lifecycle tests, adapter
MSRV and two-profile actor size evidence. Invoke it manually for a proposed
core/adapter pairing when the core source changes. Do not make its status a
global requirement for unrelated core changes. Full gates remain maintainer-owned.

Actor-building artifact tests live in the integration package. The explicit
`test-canister-artifact-contract` gate selects both its SQL and fleet checks;
the neutral Build package's ordinary tests do not compile Canic actors.

Private qualification versions do not follow root package release bumps. Product release preparation leaves this lockfile unchanged. Refresh its local
IcyDB package identities and qualify the selected graph explicitly when adopting
new core source. Public package metadata and version management remain in the
root release workflow.
