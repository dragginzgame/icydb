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
The lock now resolves current repository path dependencies with `--locked`.
Direct fixture Wire uses 0.14.2, matching IcyDB's codec and limits. This repairs
preparation and fixture serving under [#373](https://github.com/dragginzgame/icydb/issues/373);
generated endpoint and installed acceptance remain separate. Root compiler
qualification remains independent.

The root workspace owns the database implementation, public libraries, CLI and
framework-neutral build/fixture helpers. It contains no Canic dependency, including
development dependencies. This workspace owns fixture serving, host macros,
endpoint compile contracts, lifecycle and installed tests. Canic supplies the
exports and host lifecycle; IC Testkit supplies simulator admission/lifecycle;
IC Wire supplies encoding and admission. No supplier checkout overrides are used.

The current forward-qualification candidate selects Canic 0.113.0 from
`247ba9d3599658c478373cb0dfc74439a859dd8a`, Platform 0.8.3 and Timers
0.32.4. Direct Wire is 0.14.2, Memory is 0.60.0 and Testkit is 0.75.0.
The lockfile is authoritative. Current root paths additionally select Platform
0.17.0, Wire 0.14.2, Memory/Storage 0.63.0 and Metrics 0.26.0. The older
Canic-owned runtime dependencies remain selected; resolving both families is
not proof that their public types compose.

**This candidate does not yet qualify current-source Canic composition.**
Canic's exposed Wire 0.7 limits and lifecycle encoders cannot consume the current
root Wire API ([Canic #590](https://github.com/dragginzgame/canic/issues/590)).
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
