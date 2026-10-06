# IcyDB module surface hardening overlay

Apply [the shared method](../../../../audits/module-surface-hardening.md).

IcyDB adopts Shared Tooling `d957d1f8801885c5b69e4a9ef900155f5f2a8a9d` through
[the snapshot](../../../../.shared-tooling.snapshot). Apply the shared method
and [local audit governance](../../README.md) together. This changes method
identity; earlier reports remain historical and affected comparisons are
`N/A (method change)`. No audit supplies implementation or broad-gate authority.

## Identity and scope

- Report scope: `module-surface-hardening`.
- Method: shared revision above plus `ICYDB-MSH-4`; record both identities.
- Reports: the requested recurring, release-closeout or investigation location
  in [local governance](../../README.md#report-locations).
- Start with the named module in `icydb-core`; include facade, model/macros,
  config, CLI and schema only for reachable authority or generated consumers.

Exclude historical docs, target/build output and unrelated modules. Tests and
examples join scope when they explain retained surface or widen production APIs;
useful test support is not dead merely because production does not call it.
Review `sql`, `metrics`, `migration`, generated endpoint switches, diagnostics,
`__macro` and hidden exports at their maintained consumer boundaries.

## Product authority

- Accepted schema snapshots govern runtime planning, execution, decoding and mutation.
- Generated `EntityModel` / `IndexModel` serve proposal, reconciliation,
  model-only convenience and tests; never runtime fallback reconstruction.
- SQL DDL lowers into catalog-native mutation.
- Public generated endpoints use `icydb_*`; hidden Rust wrappers may use
  `__icydb_*` to avoid user-hook collisions.
- Facade exports express user concepts; generated-only support belongs behind
  the generated boundary. Require macro expansion, generated output or direct
  derive/fixture evidence before retiring generated surface.
- Persisted decoding stays bounded and fallible. Pre-1.0 hard cuts preserve
  discriminator identity, retained installations and same-contract recovery;
  resolve effects, assets and liabilities before reset or retirement.

Apply [architecture contracts](../../architecture-contracts.md),
[AGENTS.md](../../../../AGENTS.md) and the owning domain proof. Reachability is
an observation; the current invariant and caller authority explain retention.

## Runtime shape and qualification

Classify candidates as cold, warm, hot runtime, encode/decode hot,
query-executor hot, Wasm-sensitive or test-only. Inspect allocation, cloning,
formatting, dispatch and generic expansion before proposing a hot-path cleanup.
Source brevity is not a cost measurement. Preserve current optimized walkers
and fail-closed checks unless matching focused proof supports the replacement.

Use raw non-gzipped deployable Wasm bytes for size; gzip is secondary. IC cycles
and instruction counts are the only execution-cost measures. No wall-clock or
native timing benchmarks are allowed. Require measurement only for changed
runtime shape at an affected boundary; otherwise use its focused compile,
generated and behavioral checks. Report unavailable required proof explicitly.

Use [the cleanup overlay](module-cleanup-runner.md) only within implementation
authority. Tests must execute the intended assertions under the exact target,
features and source identity; [verification readout](../../README.md#verification-readout)
owns local command selection. Full gates remain user-owned.
