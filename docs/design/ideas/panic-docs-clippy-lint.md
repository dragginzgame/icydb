# Panic Docs Clippy Lint

## Status

Accepted. The workspace lint is raised to `warn` after the 2026-06-16 baseline
cleanup.

## Context

The workspace sets `clippy::missing_panics_doc` to `warn` in `Cargo.toml`.
The code-hygiene standard requires `# Panics` sections for public APIs with
reachable panic paths.

IcyDB generally prefers typed errors and subsystem invariant helpers over naked
`panic!`, `unwrap`, or `expect` in runtime code. Tests may use `expect` when the
message improves failure diagnosis.

Public Rust doc comments should not be treated as a raw wasm-size knob. A local
spot check on 2026-06-16 compiled identical `wasm32-unknown-unknown` sources
with and without large public `///` docs using `opt-level=z`, fat LTO,
single-codegen-unit, and stripped symbols; both emitted 107-byte wasm files and
were byte-for-byte identical.

## Problem

The hygiene rule and the workspace lint setting were previously able to drift:

- When the lint was `allow`, review had to catch missing panic docs manually.
- If the lint is raised globally, Clippy may create noisy churn for internal
  public-in-crate APIs, generated-code support surfaces, tests, and code whose
  panic path is an internal invariant violation rather than a caller contract.

## Options

### Keep `missing_panics_doc = "allow"`

Keep the current lint setting. Enforce caller-triggered public panic docs through
review, targeted audits, and the code-hygiene standard.

Pros:

- Avoids broad docs churn.
- Keeps source focused on real caller contracts.
- Does not pressure internal invariant failures into public API prose.

Cons:

- Missing `# Panics` docs remain easy to miss.
- Enforcement depends on review discipline.

### Raise To `warn` In Selected Crates

Start with externally consumed facade crates and keep runtime-internal crates on
`allow`.

Pros:

- Gives better coverage where public docs matter most.
- Limits churn in internal runtime modules.

Cons:

- Requires per-crate lint plumbing and baseline cleanup.
- Still may flag panic paths that should become typed errors instead of docs.

### Raise To Workspace `warn`

Make `missing_panics_doc` a workspace warning.

Pros:

- Simple, broad enforcement.
- Prevents new undocumented public panic contracts.

Cons:

- Likely high baseline churn.
- Too blunt for generated-code support, tests, and crate-visible runtime APIs.
- May encourage low-value `# Panics` sections rather than better fallibility.

## Decision

Raise `missing_panics_doc` to workspace `warn`. A focused baseline audit found
67 raw diagnostics collapsed to 18 unique source locations, small enough to
clean up directly. The cleanup separated three cases:

- APIs that should return typed errors instead of panicking.
- APIs that intentionally expose caller-triggered panics and need `# Panics`.
- Internal invariant failures that should use subsystem invariant helpers and
  should be documented as invariant violations when they remain visible through
  public APIs.

The lint remains warning-level rather than deny-level so future cleanup can
prioritize typed errors over mechanically adding low-value panic prose.

## Acceptance Criteria For Any Lint Change

- Raw wasm size is compared before and after using non-gzipped `.wasm` bytes.
- Tests and generated-code support are either excluded or explicitly justified.
- Newly added `# Panics` sections distinguish caller-triggered conditions from
  internal invariant violations.
- Any panic path that can reasonably become a typed error is converted instead
  of documented as a stable panic contract.
