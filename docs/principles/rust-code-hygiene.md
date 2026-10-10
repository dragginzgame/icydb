# Rust Code Hygiene Baseline

## Purpose

This baseline captures Rust practices that recur across the current consumers
without imposing one repository's module names, banners, lint set, or runtime
architecture on another.

## Ownership and module shape

- Give each behavior one clear owner and expose intentional APIs from that
  owner boundary.
- Prefer narrow visibility. Widen a symbol only for a demonstrated consumer.
- Keep imports at file top and group them consistently with the consumer's
  documented house style.
- In code that requires `std`, use `std::...` instead of `core::...` whenever
  the equivalent API is available through `std` at the package's supported MSRV.
  Apply this to imports, fully qualified paths, tests and examples.
- Preserve `core::...` where needed for maintained `no_std` support, including
  conditional builds and generated code targeting `no_std` consumers. Document
  that support boundary in the owning crate or local overlay. Import consistency
  must not add a `std` requirement or raise MSRV.
- Use ordinary Rust module discovery. Avoid `#[path]` wiring and ambiguous
  duplicate file/directory module shapes.
- Keep a type, its inherent implementation, and its trait implementations near
  one another when that improves navigation.

## Documentation and comments

- Public APIs document purpose, ownership, invariants, and caller-visible
  failure behavior.
- Public APIs with reachable panic paths include a `# Panics` section; prefer a
  typed error when callers can recover.
- Non-trivial private logic explains intent, invariants, or phase boundaries.
  Do not restate the next line or preserve stale implementation history.
- Module-level documentation should identify responsibility and what the module
  deliberately does not own.

Consumers may define stricter formatting for type documentation, import order,
or section banners.

## Errors and trust boundaries

- Prefer typed errors over panics for invalid input, persisted data, external
  state, and recoverable invariant failures.
- Do not match rendered error strings in code or tests when a variant, kind, or
  observable state is available.
- Persisted decoding is bounded and fallible. Validate before publishing data
  into runtime authority.
- Keep defensive validation at independent trust boundaries even when semantic
  decisions are carried from an upstream owner.

## Tests

- Keep owner-local tests near the owning code.
- Put cross-module behavior tests at the subsystem boundary they exercise.
- Add negative coverage for public validation and trust boundaries.
- Test maintained contracts rather than private implementation layout.
- Prefer fixtures and deterministic substitutes over live network calls in
  unit tests.

## Tooling

Use `rustfmt` as the formatting authority and Clippy as a linting baseline.
Consumers own exact commands, toolchain versions, lint configuration and
qualification gates within the shared baseline. Focused checks run automatically
during authorized development; the documented full validation suite runs before
delivering the completed code change as ready. Release execution retains its
separate authorization. Wasm and host-specific constraints must preserve the
baseline's required macOS support and evidence rules.
