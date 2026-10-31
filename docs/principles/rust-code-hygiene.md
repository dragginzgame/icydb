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
- Use ordinary Rust module discovery. **Do not use `#[path]` module attributes**,
  including raw-identifier spelling and `cfg_attr` path overrides, in production
  code, tests, examples, benchmarks or generated Rust. Arrange files under their
  owning module or share code through the appropriate library crate. Do not
  substitute `include!` or generated path indirection to evade this rule.
  Attribute text in parser/scanner input fixtures is data, not permitted module
  wiring. Avoid ambiguous duplicate file/directory module shapes.
- Keep a type, its inherent implementation, and its trait implementations near
  one another when that improves navigation.

## CLI applications

Follow the mandatory [Clap CLI rule](../../DRAGGINZGAME.md#cli-applications).
Use the library's existing capabilities before adding custom argument handling.

## Metrics and logging

Follow the mandatory [observability contract](../../rules/engineering-details.md#metrics-and-logging):
use `ic-metrics` and `ic-logging`, keep application adapters thin, and resolve
capability gaps through issues and improvements in IC Observability.

## Documentation and comments

- Public APIs document purpose, ownership, invariants, and caller-visible
  failure behavior.
- Public APIs with reachable panic paths include a `# Panics` section; prefer a
  typed error when callers can recover.
- Non-trivial private logic explains intent, invariants, or phase boundaries.
  Do not restate the next line or preserve stale implementation history.
- Configuration comments explain non-obvious choices, constraints or exceptions.
  Self-explanatory lint names and levels need no running commentary. Avoid a
  paragraph per lint or generic rationales repeated across a configuration table.
- Module-level documentation should identify responsibility and what the module
  deliberately does not own.

Consumers may define stricter formatting for type documentation, import order,
or section banners.

## Errors and trust boundaries

- Prefer typed errors over panics for invalid input, persisted data, external
  state, and recoverable invariant failures.
- For native/`std` error types, use `thiserror::Error` for mechanical message
  formatting and ordinary source forwarding when the package already uses
  `thiserror`. Review handwritten `Display`/`Error` pairs type by type; custom
  behavior elsewhere does not justify retaining unrelated boilerplate.
- Preserve variants, field types, display text, source identity/chaining and
  conversion behavior when replacing manual implementations. Do not introduce
  automatic `From` conversions or transparent forwarding that changes the
  existing error boundary merely to shorten a declaration.
- Retain explicit implementations where recovery-source selection, conditional
  source projection, redaction or computed formatting has a real contract that
  the derive cannot express faithfully. Preserve those projections and qualify
  their actual callers; do not flatten them into strings or discard evidence.
  This rule does not require a new dependency for a minimal or `no_std` crate,
  or an `Error` implementation on a passive wire enum that does not need one.
- Do not match rendered error strings in code or tests when a variant, kind, or
  observable state is available.
- Persisted decoding is bounded and fallible. Validate before publishing data
  into runtime authority.
- Keep defensive validation at independent trust boundaries even when semantic
  decisions are carried from an upstream owner.

## Native operating-system interfaces

- Reuse the maintained higher-level owner first, including Host FS and Host
  Process where they provide the required contract. Direct native calls must
  not create a second filesystem, process-custody or cleanup implementation.
- Prefer safe standard-library APIs when their behavior fits. For remaining
  supported native operations, prefer Rustix's typed errors and owned/borrowed
  descriptor APIs over handwritten libc wrappers. Inherit workspace dependencies
  and select only required, target-gated features; this is not a requirement to
  add Rustix to Wasm, minimal libraries or unrelated packages.
- Retain scoped raw FFI where the supported host API is unavailable through
  Rustix, an external ABI requires it, or a reviewed post-fork path needs explicit
  async-signal-safe operations. Record the reason and ownership/safety contract.
  A shorter wrapper is not proof that behavior is equivalent: preserve error
  variants/errno, descriptor flags and inheritance, cleanup, and failure order.
  Qualify affected native Linux and macOS behavior separately; named-target
  compilation alone does not establish native execution.

## External inputs and native tools

- Admit machine output from its original bytes with strict encoding and the
  required response shape. Reject invalid entries and missing required fields;
  do not silently turn them into empty or partial success with lossy decoding,
  defaults or filtering. Optional fields and forward-compatible unknown fields
  remain protocol choices. Lossy text is for diagnostic presentation, with raw
  evidence retained separately when required.
- Production subprocess capture needs explicit stdout/stderr bounds and a
  deliberate deadline or cancellation policy. Reuse an existing qualified
  process owner where available. Reject truncated machine output before parsing;
  bounded diagnostic tails may be intentional. Preserve process/descendant
  custody, original failures and uncertain external effects; termination does
  not prove rollback or authorize retry. Interactive/inherited streams and
  deliberately long-running services retain their own lifecycle contracts.
- Escape untrusted text fields when rendering application-owned terminal
  diagnostics or reports. Preserve raw evidence and machine-readable output;
  presentation escaping must not become identity normalization. Keep trusted
  application layout, intentional colour and explicit interactive passthrough
  distinct from captured external text. Prefer the existing text owner's escape
  function over another private implementation.

## Tests

- Keep owner-local tests near the owning code.
- Put cross-module behavior tests at the subsystem boundary they exercise.
- Add negative coverage for public validation and trust boundaries.
- Test maintained contracts rather than private implementation layout.
- Prefer fixtures and deterministic substitutes over live network calls in
  unit tests.
- Required source inspections must report traversal, entry, metadata and
  decoding failures instead of passing after skipping inputs. Verify that the
  intended source set was inspected; declare optional inputs and exclusions
  explicitly. Do not use `flatten()` or `filter_map(Result::ok)` to erase scan
  failures. Scope this to required current inputs, not retired files or history.
- Repairs at inspection, decoding, capture and rendering boundaries need focused
  negative regression cases selected by the owning normal test/CI path. Cover
  the failure that escaped, not just a successful fixture. Shared rules guide
  review; adopting prose alone does not install executable enforcement.

## Lint suppressions

- Fix the underlying warning where practical. When suppression is justified,
  use `#[expect(lint_name, reason = "...")]` at the narrowest useful scope so
  the compiler reports an expectation that no longer matches a warning.
- Review every existing `allow` when adding, changing or auditing lint
  suppressions, including crate/module attributes, conditional attributes,
  generated Rust, Cargo lint tables and command-line settings. Replace it with
  a scoped expectation wherever the supported compiler, target and feature
  configurations permit. Shared helpers and test fixtures follow the same rule.
- Where a warning depends on configuration, first narrow the scope or use
  `cfg_attr` to express the applicable expectation. Retain an `allow` only when
  an expectation cannot faithfully cover the maintained configurations; record
  that specific limitation using `reason` for Rust attributes. For configuration
  settings, explain non-obvious limitations once at their shared scope instead of
  repeating them per entry. Convenience or a copied existing allowance is not a justification.
- Keep `unfulfilled_lint_expectations` enabled. Remove obsolete suppressions
  when the warning disappears instead of suppressing the unfulfilled expectation
  or changing it back to `allow`.
- Cargo lint tables and command-line lint levels do not support `expect`.
  Review their `allow` entries for relocation to source-level expectations;
  retain justified configuration-level choices without requiring an inline comment
  for every lint. Comment only where a non-obvious scope, constraint or exception
  helps maintainers; use supported configuration syntax.

See the [Rust lint expectation contract](https://doc.rust-lang.org/reference/attributes/diagnostics.html#the-expect-attribute)
and [Cargo lint configuration](https://doc.rust-lang.org/cargo/reference/manifest.html#the-lints-section).

## Tooling

Use `rustfmt` as the formatting authority and Clippy as a linting baseline.
Consumers own exact commands, toolchain versions, lint configuration and
qualification gates within the shared baseline. Focused checks run automatically
during authorized development under the
[shared validation policy](../../DRAGGINZGAME.md#validation-and-evidence), which
also requires Clippy before expensive release/deployment stages. Full-suite and
release execution retain their separate authorization. Wasm and host-specific
constraints must preserve the baseline's required macOS support and evidence rules.
