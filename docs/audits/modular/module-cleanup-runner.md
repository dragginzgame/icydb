# MODULAR AUDIT - Module Cleanup Runner

Use this workflow when the user asks to clean up a named module or crate using
Module Surface Hardening rules.

This is the implementation runner for
`docs/audits/modular/module-surface-hardening.md`. The MSH document owns policy,
taxonomy, authority rules, and full audit reporting. This runner owns the short,
repeatable cleanup loop.

## Purpose

Clean a named module to production grade by deleting, narrowing, inlining,
moving, or explicitly retaining code with authority. Do not redesign the module.
Do not perform style-only cleanup unless it is adjacent to a real
removal/narrowing patch.

A module is production-grade when every retained item has an owner and authority
reason, every removable stale surface is gone, every public/exported item is
intentionally public, tests do not force production visibility wider than
needed, and hot or wasm-sensitive cleanup does not change runtime shape without
proof.

## Inputs

Capture before editing:

| Field | Value |
| ---- | ---- |
| target module | path |
| owning crate | crate name |
| expected hotness | `cold` / `warm` / `hot-runtime` / `encode-decode-hot` / `query-executor-hot` / `wasm-sensitive` / `test-only` |
| patch mode | `implementation-requested` |
| full MSH escalation needed? | yes/no |

Escalate to the full MSH report when the module touches facade API, generated
code, schema authority, storage formats, encode/decode loops, query execution,
commit/recovery, or wasm-sensitive code with unclear runtime shape.

## Phase 1 - Mechanical Inventory

List:

* public, `pub(crate)`, `pub(super)`, and `pub(in ...)` items
* `#[doc(hidden)]` items
* cfg-gated and test-only items
* re-exports and facade exports
* one-caller helpers
* public helpers consumed only by tests

Search for:

* `allow(dead_code)`, `expect(dead_code)`, and `expect(unused_imports)`
* `legacy`, `compat`, `compatibility`, `fallback`, `shim`, and `deprecated`
* `EntityModel` / `IndexModel` runtime reconstruction
* duplicate entrypoints
* test-only production consumers

Identify direct consumers through compiler output, direct imports, focused
search, and tests. Do not classify from text counts alone.

## Phase 2 - Classify

For each candidate, assign:

| Field | Values |
| ---- | ---- |
| surface class | `live-authority`, `live-generated-boundary`, `live-diagnostics`, `live-test-support`, `stale-compatibility`, `stale-generated-fallback`, `orphaned-helper`, `overexposed-internal`, `duplicate-surface`, `unclear` |
| confidence | `high`, `medium`, `low`, `blocked` |
| disposition | `DELETE NOW`, `NARROW NOW`, `INLINE NOW`, `MOVE OWNER`, `MOVE TO TEST`, `RETAIN WITH OWNER`, `DEFER WITH TRIGGER`, `RETAIN HOT PATH`, `MEASURE FIRST`, `PATCH WITH PROOF`, `REJECT CLEANUP`, `BLOCKED` |

## Phase 3 - Patch Safe Items Only

Allowed by default:

| Class | Default action |
| ---- | ---- |
| `orphaned-helper` with high confidence | delete or inline |
| `overexposed-internal` with medium/high confidence | narrow visibility |
| test-only production helper | move to test support |
| generated-only public surface | move behind `__macro` or generated boundary |
| stale compatibility before `1.0.0` | delete when compile/tests prove it |
| one-caller helper with no invariant | inline |

Not allowed without proof or owner decision:

| Surface | Default action |
| ---- | ---- |
| public facade removal | `BLOCKED` or `DEFER WITH TRIGGER` |
| generated-boundary removal | `BLOCKED` until generated output or derive tests prove safety |
| persisted format or recovery behavior | full MSH report and owner decision |
| hot-path shape change | `MEASURE FIRST` unless shape is unchanged |
| closure/generic/iterator rewrite in encode/decode or query loops | `MEASURE FIRST` or `RETAIN HOT PATH` |
| allocation, clone, formatting, or dynamic dispatch added to success path | `REJECT CLEANUP` unless proof exists |

## Phase 4 - Validate

Run the smallest meaningful validation:

* `cargo fmt --all` after code edits
* `cargo check` for the owning crate
* focused tests for the module
* clippy for the owning crate when the slice is ready
* dependency cleanup check if `Cargo.toml` changed
* raw wasm byte comparison if runtime canister payload or wasm-sensitive code
  changed
* focused benchmark or instruction proof if hot-path shape changed

Do not repeatedly rerun expensive failing commands. Capture the first failure,
fix the direct cause when it belongs to the slice, and report anything broader.

## Phase 5 - Compact Report

Use this report shape for ordinary module cleanup:

```markdown
# MSH Module Cleanup: <module>

## Verdict
- Risk score:
- Patch mode:
- Cleanup result:

## Removed / Narrowed / Inlined / Moved
| Item | Action | Why safe | Validation |
| ---- | ---- | ---- | ---- |

## Retained With Owner
| Item | Owner | Authority reason | Trigger to revisit |
| ---- | ---- | ---- | ---- |

## Blocked / Measure First
| Item | Reason | Required proof |
| ---- | ---- | ---- |

## Verification
- cargo check:
- focused tests:
- clippy:
- wasm/raw-size check, if relevant:
```

Use the full MSH report only for high-risk modules, public/facade surfaces,
generated-boundary involvement, storage/encoding/query hot paths, recovery, or
unclear authority.
