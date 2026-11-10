# IcyDB 0.307 — Wire adoption

Following the completed 0.306 deployment and the minor-boundary explanation,
the maintainer requested proceeding with the new Wire and Metrics adoption.
Package versions, deployment and the deferred Canic runtime stack are unchanged.

## Landing slice

1. Adopt published Wire 0.19.0 and Metrics 0.29.10 in the root graph, preserving
   unrelated selections. Qualify schema/facade Wire contracts, metrics consumers,
   browser bindings and the CLI. Carry pending database tooling notes to 0.307.0;
   retain published notes and the independent CLI ledger.

2. Adopt the requested Host Foundation 0.76.2 patch: align direct requirements
   with the incoming lock and qualify CLI/build consumers. Preserve Host Tools
   0.88.0, Host Text 0.74.3 and the deferred Canic graph.

3. Adopt Host Tools 0.89 and Testkit 0.97 with their required Platform 0.29
   dependency. Qualify native testing exports, startup polling and CLI/build
   consumers; preserve package versions and independent Canic qualification.

## Validation

Focused adoption complete. Evidence: `target/evidence/wire-019-metrics-02910/`.

- Strict Schema/Core/facade library and test-target Clippy passed.
- Five schema Wire, seven facade Wire and 14 metrics tests passed.
- CLI all-target and all-feature facade Wasm compilation passed.
- Browser qualification preserved the reviewed contract baseline: 91 native
  frames across 12 methods, 3,112 truncated frames and malformed/limit controls.
- Root/nested tooling admission, dependency pins, documentation, validation-stage
  contract and diff whitespace checks passed. The initial pin check exposed
  stale tooling exception values; both now match the selected 0.38.2 pin.
- Metrics 0.29.10 was already present in the incoming lock captured for this
  slice; its direct minimum is now aligned. Cargo changed only Wire and Derive
  relative to that saved lock. No Rust source adaptation was required.

Toko 0.38.2's separate qualification-layout migration remains outstanding under
#310; Canic runtime qualification remains deferred under #373. These are not
claims of complete layout or Canic adoption.
Full workspace/release and macOS checks are not requested. IC cycles,
instructions, and total/code-section Wasm size deltas are unmeasured.

## Host Foundation 0.76.2 closeout

The latest published Artifacts/Fs/Process/HTTP patch was already selected in the
incoming lock. Direct Artifacts/Fs/Process requirements now match 0.76.2; Host
Tools 0.88.0 and Text 0.74.3 remain the latest published selections. The lock is
byte-identical to this slice's saved input, preserving the pending Wire/Metrics
and tooling changes.

Strict CLI/build all-target lint passed. All 254 CLI tests passed. The initial
build run passed 21 tests and hit ETXTBSY in the optimizer shell fixture: a
parallel fork can retain a directly written executable's writable descriptor.
The fixture now uses Host Process's existing private-writer helper. Strict
build lint and all 22 build tests then passed; no production behavior changed.
Formatting, dependency pins and documentation checks passed. Evidence is under
`target/evidence/host-0762/`, including the initial failure and final passing run.
Full workspace/release and macOS checks remain skipped; no publication or
network lifecycle actions occurred. Performance deltas remain unmeasured.

## Host Tools 0.89 / Testkit 0.97 closeout

Adopted published Host Tools 0.89.0 and Testkit 0.97.0 with required Platform
0.29.2. Only those three lock records changed from this slice's input. Host
Foundation 0.76.2, Text 0.74.3, Types 0.19 and Wire 0.19 remain selected; the
independent Canic graph is untouched.

Initial compilation identified Host Tools' removal of the unsupported Oneway
Wasm-export variant. Removed IcyDB's obsolete fallible mode-admission helper;
raw export inspection retains the supplier's three actual method modes. Existing
export and policy tests qualify that boundary without a retired-name guard.

Strict facade/CLI/build all-feature all-target lint passed after that repair.
Two testing-wrapper tests, one startup budget-admission test, all 22 build tests
and all 254 CLI tests passed. The live PocketIC startup test remains explicitly
ignored; no simulator was started. All-feature facade Wasm compilation,
dependency pins, documentation, formatting and diff checks passed. Evidence:
`target/evidence/host-089-testkit-097/`, including the initial compiler failure.
No broad workspace/release validation, new macOS qualification, version changes,
commits or deployment occurred. Performance deltas are unmeasured.
