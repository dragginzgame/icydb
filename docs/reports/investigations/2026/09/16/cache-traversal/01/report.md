# Cache traversal instruction qualification

2026-09-16 · authorized line 0.257 · production implementation unchanged.

## Decision

Keep the existing cache flow. These measurements demonstrate no need for another
retained-graph walker, comparison budget, cache representation or insertion mode.
Retained traversal stayed below 5.9 million IC instructions in the tested matrix;
independent-key lookup with 2 MiB of text stayed below 79 million. Both are below
the existing 500-million read/request failure reserve. This is useful measured
headroom, **not a universal worst-case bound or proof that the reserve is otherwise
unused**. No accounting guarantee or deferral has been changed.

The existing visitor already charges container backing before visiting children,
caps recursion and recharges shared allocations per reference. Primitive buffers
skip element walks. For the measured owners, this bounds traversal shape without
another counter; it is not an exact instruction-to-byte conversion formula.

R2 kernel qualification is complete for these fixtures. Integrated complete-plan
and accepted-metadata payload qualification remains with the existing R1/R4/R2
review; this probe does not close all preparation work. Do not reopen the accepted
standard-container insertion deferral or SORT-01 without their recorded triggers.

## Method and scope

An isolated copy of current source compiles a private [probe](probe.rs.txt) beside
`query/intent/cache_key.rs`. It invokes the **actual derived StructuralQueryCacheKey
Hash/Eq**, the standard HashMap lookup, and the **actual RetainedBytes visitor**
and production Value implementations. No surrogate walker or comparator is used.
A small measurement wrapper adds root inline storage, then calls the visitor.

Fixtures are constructed before the counter interval. Independent key allocations
force deep equality; separate shared handles exercise Rc's identity shortcut.
A mismatch in the final consistency field forces late comparison failure.
Hashing, equality, lookup and retention each have separate performance-counter
intervals, including counter/call overhead. Black-boxed inputs/results and runtime
assertions prevent unused operations from disappearing. Native timing is not used.

The key probes construct structural keys directly, not accepted application
queries. Their widths, depth and labels stress the representation around the
4,096-node, 128-depth and 2-MiB input limits, with explicit beyond-limit cases.
Some fixtures cannot pass input or semantic admission. The standalone map contains
one structural key; it does not include the session identity shell, collisions
among many keys, authority checks, preparation dispatch or full request execution.

Likewise, retention probes measure owned subgraphs, not complete cached plans.
Production additionally accounts both retained key copies and the artifact. A key
with roughly 2 MiB of labels can therefore pass this single-key visitor but fail
complete-artifact retention. A depth decline only makes an artifact ineligible
for caching; it is not a new public query-depth rejection.

## Results

| Key fixture | Hash | Independent equality / late mismatch | Shared equality | One-entry lookup | Retention walk |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 fields, 8-byte labels | 55,824 | 22,949 | 215 | 78,771 | 8,850 |
| 4,096 fields, 8-byte labels | 3,362,064 | 1,389,797 | 215 | 4,751,859 | 541,074 |
| One field, 126 unary wrappers | 33,598 | 8,144 | 215 | 41,740 | 6,639 (declines) |
| 1,024 fields, 1-KiB labels | 12,027,152 | 27,398,373 | 215 | 39,425,523 | 135,570 |
| 2,048 fields, 1-KiB labels | 24,050,960 | 54,795,493 | 215 | 78,846,451 | 270,738 |
| 4,096 fields, 1-KiB labels (beyond input cap) | 48,098,576 | 109,589,733 | 215 | 157,688,307 | 522,007 (declines) |

Sharing makes equality cheap; it does **not** make hashing constant-cost.
Payload comparison dominates the large-text cases. The 1-MiB and 2-MiB lookup
samples use respectively 7.89% and 15.77% of the 500-million reserve. These are
absolute kernel costs, not a before/after performance improvement.

| Retained subgraph | Accounted bytes | IC instructions | Result |
| --- | ---: | ---: | --- |
| List of 65,535 Null values | 4,194,304 | 3,146,052 | Accepted |
| List of 65,535 empty texts | 4,194,304 | 5,898,522 | Accepted |
| List of 65,536 empty texts | Over cap | 348 | Declined before child walk |
| List of 52,428 16-byte texts | 4,194,304 | 4,718,892 | Accepted |
| List of 3,855 1-KiB texts | 4,194,304 | 347,322 | Accepted |
| Blob filling remaining root allowance | 4,194,304 | 333 | Accepted |
| 1,024 references to one 32-text list | 2,445,324 | 3,112,241 | Accepted; charged per reference |
| 2,048 references to that same list | Over cap | 5,334,524 | Declined during walk |

Wasm32 Value size is 64 bytes in this build. String/blob retention reads capacity,
not every payload byte, explaining why wide empty values cost more than large
scalars. The highest sampled retention cost is 1.18% of the failure reserve.
Boundary fixtures cover exact-cap admission. Every admitted retention subgraph
also rejects when its allowance is one byte below its measured weight.
List nesting accepts 42 wrappers and declines at 43 here; key unary nesting
accepts 59 and declines at 60. The visitor's 128-depth ceiling counts container
and owner layers, not just logical query/value nesting. No trap occurred.

## Validation and reproduction

All **189 samples** pass: 63 fixtures, three identical repeats, plus one layout
discovery call. [Samples](samples.csv) consolidate repeats; the runner asserts
all instruction counts and outputs are identical. For kind 0, m0..m8 are hash,
equal, late-mismatch, shared-equal, lookup, retention instructions, retained bytes,
admission flag and hash checksum. For kinds 1..5, m0..m6 are first/repeated/one-byte-
smaller retention instructions, admitted bytes (zero on rejection), admission
flag, root size and Value size; m7/m8 are padding. Kind definitions are in the probe.

Focused production validation passes: 81 all-feature tests and 17 SQL-disabled
tests covering retention, identity, lifecycle, rejected publication and rebinding.
No full suite was run. Production Wasm/cycle deltas and full-query instruction
costs are unmeasured; no runtime change is proposed.

Build: Rust 1.98.1 (48a229cea), wasm32-unknown-unknown, workspace wasm-release
profile (opt-level=z, fat LTO, one codegen unit, panic=abort, stripped symbols),
no wasm-opt. Probe raw Wasm is **36,432 bytes**, with **137 defined functions**
and four imports. This is a microcanister footprint, not a production size delta.
HEAD: `38d4067a04a2dc11421fc91fa06fa077d9732840`; prior dirty worktree preserved.

Reproduce by copying current Cargo manifests/lockfile, crates, canisters, schema,
testing and toolchain into an isolated directory. Place probe.rs.txt at
`crates/icydb-core/src/db/query/intent/cache_key/qualification.rs`, and declare
`mod qualification;` in cache_key.rs. Build icydb-core alone with `cargo rustc
--locked --offline -p icydb-core --lib --no-default-features --profile wasm-release
--target wasm32-unknown-unknown --crate-type cdylib`; copy its Wasm as probe.wasm.
The probe's raw IC exports have an isolated unsafe-code allowance; production
lint policy is unchanged. Compile [runner.rs.txt](runner.rs.txt) against the cached
PocketIC 16.0.0 rlib/dependencies and run it with the artifact directory argument
and POCKET_IC_BIN pointing to server 16.0.0. Source paths in panic metadata can
affect reproduced Wasm bytes. Original directory: `/tmp/icydb-cache-qualification.HMTdIs`.

The first sandbox server-start attempt was stopped without samples; local-network
permission enabled subsequent runs. Each runner releases its isolated PocketIC
instance on exit. No shared ICP network was changed. An initial probe build
rejected raw exports under unsafe-code lint; the allowance is probe-local only.

SHA-256:

- Production cache_key.rs: `39dd090a7b318dd1012105c8f851e76ca9470efc462f8a0c0bd68fef808129b2`
- Production retained.rs: `0eb84177218b3b44eda9f2e0ce72766ce75022d075ef2adf1b7451f5308288ad`
- Probe: `a6b784abcb5d255414360965e9c6c317a8b914a423f1923940fe36a8d5d90993`
- Runner: `f64f46b1c3c8d71a81da717d9650b2a8659bdf7427069adca8d47e3fb56d3c5c`
- Wasm: `bb3921381892f55c85b983044edaa8efbda31d86787b523fb53dcecc6f9ad3ee`

Evidence-only handoff: six report/artifact/tracker files, approximately 480 added
lines; zero production lines and no runtime complexity change. Release notes are
unchanged because no runtime or maintained test surface changes.
