> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

An isolated copy of current source compiles a private [probe](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/16/cache-traversal/01/probe.rs.txt) beside
`query/intent/cache_key/mod.rs`. It invokes the **actual derived StructuralQueryCacheKey
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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/16/cache-traversal/01/report.md).
