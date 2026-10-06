# Flow convergence and duplication

Apply the [common audit contract](README.md) and
[canonical authority guidance](../docs/principles/simplicity-and-maintainability.md#canonical-authority-and-converged-flow).
Review affected behavior after changes to entrypoints, adapters, generated
boundaries, diagnostics or replay, or for an explicitly requested baseline.

The maintained flow is `owner derives -> contract carries -> consumers project`.
Distinct public entrypoints can remain separate while equivalent internal
semantics converge. This review concerns duplicated decisions and flow, not
line counts, style or a general correctness/performance verdict.

## Trace

| Behavior | Canonical owner | Inputs | Carried contract | Consumers |
| --- | --- | --- | --- | --- |

For each selected behavior, trace every maintained entry surface through its
convergence point, execution and result projection. Include generated, facade,
prepared, diagnostic and recovery paths when they participate. Record unclear
ownership rather than inventing it.

Inspect branch conditions and outcomes for equivalent semantic trees, downstream
policy rediscovery, adapters that reinterpret rather than translate, repeated
conversions, stale wrappers and separate implementations of equivalent inputs.
Text matches are discovery aids, not evidence of equivalent behavior.

Before proposing consolidation, determine whether separation independently
protects a trust boundary, contains corruption/recovery, belongs to a different
semantic owner or has current cost evidence. Preserve those obligations and
layering. Hot-path allocation, cloning, formatting, dispatch or generic code
growth requires the consumer's focused measurement evidence.

## Findings

Classify as duplicate flow, policy rediscovery, late convergence, ownership
leak, stale surface, protective duplication or measured specialization. Record
the duplicated behavior axis, multiplied decision sites, present friction,
proposed convergence owner and public/persisted contract impact.

Choose a disposition: delete, consolidate, localize, keep for boundary safety,
keep for measured cost, or no action. Retained separation needs a named reason.
Include the owner map, entry-to-result trace, retained separations and any
state-space delta in the common report. Route distinct domain defects to their
owning methods; do not duplicate their findings here.
