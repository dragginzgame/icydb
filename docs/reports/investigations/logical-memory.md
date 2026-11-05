> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Lifecycle instruction-ceiling investigation

Date: 2026-09-17. The user chose to retain the 12,750,000-instruction ceiling.
Verdict: published `ic-memory 0.14.3` now passes that ceiling in IcyDB. The
prototype below is historical evidence; no local dependency override is adopted.

## Results

Same maintained lifecycle fixture and test sequence, Rust 1.98.1, PocketIC 16,
existing local/production build profiles and canonical post-link pipeline.
The published run is the retained release-failure log; candidate results come
from all three lifecycle tests against the temporary dependency override.

| Phase | Published 0.14.2 instructions | Byte-string prototype |
| --- | ---: | ---: |
| Init | 5,756,243 | 4,170,766 |
| Empty post-upgrade | 11,217,284 | 4,989,358 |
| Populated post-upgrade | 14,473,114 | 5,086,348 |
| Converged post-upgrade | 14,985,588 | 5,127,761 |

Worst-phase reduction: 9,857,827 instructions (65.782%). All three lifecycle
tests pass without raising the ceiling, including rollback/retry, ingress
surface, synchronous ordering, deferred activation and retained-row checks.
Twelve memory-admission tests and the default-manager test also pass. Allocated
stable extent remains 23,134,208 bytes in the lifecycle fixture. Raw Wasm and
cycle deltas were not measured; no native timing is used.

## Required upstream follow-up

Filed as [ic-memory #7](https://github.com/dragginzgame/ic-memory/issues/7),
including the measurements, prototype and safety qualification requirements.

This changes the outer persisted CBOR representation and needs an explicitly
qualified pre-1.0 hard cut, retaining format version 1 and requiring recreation
of earlier data. Do not ship the prototype as an IcyDB patch override. The
upstream owner must qualify malformed/oversized byte strings, current-form
fixtures, checksum/slot recovery, interrupted/refused writes and public DTO
serialization implications before publication. The experiment does not replace
that qualification, and does not prove how much of the original regression was
introduced by each earlier dependency change.

After the experiment, the workspace override, probes and temporary lockfile
changes were removed, restoring published 0.14.2 pending upstream release. Temporary native
lock resolution changed host-only edges during probing; no such change is retained.
Local PocketIC instances were used; no application network or database was changed.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/17/logical-memory/03/report.md).
