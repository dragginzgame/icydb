# Cross-repository tooling duplication

Apply the [common run contract](README.md) and the
[flow convergence method](../audits/flow-convergence-and-duplication.md).
Use an explicitly selected projects directory and prepared cloc.

From Shared Tooling, collect the existing inventory:

```bash
make cloc-tooling CLOC_PARENT=/absolute/projects/directory
# Optional per-file evidence, saved in this run's output directory:
perl scripts/dev/cloc-tooling.pl --json /absolute/projects/directory
```

The report scans immediate Git checkouts, so filter its findings to the selected
repository scope and state excluded rows. Separate verified shared copies from
locally owned tooling; use snapshot identity, hashes and modes rather than
filenames alone. Preserve partial results and errors.

Select one repeated behavior for deeper review. Trace callers, inputs, failure
and recovery semantics, ordering, host requirements and authority boundaries.
First look for an existing shared owner or obsolete code that can be retired.
Keep product policy and identities local. Similar text is a candidate, not proof
of an equivalent contract or justification for a new abstraction.

Return measured local/shared LOC, one concrete consolidation or deletion proposal,
its consumers and required focused qualification. Distinguish estimated removable
lines from measured removals; include the cost of shared replacements and adapters.
Coordinate through owning issues and preserve active work. This task does not
edit siblings, replace snapshots or promise a reduction before implementation.
