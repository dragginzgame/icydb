# Rust source checks

[The runtime panic scanner](../scripts/ci/check-runtime-panics.rs) accepts a
nonempty list of explicit Rust source paths and reports file/line/column hits.
Exit status is zero for no violations, one for forbidden constructs and two for
missing input or unreadable/malformed source. Consumers own inventory, roots,
required nonempty directories and application diagnostics.

Compile this snapshot source as a binary target in a consumer-owned private
package using the consumer's existing workspace and selected lockfile. It needs
`proc-macro2` with `span-locations` and `syn` with `full` and `visit` (the consumer
selects compatible published versions). It needs no product/runtime dependencies
or separately installed executable. Shared Tooling does not add a Rust package
or another dependency lockfile for the distributed source.

The scanner conservatively excludes items proven absent when `test` is false,
including nested test-only modules and associated items. Unknown cfg predicates
remain potentially production. It scans macro token trees, excludes strings and
comments, permits debug assertions, and excludes only a direct anonymous unit
const assertion. It is a syntax check, not macro expansion or type analysis.

Run the shared [semantic fixtures](../scripts/ci/test-runtime-panics.sh) with the
absolute compiled scanner path as the only argument. Consumer checks retain
inventory/discovery and one actual-source invocation, rather than copying these
scanner regressions. Qualify compiler/dependency changes in the consumer.
