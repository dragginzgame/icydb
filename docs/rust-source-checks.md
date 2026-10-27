# Rust source checks

[The runtime panic scanner](../scripts/ci/check-runtime-panics.rs) accepts a
nonempty list of explicit Rust source paths and reports file/line/column hits.
Exit status is zero for no violations, one for forbidden constructs and two for
missing input or unreadable/malformed source. Consumers own inventory, roots,
required nonempty directories and application diagnostics.
`--help` prints usage successfully without reading source files. Put `--` before
relative paths beginning with `-`. Non-UTF8 path arguments reach the filesystem
unchanged; hosts that cannot represent such filenames report a source-IO error
with status two. Unknown options are CLI errors with status two.

Compile this snapshot source as a binary target in a consumer-owned private
package using the consumer's existing workspace and selected lockfile. It needs
Rust 2024 on Rust 1.88 or newer,
Clap 4 with `std`, `help`, `usage` and `error-context`, `proc-macro2` with
`span-locations` and `syn` with `full` and `visit` (the consumer
selects compatible published versions). It needs no product/runtime dependencies
or separately installed executable. The distributed source adds no Rust package
or dependency lockfile to the consumer.
When adopting the Clap-based source, add the inherited `clap` dependency to the
private compiler package and prepare its selected lockfile before compilation.
Refreshing the source alone is insufficient; do not patch the vendored scanner.

The scanner conservatively excludes items proven absent when `test` is false,
including nested test-only modules and associated items. Unknown cfg predicates
remain potentially production. It scans macro token trees, excludes strings and
comments, permits debug assertions, and excludes only a direct anonymous unit
const assertion. It is a syntax check, not macro expansion or type analysis.

Run the shared [semantic fixtures](../scripts/ci/test-runtime-panics.sh) with the
absolute compiled scanner path as the only argument. Consumer checks retain
inventory/discovery and one actual-source invocation, rather than copying these
scanner regressions. Qualify compiler/dependency changes in the consumer.
The exported lifecycle companion anchors semantic-fixture evidence to its
physical directory, preserving cleanup and diagnostics if temporary-root aliases
change during execution.

Shared Tooling's native CI compiles the scanner on Linux and both macOS hosts
using the producer-only templates and locked graph under `ci/runtime-panics/`.
This representative graph does not select consumer dependencies. CI explicitly
prepares Rust 1.88 with Clippy and rustfmt, then runs
`bash scripts/ci/qualify-runtime-panics.sh 1.88.0 NEW-ABSOLUTE-EVIDENCE-DIRECTORY`.
The helper requires that compiler to be prepared already; it performs no tool
installation. Cargo can fetch the exact locked sources; explicit offline settings
remain authoritative. Compilation, strict Clippy, formatting and semantic logs,
copied inputs, their checksums and build outputs remain in the selected directory
on success or failure. Consumer source inventories and actual-source scans remain
with consumers.
