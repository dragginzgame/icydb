# Dependency advisory review

Apply the [common run contract](README.md). This initial task covers Rust
lockfiles; identify other package ecosystems as outside this task's coverage.

Inventory every selected maintained Cargo lockfile and the repository's existing
advisory policy, exemptions and prepared cargo-audit version. Check current
owner CI first. Freshness requires a current advisory observation even when the
source commit and lockfile have not changed.

For authorized execution with prepared tools, use the existing
[isolated RustSec preparation procedure](../docs/verification-helpers.md#isolated-rustsec-database-preparation).
An explicitly selected online run may read the official advisory feed into its
new evidence directory. Record its exact revision and use `cargo audit --no-fetch`
against that prepared database and the selected lockfile. An offline run records
the selected database revision and its freshness limitation. Stop on preparation
failure; do not let the audit silently fetch another database.

Report advisory ID, affected locked package/version, dependency path, supported
target/feature reachability, available correction and any reviewed exemption.
Distinguish an advisory match from proven exploitability, and audit completion
from current-feed coverage. Do not upgrade packages, edit exemptions or refresh
lockfiles as part of the check.

Return graph identity, advisory database revision/time, command/run evidence and
owning issues. This is a dependency advisory task, not a complete security audit.
