//! Executor-focused helpers for `AccessPath`; must not plan or validate queries.

use super::types::AccessPath;

#[must_use]
/// Whether the access path is a full scan.
pub const fn is_full_scan(path: &AccessPath) -> bool {
    matches!(path, AccessPath::FullScan)
}

#[must_use]
/// Whether the access path targets an index.
pub const fn is_index_prefix(path: &AccessPath) -> bool {
    matches!(path, AccessPath::IndexPrefix { .. })
}
