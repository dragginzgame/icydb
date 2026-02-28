//! Module: predicate::row_policy
//! Responsibility: missing-row handling policy shared by query/executor layers.
//! Does not own: predicate expression semantics.
//! Boundary: consumed by query plan builders and executor flows.

///
/// MissingRowPolicy
///
/// Missing-row handling policy for query execution.
///
/// This is a domain-level contract shared by query planning and executor
/// runtime behavior.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingRowPolicy {
    /// Missing rows are ignored (no error).
    Ignore,

    /// Missing rows are treated as corruption.
    Error,
}
