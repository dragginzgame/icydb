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
