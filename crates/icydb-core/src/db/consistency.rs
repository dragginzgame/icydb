///
/// ReadConsistency
///
/// Missing-row handling policy for query execution.
///
/// This is a domain-level contract shared by query planning and executor
/// runtime behavior.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Missing rows are ignored (no error).
    MissingOk,

    /// Missing rows are treated as corruption.
    Strict,
}
