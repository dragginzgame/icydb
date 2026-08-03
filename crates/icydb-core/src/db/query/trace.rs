//! Module: query::trace
//! Responsibility: compact semantic-reuse attribution for query diagnostics.
//! Does not own: query semantics, plan hashing primitives, or executor routing policy.
//! Boundary: read-only reuse signal assembled at query/session boundaries.

///
/// TraceReuseEvent
///
/// Trace-surface semantic reuse result for one query planning attempt.
/// Reuse always refers to the shared prepared query plan, so the event owns
/// only the exact-match hit or miss outcome.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceReuseEvent {
    /// The shared prepared query plan matched the current semantic identity.
    Hit,
    /// No shared prepared query plan matched the current semantic identity.
    Miss,
}

impl TraceReuseEvent {
    /// Return true when this event represents a semantic-reuse hit.
    #[must_use]
    pub const fn is_hit(self) -> bool {
        matches!(self, Self::Hit)
    }
}
