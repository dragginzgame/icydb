//! Module: query::plan::expr::path
//! Responsibility: planner-owned compiled nested field-path contracts.
//! Does not own: runtime value-storage traversal or projection materialization.
//! Boundary: freezes normalized path segments before executor row readers use them.

///
/// CompiledPath
///
/// CompiledPath is the planner-owned nested path program used by projection
/// and predicate expression execution.
/// The string form is retained for labels and compile-time transfer into
/// `CompiledExpr` field-path leaves.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CompiledPath {
    /// Owned nested map-key sequence traversed below the resolved root slot.
    segments: Vec<String>,
}

impl CompiledPath {
    /// Build a compiled path from already-normalized nested map segments.
    #[must_use]
    pub(in crate::db) const fn new(segments: Vec<String>) -> Self {
        Self { segments }
    }

    /// Borrow the nested map-key sequence used by executor value-storage walkers.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.segments.as_slice()
    }
}
