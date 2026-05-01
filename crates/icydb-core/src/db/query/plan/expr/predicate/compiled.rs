//! Module: query::plan::expr::predicate::compiled
//! Responsibility: compiled predicate evaluation interface.
//! Does not own: predicate planning, slot loading, or scan mechanics.
//! Boundary: expression-layer predicate programs expose one boolean evaluation
//! contract so executors do not branch on predicate shape.

use crate::value::Value;

///
/// CompiledPredicate
///
/// CompiledPredicate is the common expression-layer interface for boolean
/// programs that have already crossed planning and lowering boundaries.
/// Executors provide loaded slot values and consume only the boolean result.
///

#[expect(
    dead_code,
    reason = "compiled predicate callers are being migrated to the unified interface"
)]
pub(in crate::db) trait CompiledPredicate {
    /// Evaluate this compiled predicate against already-loaded slot values.
    fn eval(&self, slots: &[Value]) -> bool;
}
