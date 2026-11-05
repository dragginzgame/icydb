//! Test-only helpers for the parent module.

use super::*;

/// Evaluate one compiled expression through a canonical raw-row slot reader.
pub(in crate::db::executor) fn eval_compiled_expr_with_required_slot_reader_cow<'a>(
    expr: &'a CompiledExpr,
    slots: &'a dyn CanonicalSlotReader,
    record_slot: &'a mut dyn FnMut(usize),
) -> Result<Cow<'a, Value>, InternalError> {
    CanonicalSlotExprReader::new(std::slice::from_ref(expr), slots, record_slot, true)
        .evaluate_projection(expr)
        .map(Cow::Owned)
}
