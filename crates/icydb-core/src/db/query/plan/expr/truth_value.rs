//! Module: query::plan::expr::truth_value
//! Responsibility: evaluated-value truth admission for expression boolean
//! contexts.
//! Does not own: expression canonicalization, predicate compilation, type
//! inference, or projection materialization.
//! Boundary: consumes already-evaluated `Value` results and answers whether
//! they pass TRUE-only SQL admission.
//!
//! Truth semantics contract:
//! - `Value::Bool(true)` is the only value that is admitted as TRUE.
//! - `Value::Bool(false)` is FALSE and does not pass TRUE-only admission.
//! - `Value::Null` represents SQL UNKNOWN for boolean admission and does not
//!   pass TRUE-only admission.
//! - IcyDB does not expose a separate UNKNOWN value variant; expression
//!   evaluation carries UNKNOWN as `Value::Null` until this boundary.
//! - Any other value is invalid in an already-typed boolean context and must be
//!   reported by the caller through the supplied error constructor.

use crate::value::Value;

/// Admit one borrowed evaluated boolean-context value through the shared
/// TRUE-only policy without forcing callers that already hold row-local values
/// to clone on the successful path.
pub(in crate::db) fn admit_true_only_boolean_value<E>(
    value: &Value,
    invalid: impl FnOnce(&Value) -> E,
) -> Result<bool, E> {
    match value {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(invalid(other)),
    }
}

/// Collapse one evaluated boolean-context value through the shared TRUE-only
/// admission boundary used by WHERE-style row filtering, grouped HAVING, CASE
/// branch selection, and aggregate FILTER semantics.
pub(in crate::db) fn collapse_true_only_boolean_admission<E>(
    value: Value,
    invalid: impl FnOnce(Box<Value>) -> E,
) -> Result<bool, E> {
    admit_true_only_boolean_value(&value, |found| invalid(Box::new(found.clone())))
}
