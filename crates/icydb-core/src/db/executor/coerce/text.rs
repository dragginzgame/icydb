//! Text coercion layer
//!
//! Centralises all text-based comparison logic so FilterEvaluator and
//! coerce_basic() don’t need to know about TextMode, folding, or the
//! Value::text_* helpers.

use crate::{
    db::primitives::filter::Cmp,
    value::{TextMode, Value},
};

/// Entry point used by coerce_basic().
///
/// Returns `None` if *either* side is not `Value::Text(_)`.
#[must_use]
pub fn coerce_text(left: &Value, right: &Value, cmp: Cmp) -> Option<bool> {
    // Fast reject for non-text values (ensures purity of this layer).
    if !left.is_text() || !right.is_text() {
        return None;
    }

    match cmp {
        // --- strict CS (case-sensitive) ---
        Cmp::Eq => eq_cs(left, right),
        Cmp::Ne => eq_cs(left, right).map(|b| !b),

        Cmp::Contains => contains_cs(left, right),
        Cmp::StartsWith => starts_with_cs(left, right),
        Cmp::EndsWith => ends_with_cs(left, right),

        // --- CI (case-insensitive) ---
        Cmp::EqCi => eq_ci(left, right),
        Cmp::NeCi => eq_ci(left, right).map(|b| !b),

        Cmp::ContainsCi => contains_ci(left, right),
        Cmp::StartsWithCi => starts_with_ci(left, right),
        Cmp::EndsWithCi => ends_with_ci(left, right),

        _ => None,
    }
}

/// ----------------------
/// Raw helpers (Value::*)
/// ----------------------

fn eq_cs(a: &Value, b: &Value) -> Option<bool> {
    a.text_eq(b, TextMode::Cs)
}

fn contains_cs(a: &Value, b: &Value) -> Option<bool> {
    a.text_contains(b, TextMode::Cs)
}

fn starts_with_cs(a: &Value, b: &Value) -> Option<bool> {
    a.text_starts_with(b, TextMode::Cs)
}

fn ends_with_cs(a: &Value, b: &Value) -> Option<bool> {
    a.text_ends_with(b, TextMode::Cs)
}

fn eq_ci(a: &Value, b: &Value) -> Option<bool> {
    a.text_eq(b, TextMode::Ci)
}

fn contains_ci(a: &Value, b: &Value) -> Option<bool> {
    a.text_contains(b, TextMode::Ci)
}

fn starts_with_ci(a: &Value, b: &Value) -> Option<bool> {
    a.text_starts_with(b, TextMode::Ci)
}

fn ends_with_ci(a: &Value, b: &Value) -> Option<bool> {
    a.text_ends_with(b, TextMode::Ci)
}
