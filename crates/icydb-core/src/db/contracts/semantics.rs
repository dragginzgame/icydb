use crate::value::Value;
use std::cmp::Ordering;

///
/// OrderingSemantics
///
/// Explicit ordering contract used by sort/range/continuation paths.
/// Callers must not reuse this ordering surface for deduplication equality.
///

pub(in crate::db) trait OrderingSemantics<T: ?Sized> {
    fn compare(left: &T, right: &T) -> Ordering;
}

///
/// CanonicalValueOrderingSemantics
///
/// Canonical total ordering for `Value` sort/range/cursor boundaries.
///

pub(in crate::db) struct CanonicalValueOrderingSemantics;

impl OrderingSemantics<Value> for CanonicalValueOrderingSemantics {
    fn compare(left: &Value, right: &Value) -> Ordering {
        Value::canonical_cmp(left, right)
    }
}

/// Compare two values with canonical ordering semantics.
#[must_use]
pub(in crate::db) fn canonical_value_compare(left: &Value, right: &Value) -> Ordering {
    CanonicalValueOrderingSemantics::compare(left, right)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{db::contracts::canonical_value_compare, value::Value};
    use std::cmp::Ordering;

    #[test]
    fn canonical_value_ordering_uses_value_canonical_order() {
        assert_eq!(
            canonical_value_compare(&Value::Uint(7), &Value::Uint(8)),
            Ordering::Less
        );
        assert_eq!(
            canonical_value_compare(&Value::Text("x".to_string()), &Value::Text("x".to_string())),
            Ordering::Equal
        );
    }
}
