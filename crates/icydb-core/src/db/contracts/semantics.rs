use crate::{db::group_key::GroupKey, value::Value};
use std::cmp::Ordering;

///
/// EqualitySemantics
///
/// Explicit equality contract used by deduplication/grouping paths.
/// Callers must not infer equality from ordering comparators.
///

pub(in crate::db) trait EqualitySemantics<T: ?Sized> {
    fn equals(left: &T, right: &T) -> bool;
}

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
/// GroupKeyEqualitySemantics
///
/// Canonical equality semantics for grouped/distinct materialized keys.
///

pub(in crate::db) struct GroupKeyEqualitySemantics;

impl EqualitySemantics<GroupKey> for GroupKeyEqualitySemantics {
    fn equals(left: &GroupKey, right: &GroupKey) -> bool {
        left == right
    }
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

/// Compare two group keys with canonical equality semantics.
#[must_use]
pub(in crate::db) fn canonical_group_key_equals(left: &GroupKey, right: &GroupKey) -> bool {
    GroupKeyEqualitySemantics::equals(left, right)
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
    use crate::{
        db::{
            contracts::{canonical_group_key_equals, canonical_value_compare},
            group_key::CanonicalKey,
        },
        types::Decimal,
        value::Value,
    };
    use std::cmp::Ordering;

    #[test]
    fn canonical_group_key_equality_matches_key_contract() {
        let left = Value::Decimal(Decimal::new(100, 2))
            .canonical_key()
            .expect("left key");
        let right = Value::Decimal(Decimal::new(1, 0))
            .canonical_key()
            .expect("right key");
        let other = Value::Decimal(Decimal::new(2, 0))
            .canonical_key()
            .expect("other key");

        assert!(canonical_group_key_equals(&left, &right));
        assert!(!canonical_group_key_equals(&left, &other));
    }

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
