//! Module: db::executor::aggregate::materialized_distinct::tests
//! Covers materialized-distinct aggregate behavior and deduplication rules.
//! Does not own: production aggregate behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::executor::{
        aggregate::materialized_distinct::insert_materialized_distinct_value, group::GroupKeySet,
    },
    value::Value,
};

#[test]
fn insert_materialized_distinct_value_dedups_repeated_values() {
    let mut distinct_values = GroupKeySet::new();

    assert!(
        insert_materialized_distinct_value(&mut distinct_values, &Value::Nat(7))
            .expect("first distinct insertion should succeed"),
        "first value should be inserted",
    );
    assert!(
        !insert_materialized_distinct_value(&mut distinct_values, &Value::Nat(7))
            .expect("duplicate distinct insertion should succeed"),
        "duplicate value should not be inserted twice",
    );
}
