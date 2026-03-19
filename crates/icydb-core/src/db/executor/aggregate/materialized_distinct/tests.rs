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
        insert_materialized_distinct_value(&mut distinct_values, &Value::Uint(7))
            .expect("first distinct insertion should succeed"),
        "first value should be inserted",
    );
    assert!(
        !insert_materialized_distinct_value(&mut distinct_values, &Value::Uint(7))
            .expect("duplicate distinct insertion should succeed"),
        "duplicate value should not be inserted twice",
    );
}
