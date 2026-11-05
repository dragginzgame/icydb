use crate::{
    db::{
        executor::pipeline::runtime::RowView,
        query::plan::{FieldSlot, GroupedDistinctExecutionStrategy},
        schema::AcceptedFieldKind,
    },
    types::{Decimal, U256},
    value::Value,
};

use super::{
    GlobalDistinctAggregateKind, GlobalDistinctFieldAccumulator,
    GlobalDistinctFieldAggregateDispatcher,
};
use icydb_diagnostic_code::DiagnosticCode;

#[test]
fn global_distinct_extract_skips_null_but_rejects_missing_and_wrong_kind() {
    let slot = FieldSlot::from_test_accepted_kind(0, "qty", AcceptedFieldKind::Nat64);
    for strategy in [
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldCount {
            target_slot: slot.clone(),
        },
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldSum {
            target_slot: slot.clone(),
        },
        GroupedDistinctExecutionStrategy::GlobalDistinctFieldAvg { target_slot: slot },
    ] {
        let dispatcher = GlobalDistinctFieldAggregateDispatcher::resolve(&strategy).unwrap();
        assert!(
            dispatcher
                .extract(&RowView::new(vec![Some(Value::Null)]))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            dispatcher
                .extract(&RowView::new(vec![Some(Value::Nat64(2))]))
                .unwrap(),
            Some(&Value::Nat64(2))
        );
        for slots in [vec![None], vec![Some(Value::Text("invalid".into()))]] {
            let error = dispatcher.extract(&RowView::new(slots)).unwrap_err();
            assert_eq!(
                error.diagnostic().code(),
                DiagnosticCode::RuntimeInvariantViolation
            );
        }
    }
}

#[test]
fn global_distinct_accumulator_delegates_to_shared_value_reducers() {
    let one = Decimal::from_i64(1).expect("decimal one");
    let three = Decimal::from_i64(3).expect("decimal three");

    let mut count =
        GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Count.reducer_spec());
    count
        .apply_distinct_value(&Value::Decimal(one))
        .expect("count ingest");
    count
        .apply_distinct_value(&Value::Decimal(three))
        .expect("count ingest");
    assert_eq!(count.finalize().expect("count finalize"), Value::Nat64(2));

    let mut sum =
        GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Sum.reducer_spec());
    sum.apply_distinct_value(&Value::Decimal(one))
        .expect("sum ingest");
    sum.apply_distinct_value(&Value::Decimal(three))
        .expect("sum ingest");
    assert_eq!(
        sum.finalize().expect("sum finalize"),
        Value::Decimal(Decimal::from_i64(4).expect("decimal four")),
    );

    let mut u256_sum =
        GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Sum.reducer_spec());
    u256_sum
        .apply_distinct_value(&Value::U256(U256::from(2_u64)))
        .expect("U256 sum ingest");
    u256_sum
        .apply_distinct_value(&Value::U256(U256::from(3_u64)))
        .expect("U256 sum ingest");
    assert_eq!(
        u256_sum.finalize().expect("U256 sum finalize"),
        Value::U256(U256::from(5_u64)),
    );

    let mut avg =
        GlobalDistinctFieldAccumulator::new(GlobalDistinctAggregateKind::Avg.reducer_spec());
    avg.apply_distinct_value(&Value::Decimal(one))
        .expect("avg ingest");
    avg.apply_distinct_value(&Value::Decimal(three))
        .expect("avg ingest");
    assert_eq!(
        avg.finalize().expect("avg finalize"),
        Value::Decimal(Decimal::from_i64(2).expect("decimal two")),
    );
}
