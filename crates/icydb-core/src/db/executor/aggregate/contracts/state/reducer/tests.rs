use crate::{
    db::executor::aggregate::{AggregateKind, contracts::state::GroupedAggregateReducerState},
    types::{Decimal, U256},
    value::Value,
};

#[test]
fn expression_extrema_preserve_owned_borrowed_inputs_and_variant_refusals() {
    use crate::error::{ErrorClass, ErrorOrigin};
    use std::borrow::Cow;

    for kind in [AggregateKind::Min, AggregateKind::Max] {
        for owned in [false, true] {
            let mut reducer = GroupedAggregateReducerState::for_kind(kind);
            for value in [
                Value::Null,
                Value::Text("middle".into()),
                Value::Text("a".into()),
                Value::Text("z".into()),
            ] {
                let input = if owned {
                    Cow::Owned(value.clone())
                } else {
                    Cow::Borrowed(&value)
                };
                match kind {
                    AggregateKind::Min => reducer.ingest_min_value(input),
                    _ => reducer.ingest_max_value(input),
                }
                .unwrap();
            }
            let expected = if kind == AggregateKind::Min { "a" } else { "z" };
            assert_eq!(reducer.into_value().unwrap(), Value::Text(expected.into()));
        }
    }

    for kind in [
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Exists,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        for owned in [false, true] {
            for minimum in [false, true] {
                if (minimum && kind == AggregateKind::Min)
                    || (!minimum && kind == AggregateKind::Max)
                {
                    continue;
                }
                let mut reducer = GroupedAggregateReducerState::for_kind(kind);
                let value = Value::Text("invalid route".into());
                let input = if owned {
                    Cow::Owned(value.clone())
                } else {
                    Cow::Borrowed(&value)
                };
                let error = if minimum {
                    reducer.ingest_min_value(input)
                } else {
                    reducer.ingest_max_value(input)
                }
                .unwrap_err();
                assert_eq!(error.class(), ErrorClass::InvariantViolation);
                assert_eq!(error.origin(), ErrorOrigin::Query);
            }
        }
    }
}

#[test]
fn grouped_count_sum_and_avg_delegate_to_shared_value_reducers() {
    let one = Decimal::from_i64(1).expect("decimal one");
    let three = Decimal::from_i64(3).expect("decimal three");

    let mut count = GroupedAggregateReducerState::for_kind(AggregateKind::Count);
    count.increment_count().expect("count ingest");
    count.increment_count().expect("count ingest");
    assert_eq!(count.into_value().expect("count finalize"), Value::Nat64(2));

    let mut sum = GroupedAggregateReducerState::for_kind(AggregateKind::Sum);
    sum.add_sum_value(&Value::Decimal(one)).expect("sum ingest");
    sum.add_sum_value(&Value::Decimal(three))
        .expect("sum ingest");
    assert_eq!(
        sum.into_value().expect("sum finalize"),
        Value::Decimal(Decimal::from_i64(4).expect("decimal four")),
    );

    let mut u256_sum = GroupedAggregateReducerState::for_kind(AggregateKind::Sum);
    u256_sum
        .add_sum_value(&Value::U256(U256::from(2_u64)))
        .expect("U256 sum ingest");
    u256_sum
        .add_sum_value(&Value::U256(U256::from(3_u64)))
        .expect("U256 sum ingest");
    assert_eq!(
        u256_sum.into_value().expect("U256 sum finalize"),
        Value::U256(U256::from(5_u64)),
    );

    let mut avg = GroupedAggregateReducerState::for_kind(AggregateKind::Avg);
    avg.add_average_value(&Value::Decimal(one))
        .expect("avg ingest");
    avg.add_average_value(&Value::Decimal(three))
        .expect("avg ingest");
    assert_eq!(
        avg.into_value().expect("avg finalize"),
        Value::Decimal(Decimal::from_i64(2).expect("decimal two")),
    );
}
