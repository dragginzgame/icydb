use super::ValueReducerState;
use crate::{types::U256, value::Value};
use icydb_diagnostic_code::DiagnosticCode;

#[test]
fn aggregate_coercion_distinguishes_numeric_domain_from_invalid_input() {
    for (value, expected) in [
        (
            Value::Nat128(u128::MAX),
            DiagnosticCode::QueryNumericNotRepresentable,
        ),
        (
            Value::Text("not numeric".into()),
            DiagnosticCode::RuntimeInvariantViolation,
        ),
    ] {
        for owned in [false, true] {
            for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
                let error = if owned {
                    reducer.ingest_owned(value.clone())
                } else {
                    reducer.ingest(&value)
                }
                .expect_err("invalid reducer operand");
                assert_eq!(error.diagnostic().code(), expected);
            }
        }
    }
    let error = ValueReducerState::avg()
        .ingest(&Value::U256(U256::ONE))
        .expect_err("AVG does not admit U256");
    assert_eq!(
        error.diagnostic().code(),
        DiagnosticCode::RuntimeInvariantViolation
    );
}

#[test]
fn aggregate_decimal_bounds_and_nulls_preserve_current_results() {
    use crate::types::Decimal;

    for owned in [false, true] {
        for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
            reducer.ingest(&Value::Null).unwrap();
            let maximum = Value::Int128(i128::MAX);
            if owned {
                reducer.ingest_owned(maximum).unwrap();
            } else {
                reducer.ingest(&maximum).unwrap();
            }
            let error = reducer
                .ingest(&Value::Int128(1))
                .expect_err("bounded running sum");
            assert_eq!(
                error.diagnostic().code(),
                DiagnosticCode::QueryNumericOverflow
            );
        }
    }
    for mut reducer in [ValueReducerState::sum(), ValueReducerState::avg()] {
        reducer.ingest(&Value::Null).unwrap();
        assert_eq!(reducer.into_final_value().unwrap(), Value::Null);
    }
    let mut average = ValueReducerState::avg();
    average.ingest(&Value::Nat64(1)).unwrap();
    average.ingest_owned(Value::Nat64(2)).unwrap();
    assert_eq!(
        average.into_final_value().unwrap(),
        Value::Decimal(Decimal::new(15, 1))
    );
}

#[test]
fn u256_sum_stays_inline_and_returns_u256() {
    let mut reducer = ValueReducerState::sum();

    reducer
        .ingest_sum_value(&Value::U256(U256::from(2_u64)))
        .expect("first U256 SUM value should ingest");
    reducer
        .ingest_sum_value(&Value::U256(U256::from(3_u64)))
        .expect("second U256 SUM value should ingest");

    assert_eq!(
        reducer
            .into_final_value()
            .expect("U256 SUM should finalize"),
        Value::U256(U256::from(5_u64)),
    );
    assert_eq!(std::mem::size_of::<ValueReducerState>(), 80);
    assert_eq!(std::mem::align_of::<ValueReducerState>(), 16);
}

#[test]
fn u256_sum_reports_typed_overflow() {
    let mut reducer = ValueReducerState::sum();
    reducer
        .ingest_sum_value(&Value::U256(U256::MAX))
        .expect("first U256 SUM value should ingest");
    let err = reducer
        .ingest_sum_value(&Value::U256(U256::ONE))
        .expect_err("U256 SUM should reject overflow");

    assert_eq!(
        err.diagnostic().code(),
        DiagnosticCode::QueryNumericOverflow
    );
}
