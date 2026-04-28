//! Module: db::executor::aggregate::numeric::tests
//! Covers numeric aggregate execution behavior and numeric fold invariants.
//! Does not own: production aggregate behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use crate::{
    db::{
        executor::aggregate::numeric::add_numeric_decimal,
        numeric::{NumericEvalError, average_decimal_terms_checked},
    },
    types::Decimal,
};

#[test]
fn aggregate_numeric_addition_reports_checked_overflow() {
    let left = Decimal::from_i128_with_scale(i128::MAX, 0);
    let right = Decimal::from_i128_with_scale(1, 0);

    let err = add_numeric_decimal(left, right).expect_err("overflow should fail checked addition");

    assert_eq!(err.message(), NumericEvalError::Overflow.to_string());
}

#[test]
fn aggregate_numeric_avg_division_uses_shared_rounding_semantics() {
    let sum = Decimal::from_num(-1_i64).expect("sum decimal");

    let result =
        average_decimal_terms_checked(sum, 6_u64).expect("decimal avg should produce one value");

    assert_eq!(
        result,
        Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
    );
}
