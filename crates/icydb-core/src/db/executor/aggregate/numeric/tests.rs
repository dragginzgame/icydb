use crate::{
    db::{executor::aggregate::numeric::add_numeric_decimal, numeric::average_decimal_terms},
    types::Decimal,
};

#[test]
fn aggregate_numeric_addition_uses_shared_saturating_decimal_semantics() {
    let left = Decimal::from_i128_with_scale(i128::MAX, 0);
    let right = Decimal::from_i128_with_scale(1, 0);

    let result = add_numeric_decimal(left, right);

    assert_eq!(result, Decimal::from_i128_with_scale(i128::MAX, 0));
}

#[test]
fn aggregate_numeric_avg_division_uses_shared_rounding_semantics() {
    let sum = Decimal::from_num(-1_i64).expect("sum decimal");

    let result = average_decimal_terms(sum, 6_u64).expect("decimal avg should produce one value");

    assert_eq!(
        result,
        Decimal::from_i128_with_scale(-166_666_666_666_666_667, 18)
    );
}
