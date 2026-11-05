use super::U256;
use crate::{Decimal, NumericValue};

#[test]
fn wire_uses_fixed_width_bytes_and_rejects_wrong_lengths() {
    let bytes = crate::wire::encode_one(U256::MAX).unwrap();
    assert_eq!(bytes, [&[32][..], &[0xff; 32][..]].concat());
    assert_eq!(crate::wire::decode_one::<U256>(&bytes).unwrap(), U256::MAX);
    for len in [0, 31, 33] {
        assert!(
            crate::wire::decode_one::<U256>(
                &crate::wire::encode_one(serde_bytes::Bytes::new(&vec![0xff; len])).unwrap()
            )
            .is_err()
        );
    }
}

#[test]
fn fixed_bytes_and_decimal_are_exact() {
    let value = "57896044618658097711785492504343953926634992332820282019728792003956564819968"
        .parse::<U256>()
        .expect("2^255 should parse");
    assert_eq!(value.to_be_bytes()[0], 0x80);
    assert_eq!(U256::from_be_bytes(value.to_be_bytes()), value);
    assert_eq!(
        value.to_string(),
        "57896044618658097711785492504343953926634992332820282019728792003956564819968"
    );
    assert_eq!(U256::ZERO.to_string(), "0");
    assert_eq!(U256::ONE.to_string(), "1");
    assert_eq!(U256::MAX.to_string(), u256_max_decimal());
}

#[test]
fn checked_arithmetic_enforces_the_u256_domain() {
    let two = U256::from(2_u64);
    let three = U256::from(3_u64);

    assert_eq!(two.checked_add(three), Some(U256::from(5_u64)));
    assert_eq!(three.checked_sub(two), Some(U256::ONE));
    assert_eq!(two.checked_mul(three), Some(U256::from(6_u64)));
    assert_eq!(U256::from(7_u64).checked_div(two), Some(three));
    assert_eq!(U256::from(7_u64).checked_rem(two), Some(U256::ONE));
    assert_eq!(U256::MAX.checked_add(U256::ONE), None);
    assert_eq!(U256::ZERO.checked_sub(U256::ONE), None);
    assert_eq!(U256::MAX.checked_mul(two), None);
    assert_eq!(U256::ONE.checked_div(U256::ZERO), None);
    assert_eq!(U256::ONE.checked_rem(U256::ZERO), None);
}

#[test]
fn generic_numeric_conversion_is_fallible_without_widening_the_u256_domain() {
    let value = U256::from(u128::try_from(i128::MAX).expect("i128::MAX should fit u128"));
    assert_eq!(
        value.try_to_decimal().and_then(U256::try_from_decimal),
        Some(value),
    );
    assert_eq!(U256::MAX.try_to_decimal(), None);
    let negative_one = Decimal::from_i128(-1).expect("-1 should be a valid Decimal");
    assert_eq!(U256::try_from_decimal(negative_one), None);
}

fn u256_max_decimal() -> &'static str {
    "115792089237316195423570985008687907853269984665640564039457584007913129639935"
}
