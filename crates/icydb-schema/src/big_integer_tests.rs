//! Exact size checks use the maintained encoder as their independent oracle.

use crate::{IntBig, NatBig};
use num_bigint::{BigInt, BigUint};

#[test]
fn signed_leb128_lengths_match_dense_small_values() {
    for integer in -20_000_i64..=20_000 {
        let value = IntBig::from(integer);
        assert_eq!(
            value.leb128_len(),
            value.to_leb128().len() as u64,
            "{integer}"
        );
    }
}

#[test]
fn signed_leb128_lengths_match_wide_sign_and_limb_boundaries() {
    for bit in 0_usize..=2048 {
        let power = BigInt::from(1_u8) << bit;
        for delta in [-1_i32, 0, 1] {
            let magnitude = &power + BigInt::from(delta);
            for integer in [magnitude.clone(), -magnitude] {
                let value = IntBig::from_bigint(integer);
                assert_eq!(
                    value.leb128_len(),
                    value.to_leb128().len() as u64,
                    "bit={bit}, delta={delta}, value={value}"
                );
            }
        }
    }
}

#[test]
fn unsigned_leb128_lengths_match_zero_and_wide_boundaries() {
    let zero = NatBig::default();
    assert_eq!(zero.leb128_len(), 1);
    assert_eq!(zero.leb128_len(), zero.to_leb128().len() as u64);
    for bit in 0_usize..=2048 {
        let power = BigUint::from(1_u8) << bit;
        for integer in [&power - 1_u8, power.clone(), &power + 1_u8] {
            let value = NatBig::from_biguint(integer);
            assert_eq!(
                value.leb128_len(),
                value.to_leb128().len() as u64,
                "bit={bit}, value={value}"
            );
        }
    }
}
