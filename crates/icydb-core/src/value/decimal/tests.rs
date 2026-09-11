//! Decimal conversion is shared, admitted before work, and byte-compatible.

use super::*;
use num_bigint::BigUint;
use std::convert::Infallible;

#[test]
fn conversion_admits_both_buffers_and_work_before_reading_the_iterator() {
    let visits = std::cell::Cell::new(0);
    let limbs = [1_u32, 2, 3]
        .into_iter()
        .inspect(|_| visits.set(visits.get() + 1));
    let result = chunks_from_limbs(limbs, |bytes, steps| {
        assert_eq!((bytes, steps), (28, 22));
        Err(())
    });
    assert_eq!(result, Err(()));
    assert_eq!(visits.get(), 0);
    for len in 0..5 {
        let capacity = (len * 32_usize).div_ceil(29);
        let chunks = chunks_from_limbs((0..len).map(|_| 1_u32), |bytes, steps| {
            assert_eq!(bytes, (len + capacity) as u64 * 4);
            assert_eq!(steps, (len * capacity + 2 * len + capacity) as u64);
            Ok::<_, Infallible>(())
        })
        .unwrap();
        assert_eq!(chunks.capacity(), capacity);
    }
}

#[test]
fn chunk_conversion_handles_zero_limbs_and_binary_radix_boundaries() {
    for (limbs, decimal) in [
        (vec![], "0"),
        (vec![0, 0], "0"),
        (vec![1, 0], "1"),
        (vec![u32::MAX], "4294967295"),
        (vec![0, 1], "4294967296"),
        (vec![u32::MAX, u32::MAX], "18446744073709551615"),
        (vec![0, 0, 1], "18446744073709551616"),
    ] {
        let chunks = chunks_from_limbs(limbs.into_iter(), |_, _| Ok::<_, Infallible>(())).unwrap();
        let mut out = Vec::new();
        visit_digits(&chunks, |digits| {
            out.extend_from_slice(digits);
            Ok::<_, Infallible>(())
        })
        .unwrap();
        assert_eq!(out, decimal.as_bytes());
        assert_eq!(digit_count(&chunks).unwrap(), out.len());
    }
}

#[test]
fn grouped_literals_preserve_candid_spelling_across_sign_and_radix_boundaries() {
    for digits in [
        "0",
        "1",
        "12",
        "123",
        "1234",
        "12345",
        "123456",
        "1000000000",
        "18446744073709551616",
        "340282366920938463463374607431768211455",
    ] {
        let unsigned: NatBig = digits.parse().unwrap();
        let mut out = String::new();
        write_unsigned_literal(&unsigned, &mut out).unwrap();
        assert_eq!(out, unsigned.to_string());
        for negative in [false, true] {
            let text = if negative {
                format!("-{digits}")
            } else {
                digits.to_string()
            };
            let signed: IntBig = text.parse().unwrap();
            out.clear();
            write_signed_literal(&signed, &mut out).unwrap();
            assert_eq!(out, signed.to_string());
        }
    }
}

#[test]
fn magnitude_capacity_covers_large_radix_expansion_without_growth() {
    for limbs in [1, 2, 13, 29, 30, 64, 128, 1024] {
        let value = NatBig::from_biguint(BigUint::new(vec![u32::MAX; limbs]));
        let mut allocations = Vec::new();
        let chunks = unsigned_chunks(&value, |bytes, _| {
            allocations.push(bytes);
            Ok::<_, Infallible>(())
        })
        .unwrap();
        let capacity = (limbs * 32).div_ceil(29);
        assert_eq!(allocations, [(limbs + capacity) as u64 * 4]);
        assert_eq!(chunks.capacity(), capacity);
        assert!(chunks.len() <= capacity);
        let mut actual = String::new();
        write_unsigned_literal(&value, &mut actual).unwrap();
        assert_eq!(actual, value.to_string());
    }
}

#[test]
fn large_conversion_rejects_upfront_without_visiting_limbs() {
    let visits = std::cell::Cell::new(0);
    let limbs = (0..4096).map(|_| {
        visits.set(visits.get() + 1);
        u32::MAX
    });
    let result = chunks_from_limbs(
        limbs,
        |_, steps| {
            if steps > 16_000_000 { Err(()) } else { Ok(()) }
        },
    );
    assert_eq!(result, Err(()));
    assert_eq!(visits.get(), 0);
}

#[test]
fn digit_emission_stops_before_later_chunks_on_failure() {
    let chunks = [1, 2, 3];
    let mut visits = 0;
    let result = visit_digits(&chunks, |_| {
        visits += 1;
        Err(())
    });
    assert_eq!(result, Err(()));
    assert_eq!(visits, 1);
}
