use super::*;
use crate::{
    types::Decimal,
    value::{Value, with_test_hash_override},
};

#[test]
fn incremental_hash_and_vector_capacity_stays_inside_retained_envelope() {
    let mut map = StableHashMap::<usize>::with_hasher(StableHashBuildHasher);
    let mut values = Vec::<usize>::new();

    for entry in 1..=1_024_usize {
        try_reserve_hash_entry(&mut map).expect("hash capacity reservation");
        map.insert(u64::try_from(entry).expect("test key"), entry);
        try_reserve_vec_elements(&mut values, 1).expect("vector capacity reservation");
        values.push(entry);

        let admitted_slots = entry.saturating_mul(RETAINED_CAPACITY_SLOTS_PER_ENTRY);
        assert!(map.capacity() <= admitted_slots);
        assert!(values.capacity() <= admitted_slots);
    }
}

#[test]
fn stable_hash_uses_digest_prefix_contract() {
    let digest = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xF0, 0x0A,
        0x0B,
    ];
    assert_eq!(
        stable_hash_from_digest(digest),
        0x1122_3344_5566_7788,
        "stable hash must use the canonical leading 64 bits of the value digest",
    );
    with_test_hash_override(Ok(digest), || {
        assert_eq!(
            stable_hash_value(&Value::Null).expect("stable hash"),
            0x1122_3344_5566_7788,
            "grouped hashing must project the canonical digest prefix",
        );
    });
}

#[test]
fn stable_hash_is_deterministic_for_same_value() {
    let value = Value::Decimal(Decimal::new(12300, 4));
    let left = stable_hash_value(&value).expect("stable hash");
    let right = stable_hash_value(&value).expect("stable hash");
    assert_eq!(left, right);
}

#[test]
fn stable_hash_respects_canonical_map_order() {
    let left = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Nat64(9)),
        (Value::Text("a".to_string()), Value::Nat64(1)),
    ]);
    let right = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Nat64(1)),
        (Value::Text("z".to_string()), Value::Nat64(9)),
    ]);
    assert_eq!(
        stable_hash_value(&left).expect("stable hash"),
        stable_hash_value(&right).expect("stable hash"),
        "stable hash must not depend on non-canonical map insertion order",
    );
}
