use super::*;
use sha2::Digest;

#[test]
fn nested_access_hash_propagates_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    let access = AccessPlan::Union(vec![AccessPlan::Intersection(vec![AccessPlan::by_key(
        Value::Nat64(7),
    )])]);
    with_test_hash_override(Err(test_hash_budget_error), || {
        let error = hash_access_plan(&mut Sha256::new(), &access).unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

#[test]
fn access_key_hashes_preserve_payload_and_framing() {
    let first = Value::Text("start".repeat(1024));
    let last = Value::Text("stop".repeat(1024));
    let nested = Value::List(vec![Value::Map(vec![(
        Value::Text("payload".to_string()),
        Value::NatBig(crate::types::NatBig::from_biguint(
            num_bigint::BigUint::from(1_u8) << 4096_usize,
        )),
    )])]);
    // Raw access fixtures qualify hash framing, not key-type admission. Cover
    // payload-bearing values so the hash boundary cannot assume Copy keys.
    for (access, tag, values, framed_list) in [
        (
            AccessPlan::by_key(first.clone()),
            ACCESS_TAG_BY_KEY,
            vec![first.clone()],
            false,
        ),
        (
            AccessPlan::by_keys(vec![first.clone(), nested.clone(), first.clone()]),
            ACCESS_TAG_BY_KEYS,
            vec![first.clone(), nested, first.clone()],
            true,
        ),
        (
            AccessPlan::key_range(first.clone(), last.clone()),
            ACCESS_TAG_KEY_RANGE,
            vec![first, last],
            false,
        ),
    ] {
        let snapshot = access.clone();
        let mut expected = Sha256::new();
        write_tag(&mut expected, tag);
        if framed_list {
            write_u32(&mut expected, u32::try_from(values.len()).unwrap());
        }
        for value in &values {
            write_value(&mut expected, value).unwrap();
        }
        let expected = expected.finalize();
        for _ in 0..2 {
            let mut planned = Sha256::new();
            hash_access_plan(&mut planned, &access).unwrap();
            assert_eq!(planned.finalize(), expected);
        }
        assert_eq!(access, snapshot);
    }
}

#[test]
fn access_projection_hash_preserves_canonical_postorder() {
    let access = AccessPlan::Union(vec![
        AccessPlan::by_keys(vec![Value::Nat64(7), Value::Nat64(2)]),
        AccessPlan::Intersection(vec![AccessPlan::by_keys(vec![]), AccessPlan::Union(vec![])]),
    ]);
    // Pin the existing stream explicitly, independently of either walker.
    let mut expected = Sha256::new();
    write_tag(&mut expected, ACCESS_TAG_BY_KEYS);
    write_u32(&mut expected, 2);
    write_value(&mut expected, &Value::Nat64(7)).unwrap();
    write_value(&mut expected, &Value::Nat64(2)).unwrap();
    write_tag(&mut expected, ACCESS_TAG_BY_KEYS);
    write_u32(&mut expected, 0);
    write_tag(&mut expected, ACCESS_TAG_UNION);
    write_u32(&mut expected, 0);
    write_tag(&mut expected, ACCESS_TAG_INTERSECTION);
    write_u32(&mut expected, 2);
    write_tag(&mut expected, ACCESS_TAG_UNION);
    write_u32(&mut expected, 2);
    let mut planned = Sha256::new();
    hash_access_plan(&mut planned, &access).unwrap();
    assert_eq!(planned.finalize(), expected.clone().finalize());
}
