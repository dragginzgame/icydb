//! Module: value::hash::tests
//! Covers the frozen seed, version, and digest vectors for canonical value
//! hashing.

use super::*;
use crate::{
    types::{Decimal, Float32 as F32, Float64 as F64},
    value::{Value, ValueEnum},
};

#[test]
fn hash_contract_seed_and_version_are_frozen() {
    assert_eq!(VALUE_HASH_SEED, 0);
    assert_eq!(VALUE_HASH_VERSION, 1);
}

#[test]
fn hash_digest_contract_vectors_are_frozen_for_upgrade_stability() {
    let vectors = vec![
        (
            "null",
            Value::Null,
            0x07d3_310a_0679_d482_1974_aae7_68bf_e723,
        ),
        (
            "empty_list",
            Value::List(Vec::new()),
            0xc961_6022_4099_6188_601a_2e1d_a3d4_577d,
        ),
        (
            "empty_map",
            Value::Map(Vec::new()),
            0xfe53_d0af_a864_e41f_7540_c693_50b9_b8d7,
        ),
        (
            "uint_42",
            Value::Uint(42),
            0x8c99_03a0_7f2c_731c_2e7a_9cd6_52cb_010f,
        ),
        (
            "int_neg7",
            Value::Int(-7),
            0x7470_6cc5_9093_df80_0d3b_e517_da6b_0104,
        ),
        (
            "text_alpha",
            Value::Text("alpha".to_string()),
            0x6ec7_96a5_45c2_ad82_58ff_9d4a_4ea8_1c2b,
        ),
        (
            "decimal_1",
            Value::Decimal(Decimal::new(10, 1)),
            0x7d42_1e3f_fffc_9100_0be6_fa20_26b6_0b82,
        ),
        (
            "map_a1_z9",
            Value::Map(vec![
                (Value::Text("a".to_string()), Value::Uint(1)),
                (Value::Text("z".to_string()), Value::Uint(9)),
            ]),
            0xea0e_28c9_f878_6d85_c240_88c8_f0d7_9d81,
        ),
        (
            "nested_list_map",
            Value::List(vec![
                Value::Map(vec![
                    (Value::Text("z".to_string()), Value::Uint(9)),
                    (Value::Text("a".to_string()), Value::Uint(1)),
                ]),
                Value::Decimal(Decimal::new(25, 0)),
            ]),
            0x65e9_3e05_85b6_72cc_de03_6d05_eaa9_8c57,
        ),
    ];
    for (label, value, expected_digest) in vectors {
        let actual_digest = hash_value(&value).expect("hash value");
        assert_eq!(
            u128::from_be_bytes(actual_digest),
            expected_digest,
            "value-hash digest vector drift for {label}; canonical encoding/seed/version contract changed",
        );
    }
}

fn v_f64(x: f64) -> Value {
    Value::Float64(F64::try_new(x).expect("finite f64"))
}
fn v_f32(x: f32) -> Value {
    Value::Float32(F32::try_new(x).expect("finite f32"))
}
fn v_i(x: i64) -> Value {
    Value::Int(x)
}
fn v_txt(s: &str) -> Value {
    Value::Text(s.to_string())
}

#[test]
fn hash_is_deterministic_for_int() {
    let v = Value::Int(42);
    let a = hash_value(&v).expect("hash value");
    let b = hash_value(&v).expect("hash value");
    assert_eq!(a, b, "hash should be deterministic for same value");
}

#[test]
fn different_variants_produce_different_hashes() {
    let a = hash_value(&Value::Int(5)).expect("hash value");
    let b = hash_value(&Value::Uint(5)).expect("hash value");
    assert_ne!(
        a, b,
        "Int(5) and Uint(5) must hash differently (different tag)"
    );
}

#[test]
fn enum_hash_tracks_path_presence() {
    let strict = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
    let loose = Value::Enum(ValueEnum::new("A", None));
    assert_ne!(
        hash_value(&strict).expect("hash value"),
        hash_value(&loose).expect("hash value"),
        "Enum hashes must differ when path is present vs absent"
    );
}

#[test]
fn enum_hash_includes_payload() {
    let base = ValueEnum::new("A", Some("MyEnum"));
    let with_one = Value::Enum(base.clone().with_payload(Value::Uint(1)));
    let with_two = Value::Enum(base.with_payload(Value::Uint(2)));

    assert_ne!(
        hash_value(&with_one).expect("hash value"),
        hash_value(&with_two).expect("hash value"),
        "Enum payload must influence hash/fingerprint"
    );
}

#[test]
fn float32_and_float64_hash_differ() {
    let a = hash_value(&v_f32(1.0)).expect("hash value");
    let b = hash_value(&v_f64(1.0)).expect("hash value");
    assert_ne!(
        a, b,
        "Float32 and Float64 must hash differently (different tag)"
    );
}

#[test]
fn decimal_hash_normalizes_equivalent_values() {
    let one = Value::Decimal(Decimal::new(1, 0));
    let one_point_zero = Value::Decimal(Decimal::new(10, 1));

    assert_eq!(
        one, one_point_zero,
        "decimal values should be semantically equal"
    );
    assert_eq!(
        hash_value(&one).expect("hash value"),
        hash_value(&one_point_zero).expect("hash value"),
        "equivalent decimals must hash to the same fingerprint"
    );
}

#[test]
fn text_is_length_and_content_sensitive() {
    let a = hash_value(&v_txt("foo")).expect("hash value");
    let b = hash_value(&v_txt("bar")).expect("hash value");
    assert_ne!(a, b, "different strings should hash differently");

    let c = hash_value(&v_txt("foo")).expect("hash value");
    assert_eq!(a, c, "same string should hash the same");
}

#[test]
fn list_hash_is_order_sensitive() {
    let l1 = Value::from_slice(&[v_i(1), v_i(2)]);
    let l2 = Value::from_slice(&[v_i(2), v_i(1)]);
    assert_ne!(
        hash_value(&l1).expect("hash value"),
        hash_value(&l2).expect("hash value"),
        "list order should affect hash"
    );
}

#[test]
fn list_hash_is_length_sensitive() {
    let l1 = Value::from_slice(&[v_i(1)]);
    let l2 = Value::from_slice(&[v_i(1), v_i(1)]);
    assert_ne!(
        hash_value(&l1).expect("hash value"),
        hash_value(&l2).expect("hash value"),
        "list length should affect hash"
    );
}

#[test]
fn list_blob_boundaries_are_length_framed() {
    let left = Value::List(vec![
        Value::Blob(vec![0x10, 0xFF, 0x02, 0x11]),
        Value::Blob(vec![0x12]),
    ]);
    let right = Value::List(vec![
        Value::Blob(vec![0x10]),
        Value::Blob(vec![0x11, 0xFF, 0x02, 0x12]),
    ]);

    assert_ne!(
        hash_value(&left).expect("hash value"),
        hash_value(&right).expect("hash value"),
        "blob boundaries must be length-framed to avoid collisions"
    );
}

#[test]
fn map_hash_is_order_independent_for_non_canonical_construction_order() {
    let left = Value::Map(vec![
        (Value::Text("z".to_string()), Value::Uint(9)),
        (Value::Text("a".to_string()), Value::Uint(1)),
    ]);
    let right = Value::Map(vec![
        (Value::Text("a".to_string()), Value::Uint(1)),
        (Value::Text("z".to_string()), Value::Uint(9)),
    ]);

    assert_eq!(
        hash_value(&left).expect("hash value"),
        hash_value(&right).expect("hash value"),
        "map hash must be deterministic regardless of insertion order"
    );
}
