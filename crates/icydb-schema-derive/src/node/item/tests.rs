//! Module: node::item::tests
//! Responsibility: regression coverage for this module.
//! Does not own: production behavior.
//! Boundary: test-only contracts.

use super::Item;
use darling::{FromMeta, ast::NestedMeta};
use icydb_schema::types::Primitive;
use quote::quote;

#[test]
fn validate_accepts_scale_for_decimal_primitive() {
    let item = Item {
        primitive: Some(Primitive::Decimal),
        scale: Some(8),
        ..Item::default()
    };

    assert!(item.validate().is_ok());
}

#[test]
fn validate_rejects_scale_for_non_decimal_primitive() {
    let item = Item {
        primitive: Some(Primitive::Nat64),
        scale: Some(8),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_scale_without_declared_primitive() {
    let item = Item {
        scale: Some(8),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_decimal_without_scale() {
    let item = Item {
        primitive: Some(Primitive::Decimal),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_accepts_max_len_for_text_primitive() {
    let item = Item {
        primitive: Some(Primitive::Text),
        max_len: Some(32),
        ..Item::default()
    };

    assert!(item.validate().is_ok());
}

#[test]
fn validate_accepts_max_len_for_blob_primitive() {
    let item = Item {
        primitive: Some(Primitive::Blob),
        max_len: Some(32),
        ..Item::default()
    };

    assert!(item.validate().is_ok());
}

#[test]
fn validate_accepts_max_bytes_for_big_integer_primitives() {
    for primitive in [Primitive::IntBig, Primitive::NatBig] {
        let item = Item {
            primitive: Some(primitive),
            max_bytes: Some(512),
            ..Item::default()
        };

        assert!(item.validate().is_ok());
    }
}

#[test]
fn validate_accepts_explicit_unbounded_for_text_primitive() {
    let item = Item {
        primitive: Some(Primitive::Text),
        unbounded: true,
        ..Item::default()
    };

    assert!(item.validate().is_ok());
}

#[test]
fn validate_accepts_explicit_unbounded_for_blob_primitive() {
    let item = Item {
        primitive: Some(Primitive::Blob),
        unbounded: true,
        ..Item::default()
    };

    assert!(item.validate().is_ok());
}

#[test]
fn from_list_accepts_unbounded_flag_directive() {
    let args = NestedMeta::parse_meta_list(quote!(prim = "Text", unbounded))
        .expect("item args should parse");

    let item = Item::from_list(&args).expect("item meta should lower");

    assert!(item.unbounded);
    assert!(item.validate().is_ok());
}

#[test]
fn from_list_accepts_unbounded_name_value_directive() {
    let args = NestedMeta::parse_meta_list(quote!(prim = "Blob", unbounded = true))
        .expect("item args should parse");

    let item = Item::from_list(&args).expect("item meta should lower");

    assert!(item.unbounded);
    assert!(item.validate().is_ok());
}

#[test]
fn validate_rejects_max_len_for_unbounded_primitive() {
    let item = Item {
        primitive: Some(Primitive::Nat64),
        max_len: Some(32),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_max_bytes_for_non_big_integer_primitive() {
    let item = Item {
        primitive: Some(Primitive::Nat64),
        max_bytes: Some(512),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_zero_max_bytes() {
    let item = Item {
        primitive: Some(Primitive::NatBig),
        max_bytes: Some(0),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_implicit_unbounded_text() {
    let item = Item {
        primitive: Some(Primitive::Text),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_implicit_unbounded_blob() {
    let item = Item {
        primitive: Some(Primitive::Blob),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_unbounded_with_max_len() {
    let item = Item {
        primitive: Some(Primitive::Text),
        max_len: Some(32),
        unbounded: true,
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_max_len_without_declared_primitive() {
    let item = Item {
        max_len: Some(32),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}

#[test]
fn validate_rejects_zero_max_len() {
    let item = Item {
        primitive: Some(Primitive::Text),
        max_len: Some(0),
        ..Item::default()
    };

    assert!(item.validate().is_err());
}
