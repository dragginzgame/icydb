use super::{Field, FieldGeneration, FieldWriteManagement, Value};
use crate::node::{Arg, ArgNumber, Item};
use darling::{FromMeta, ast::NestedMeta};
use icydb_schema::types::Primitive;
use quote::format_ident;
use quote::quote;
use std::str::FromStr;
use syn::parse_quote;

fn relation_field(ident: &str, many: bool) -> Field {
    Field {
        ident: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many,
            item: Item {
                relation: Some(syn::parse_quote!(User)),
                primitive: Some(Primitive::Ulid),
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

#[test]
fn relation_one_suffix_is_required() {
    let field = relation_field("user", false);
    let err = field
        .validate()
        .expect_err("one relation field without _id suffix must fail");
    assert!(
        err.to_string().contains("must end with '_id'"),
        "unexpected validation error: {err}",
    );
}

#[test]
fn relation_many_suffix_is_required() {
    let field = relation_field("users", true);
    let err = field
        .validate()
        .expect_err("many relation field without _ids suffix must fail");
    assert!(
        err.to_string().contains("must end with '_ids'"),
        "unexpected validation error: {err}",
    );
}

#[test]
fn relation_suffix_validation_accepts_canonical_idents() {
    relation_field("user_id", false)
        .validate()
        .expect("one relation field with _id suffix should pass");
    relation_field("user_ids", true)
        .validate()
        .expect("many relation field with _ids suffix should pass");
}

#[test]
fn default_match_detects_primitive_default_constructors() {
    let field = Field {
        ident: format_ident!("name"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
                ..Item::default()
            },
        },
        default: Some(Arg::FuncPath(parse_quote!(String::new))),
        generated: None,
        write_management: None,
    };

    assert!(
        field.default_matches_implicit_default(),
        "String::new should not force a manual Default impl",
    );
}

#[test]
fn default_match_detects_custom_type_default_constructors() {
    let field = Field {
        ident: format_ident!("profile"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                is: Some(parse_quote!(crate::Profile)),
                ..Item::default()
            },
        },
        default: Some(Arg::FuncPath(parse_quote!(crate::Profile::default))),
        generated: None,
        write_management: None,
    };

    assert!(
        field.default_matches_implicit_default(),
        "custom type default() should not force a manual Default impl",
    );
}

#[test]
fn default_match_rejects_custom_non_default_constructors() {
    let field = Field {
        ident: format_ident!("id"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Ulid),
                ..Item::default()
            },
        },
        default: Some(Arg::FuncPath(parse_quote!(Ulid::generate))),
        generated: None,
        write_management: None,
    };

    assert!(
        !field.default_matches_implicit_default(),
        "custom constructors must still force an explicit Default impl",
    );
}

#[test]
fn database_default_expr_encodes_explicit_function_schema_default() {
    let field = Field {
        ident: format_ident!("name"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
                ..Item::default()
            },
        },
        default: Some(Arg::FuncPath(parse_quote!(String::new))),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("explicit String::new schema default should validate");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("explicit String::new schema default should encode");
    let tokens = field.database_default_expr().to_string();

    assert_eq!(payload, vec![0xFF, 0x01]);
    assert!(
        tokens.contains("FieldDatabaseDefault :: EncodedSlotPayload"),
        "explicit schema default should become a database default: {tokens}",
    );
}

#[test]
fn database_default_expr_encodes_supported_literal_schema_default() {
    let field = Field {
        ident: format_ident!("name"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("guest"))),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("literal text default should also be a valid database default");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("literal schema default should encode as database default");

    assert_eq!(
        payload,
        vec![0xFF, 0x01, b'g', b'u', b'e', b's', b't'],
        "literal schema default should encode as the database default",
    );
}

#[test]
fn database_default_expr_encodes_explicit_text_default_payload() {
    let field = Field {
        ident: format_ident!("nickname"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Text),
                max_len: Some(8),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("unknown"))),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("text default within max_len should validate");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("text default should encode");

    assert_eq!(
        payload,
        vec![0xFF, 0x01, b'u', b'n', b'k', b'n', b'o', b'w', b'n'],
        "default should encode the canonical persisted scalar slot payload",
    );
}

#[test]
fn database_default_accepts_optional_literal_default_payload() {
    let field = Field {
        ident: format_ident!("anti_aliasing"),
        value: Value {
            opt: true,
            many: false,
            item: Item {
                primitive: Some(Primitive::Bool),
                ..Item::default()
            },
        },
        default: Some(Arg::Bool(false)),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("optional literal default should be a valid database default");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("optional literal default should encode as database default");

    assert_eq!(
        payload,
        vec![0xFF, 0x01, 0],
        "optional literal default should encode the non-null field payload",
    );
}

#[test]
fn database_default_encodes_decimal_literal_with_declared_scale() {
    let field = Field {
        ident: format_ident!("ratio"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Decimal),
                scale: Some(2),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Nat8(0))),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("decimal literal default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("decimal literal default should encode");
    let expected = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Decimal::from_i128_with_scale(0, 2),
        icydb_core::model::FieldKind::Decimal { scale: 2 },
        "ratio",
    )
    .expect("expected decimal payload should encode");

    assert_eq!(
        payload, expected,
        "decimal literal default should use the by-kind decimal payload",
    );
}

#[test]
fn database_default_encodes_explicit_decimal_text_payload() {
    let field = Field {
        ident: format_ident!("project"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Decimal),
                scale: Some(2),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("1.25"))),
        generated: None,
        write_management: None,
    };

    field
        .validate()
        .expect("decimal text default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("decimal text default should encode");
    let expected = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Decimal::from_i128_with_scale(125, 2),
        icydb_core::model::FieldKind::Decimal { scale: 2 },
        "project",
    )
    .expect("expected decimal payload should encode");

    assert_eq!(
        payload, expected,
        "decimal text default should use the by-kind decimal payload",
    );
}

#[test]
fn database_default_rejects_invalid_decimal_text_payload() {
    let field = Field {
        ident: format_ident!("ratio"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Decimal),
                scale: Some(2),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("not-a-decimal"))),
        generated: None,
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("invalid decimal text default should fail");

    assert!(
        err.to_string()
            .contains("default for primitive Decimal is invalid"),
        "unexpected decimal default validation error: {err}",
    );
}

#[test]
fn database_default_encodes_int128_literal_with_by_kind_codec() {
    let field = Field {
        ident: format_ident!("signed_balance"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Int128),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Int128(i128::MIN))),
        generated: None,
        write_management: None,
    };

    field.validate().expect("Int128 default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("Int128 default should encode");
    let expected = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &i128::MIN,
        icydb_core::model::FieldKind::Int128,
        "signed_balance",
    )
    .expect("expected Int128 payload should encode");

    assert_eq!(
        payload, expected,
        "Int128 default should use the by-kind Int128 payload",
    );
}

#[test]
fn database_default_encodes_nat128_literal_with_by_kind_codec() {
    let field = Field {
        ident: format_ident!("supply"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Nat128),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Nat128(u128::MAX))),
        generated: None,
        write_management: None,
    };

    field.validate().expect("Nat128 default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("Nat128 default should encode");
    let expected = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &u128::MAX,
        icydb_core::model::FieldKind::Nat128,
        "supply",
    )
    .expect("expected Nat128 payload should encode");

    assert_eq!(
        payload, expected,
        "Nat128 default should use the by-kind Nat128 payload",
    );
}

#[test]
fn database_default_rejects_negative_nat128_literal() {
    let field = Field {
        ident: format_ident!("supply"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Nat128),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Int8(-1))),
        generated: None,
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("negative Nat128 default should fail");

    assert!(
        err.to_string()
            .contains("default for primitive Nat128 requires an unsigned integer literal"),
        "unexpected Nat128 default validation error: {err}",
    );
}

#[test]
fn database_default_accepts_non_identity_primitive_default_constructors() {
    let cases = [
        (Primitive::Blob, Arg::FuncPath(parse_quote!(Blob::default))),
        (Primitive::Bool, Arg::FuncPath(parse_quote!(bool::default))),
        (Primitive::Date, Arg::FuncPath(parse_quote!(Date::default))),
        (
            Primitive::Decimal,
            Arg::FuncPath(parse_quote!(Decimal::default)),
        ),
        (
            Primitive::Duration,
            Arg::FuncPath(parse_quote!(Duration::default)),
        ),
        (
            Primitive::Float32,
            Arg::FuncPath(parse_quote!(Float32::default)),
        ),
        (
            Primitive::Float64,
            Arg::FuncPath(parse_quote!(Float64::default)),
        ),
        (
            Primitive::IntBig,
            Arg::FuncPath(parse_quote!(IntBig::default)),
        ),
        (Primitive::Int8, Arg::FuncPath(parse_quote!(i8::default))),
        (Primitive::Int16, Arg::FuncPath(parse_quote!(i16::default))),
        (Primitive::Int32, Arg::FuncPath(parse_quote!(i32::default))),
        (Primitive::Int64, Arg::FuncPath(parse_quote!(i64::default))),
        (
            Primitive::Int128,
            Arg::FuncPath(parse_quote!(i128::default)),
        ),
        (
            Primitive::NatBig,
            Arg::FuncPath(parse_quote!(NatBig::default)),
        ),
        (Primitive::Nat8, Arg::FuncPath(parse_quote!(u8::default))),
        (Primitive::Nat16, Arg::FuncPath(parse_quote!(u16::default))),
        (Primitive::Nat32, Arg::FuncPath(parse_quote!(u32::default))),
        (Primitive::Nat64, Arg::FuncPath(parse_quote!(u64::default))),
        (
            Primitive::Nat128,
            Arg::FuncPath(parse_quote!(u128::default)),
        ),
        (
            Primitive::Text,
            Arg::FuncPath(parse_quote!(String::default)),
        ),
        (
            Primitive::Timestamp,
            Arg::FuncPath(parse_quote!(Timestamp::default)),
        ),
        (Primitive::Unit, Arg::ConstPath(parse_quote!(Unit))),
    ];

    for (primitive, default) in cases {
        let field = default_test_field(primitive, default);

        field
            .validate()
            .unwrap_or_else(|err| panic!("{primitive:?} default should validate: {err}"));
        super::database_default_slot_payload_bytes(
            field.default.as_ref().expect("default should exist"),
            &field.value,
        )
        .unwrap_or_else(|err| panic!("{primitive:?} default should encode: {err}"));
    }
}

#[test]
fn database_default_rejects_identity_like_default_constructors() {
    let cases = [
        (
            Primitive::Account,
            Arg::FuncPath(parse_quote!(Account::default)),
            "Account::default",
        ),
        (
            Primitive::Principal,
            Arg::FuncPath(parse_quote!(Principal::default)),
            "Principal::default",
        ),
        (
            Primitive::Subaccount,
            Arg::FuncPath(parse_quote!(Subaccount::default)),
            "Subaccount::default",
        ),
        (
            Primitive::Ulid,
            Arg::FuncPath(parse_quote!(Ulid::default)),
            "Ulid::default",
        ),
    ];

    for (primitive, default, constructor) in cases {
        let field = default_test_field(primitive, default);
        let err = field
            .validate()
            .expect_err("identity-like constructor should reject as schema default");
        let message = err.to_string();

        assert!(
            message.contains("identity-like") && message.contains("explicit persisted literal"),
            "unexpected identity default validation error for {constructor}: {message}",
        );
    }
}

#[test]
fn database_default_encodes_int_and_nat_big_payloads_with_by_kind_codecs() {
    let int_literal = "-170141183460469231731687303715884105729";
    let int_field = default_test_field(
        Primitive::IntBig,
        Arg::String(syn::LitStr::new(
            int_literal,
            proc_macro2::Span::call_site(),
        )),
    );
    let int_payload = super::database_default_slot_payload_bytes(
        int_field.default.as_ref().expect("default should exist"),
        &int_field.value,
    )
    .expect("Int default should encode");
    let expected_int = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::IntBig::from_str(int_literal).expect("expected IntBig should parse"),
        icydb_core::model::FieldKind::IntBig {
            max_bytes: icydb_core::model::DEFAULT_BIG_INT_MAX_BYTES,
        },
        "int_big",
    )
    .expect("expected Int payload should encode");
    assert_eq!(int_payload, expected_int);

    let nat_literal = "340282366920938463463374607431768211456";
    let nat_field = default_test_field(
        Primitive::NatBig,
        Arg::String(syn::LitStr::new(
            nat_literal,
            proc_macro2::Span::call_site(),
        )),
    );
    let nat_payload = super::database_default_slot_payload_bytes(
        nat_field.default.as_ref().expect("default should exist"),
        &nat_field.value,
    )
    .expect("Nat default should encode");
    let expected_nat = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::NatBig::from_str(nat_literal).expect("expected NatBig should parse"),
        icydb_core::model::FieldKind::NatBig {
            max_bytes: icydb_core::model::DEFAULT_BIG_INT_MAX_BYTES,
        },
        "nat_big",
    )
    .expect("expected Nat payload should encode");
    assert_eq!(nat_payload, expected_nat);
}

#[test]
fn database_default_encodes_identity_blob_and_unit_payloads_with_by_kind_codecs() {
    let blob_field = default_test_field(Primitive::Blob, Arg::String(parse_quote!("abc")));
    let blob_payload = super::database_default_slot_payload_bytes(
        blob_field.default.as_ref().expect("default should exist"),
        &blob_field.value,
    )
    .expect("Blob default should encode");
    let expected_blob = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Blob::from(&b"abc"[..]),
        icydb_core::model::FieldKind::Blob { max_len: Some(64) },
        "blob",
    )
    .expect("expected Blob payload should encode");
    assert_eq!(blob_payload, expected_blob);

    let principal_field =
        default_test_field(Primitive::Principal, Arg::String(parse_quote!("aaaaa-aa")));
    let principal_payload = super::database_default_slot_payload_bytes(
        principal_field
            .default
            .as_ref()
            .expect("default should exist"),
        &principal_field.value,
    )
    .expect("Principal default should encode");
    let expected_principal = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Principal::from_str("aaaaa-aa")
            .expect("expected Principal should parse"),
        icydb_core::model::FieldKind::Principal,
        "principal",
    )
    .expect("expected Principal payload should encode");
    assert_eq!(principal_payload, expected_principal);

    let subaccount_literal = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let subaccount_field = default_test_field(
        Primitive::Subaccount,
        Arg::String(syn::LitStr::new(
            subaccount_literal,
            proc_macro2::Span::call_site(),
        )),
    );
    let subaccount_payload = super::database_default_slot_payload_bytes(
        subaccount_field
            .default
            .as_ref()
            .expect("default should exist"),
        &subaccount_field.value,
    )
    .expect("Subaccount default should encode");
    let expected_subaccount = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Subaccount::from_array([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ]),
        icydb_core::model::FieldKind::Subaccount,
        "subaccount",
    )
    .expect("expected Subaccount payload should encode");
    assert_eq!(subaccount_payload, expected_subaccount);

    let ulid_field = default_test_field(
        Primitive::Ulid,
        Arg::String(parse_quote!("00000000000000000000000000")),
    );
    let ulid_payload = super::database_default_slot_payload_bytes(
        ulid_field.default.as_ref().expect("default should exist"),
        &ulid_field.value,
    )
    .expect("Ulid default should encode");
    let expected_ulid = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Ulid::nil(),
        icydb_core::model::FieldKind::Ulid,
        "ulid",
    )
    .expect("expected Ulid payload should encode");
    assert_eq!(ulid_payload, expected_ulid);

    let unit_field = default_test_field(Primitive::Unit, Arg::ConstPath(parse_quote!(Unit)));
    let unit_payload = super::database_default_slot_payload_bytes(
        unit_field.default.as_ref().expect("default should exist"),
        &unit_field.value,
    )
    .expect("Unit default should encode");
    let expected_unit = icydb_core::__macro::encode_persisted_slot_payload_by_kind(
        &icydb_core::types::Unit,
        icydb_core::model::FieldKind::Unit,
        "unit",
    )
    .expect("expected Unit payload should encode");
    assert_eq!(unit_payload, expected_unit);
}

#[test]
fn database_default_encodes_date_text_with_scalar_codec() {
    let field = Field {
        ident: format_ident!("launch_date"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Date),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("2025-01-02"))),
        generated: None,
        write_management: None,
    };

    field.validate().expect("Date default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("Date default should encode");
    let expected = icydb_core::__macro::encode_persisted_scalar_slot_payload(
        &icydb_core::types::Date::parse_flexible("2025-01-02").expect("expected date should parse"),
        "launch_date",
    )
    .expect("expected Date payload should encode");

    assert_eq!(
        payload, expected,
        "Date default should use the scalar Date payload",
    );
}

#[test]
fn database_default_encodes_duration_text_with_scalar_codec() {
    let field = Field {
        ident: format_ident!("cooldown"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Duration),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("5s"))),
        generated: None,
        write_management: None,
    };

    field.validate().expect("Duration default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("Duration default should encode");
    let expected = icydb_core::__macro::encode_persisted_scalar_slot_payload(
        &icydb_core::types::Duration::from_secs(5),
        "cooldown",
    )
    .expect("expected Duration payload should encode");

    assert_eq!(
        payload, expected,
        "Duration default should use the scalar Duration payload",
    );
}

#[test]
fn database_default_encodes_timestamp_text_with_scalar_codec() {
    let field = Field {
        ident: format_ident!("published_at"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Timestamp),
                ..Item::default()
            },
        },
        default: Some(Arg::String(parse_quote!("2025-01-01T12:30:00.123Z"))),
        generated: None,
        write_management: None,
    };

    field.validate().expect("Timestamp default should be valid");
    let payload = super::database_default_slot_payload_bytes(
        field.default.as_ref().expect("default should exist"),
        &field.value,
    )
    .expect("Timestamp default should encode");
    let expected = icydb_core::__macro::encode_persisted_scalar_slot_payload(
        &icydb_core::types::Timestamp::from_millis(1_735_734_600_123),
        "published_at",
    )
    .expect("expected Timestamp payload should encode");

    assert_eq!(
        payload, expected,
        "Timestamp default should use the scalar Timestamp payload",
    );
}

#[test]
fn database_default_rejects_negative_duration_literal() {
    let field = Field {
        ident: format_ident!("cooldown"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Duration),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Int8(-1))),
        generated: None,
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("negative Duration default should fail");

    assert!(
        err.to_string()
            .contains("default for primitive Duration requires an unsigned millisecond literal"),
        "unexpected Duration default validation error: {err}",
    );
}

fn default_test_field(primitive: Primitive, default: Arg) -> Field {
    Field {
        ident: format_ident!("defaulted"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(primitive),
                max_len: matches!(primitive, Primitive::Blob | Primitive::Text).then_some(64),
                scale: (primitive == Primitive::Decimal).then_some(2),
                ..Item::default()
            },
        },
        default: Some(default),
        generated: None,
        write_management: None,
    }
}

#[test]
fn database_default_rejects_out_of_range_numeric_default() {
    let field = Field {
        ident: format_ident!("rank"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Nat8),
                ..Item::default()
            },
        },
        default: Some(Arg::Number(ArgNumber::Nat16(300))),
        generated: None,
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("default outside the field primitive range should fail");

    assert!(
        err.to_string().contains("outside primitive Nat8 range"),
        "unexpected default range validation error: {err}",
    );
}

#[test]
fn generated_clause_accepts_single_value_primitive_ulid_fields() {
    let field = Field {
        ident: format_ident!("id"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Ulid),
                ..Item::default()
            },
        },
        default: None,
        generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
            Ulid::generate
        )))),
        write_management: None,
    };

    field
        .validate()
        .expect("generated(insert = ...) should be admitted for primitive Ulid fields");
}

#[test]
fn generated_clause_parser_accepts_arbitrary_quoted_generator_paths() {
    let generated = FieldGeneration::from_list(&[NestedMeta::Meta(syn::Meta::NameValue(
        parse_quote!(insert = "Id::generate"),
    ))])
    .expect("generated(insert = \"...\") should parse any quoted generator path");

    let FieldGeneration::Insert(Arg::FuncPath(path)) = generated else {
        panic!("generated(insert = \"...\") should lower to a function path");
    };

    assert_eq!(
        path.segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>(),
        vec!["Id".to_string(), "generate".to_string()],
        "generated(insert = \"...\") should preserve the quoted path segments",
    );
}

#[test]
fn from_list_parses_generated_insert_clause() {
    let args = NestedMeta::parse_meta_list(quote!(
        ident = "id",
        value(item(prim = "Ulid")),
        generated(insert = "Ulid::generate")
    ))
    .expect("field args should parse");

    let field = Field::from_list(&args).expect("field meta should lower");

    assert!(
        matches!(field.generated, Some(FieldGeneration::Insert(_))),
        "generated(insert = ...) should parse into FieldGeneration::Insert",
    );
    assert_eq!(field.value.item.primitive, Some(Primitive::Ulid));
}

#[test]
fn generated_clause_accepts_single_value_primitive_timestamp_fields() {
    let field = Field {
        ident: format_ident!("created_on_insert"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Timestamp),
                ..Item::default()
            },
        },
        default: None,
        generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
            Timestamp::now
        )))),
        write_management: None,
    };

    field
        .validate()
        .expect("generated(insert = ...) should be admitted for primitive Timestamp fields");
}

#[test]
fn generated_clause_rejects_mismatched_field_and_generator_contracts() {
    let field = Field {
        ident: format_ident!("name"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
                ..Item::default()
            },
        },
        default: None,
        generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
            Ulid::generate
        )))),
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("generated(insert = ...) should stay fail-closed on mismatched fields");
    assert!(
        err.to_string()
            .contains("generated(insert = \"Ulid::generate\") requires a primitive Ulid field"),
        "unexpected generated(insert = ...) validation error: {err}",
    );
}

#[test]
fn generated_clause_rejects_non_ulid_generators() {
    let field = Field {
        ident: format_ident!("id"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Ulid),
                ..Item::default()
            },
        },
        default: None,
        generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
            Id::generate
        )))),
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("generated(insert = ...) should stay fail-closed on non-Ulid generators");
    assert!(
        err.to_string().contains(
            "generated(insert = ...) currently supports only Ulid::generate or Timestamp::now"
        ),
        "unexpected generated(insert = ...) validation error: {err}",
    );
}

#[test]
fn generated_clause_rejects_default_contracts() {
    let field = Field {
        ident: format_ident!("created_on_insert"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Timestamp),
                ..Item::default()
            },
        },
        default: Some(Arg::ConstPath(parse_quote!(Timestamp::EPOCH))),
        generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
            Timestamp::now
        )))),
        write_management: None,
    };

    let err = field
        .validate()
        .expect_err("generated(insert = ...) should reject conflicting default contracts");
    assert!(
        err.to_string().contains(
            "generated(insert = ...) cannot be combined with default = ...; default is a database/schema default"
        ),
        "unexpected generated/default conflict validation error: {err}",
    );
}

#[test]
fn created_and_updated_fields_emit_write_management_metadata() {
    assert_eq!(
        Field::created_at().write_management,
        Some(FieldWriteManagement::CreatedAt),
        "created_at helper should mark the field as insert-managed",
    );
    assert_eq!(
        Field::updated_at().write_management,
        Some(FieldWriteManagement::UpdatedAt),
        "updated_at helper should mark the field as update-managed",
    );
}
