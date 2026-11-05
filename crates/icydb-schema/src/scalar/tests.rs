use super::{ALL_SCALAR_KINDS, ScalarKind};
use std::collections::HashSet;

const EXPECTED_SCALAR_KINDS: [ScalarKind; 20] = [
    ScalarKind::Account,
    ScalarKind::Blob,
    ScalarKind::Bool,
    ScalarKind::Date,
    ScalarKind::Decimal,
    ScalarKind::Duration,
    ScalarKind::Enum,
    ScalarKind::Float32,
    ScalarKind::Float64,
    ScalarKind::Int,
    ScalarKind::Int128,
    ScalarKind::Principal,
    ScalarKind::Subaccount,
    ScalarKind::Text,
    ScalarKind::Timestamp,
    ScalarKind::Nat,
    ScalarKind::Nat128,
    ScalarKind::U256,
    ScalarKind::Ulid,
    ScalarKind::Unit,
];

#[test]
fn all_scalar_kinds_has_expected_length_and_order() {
    assert_eq!(ALL_SCALAR_KINDS, EXPECTED_SCALAR_KINDS);
}

#[test]
fn all_scalar_kinds_is_unique() {
    let unique = ALL_SCALAR_KINDS.into_iter().collect::<HashSet<_>>();

    assert_eq!(unique.len(), ALL_SCALAR_KINDS.len());
}

#[test]
fn documentation_primary_keys_match_compiled_capabilities() {
    // Read the checkout at test time so published library builds do not
    // depend on repository documentation. Compare data, not source syntax.
    let guide = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/guides/schema-authoring.md"
    ))
    .expect("repository schema authoring guide");
    let section = guide
        .split_once("<!-- icydb-primary-key-primitives:start -->")
        .expect("primary-key data start")
        .1
        .split_once("<!-- icydb-primary-key-primitives:end -->")
        .expect("primary-key data end")
        .0;
    let documented = section
        .split('`')
        .skip(1)
        .step_by(2)
        .collect::<HashSet<_>>();
    macro_rules! admitted_primitives {
        (@entries $(($primitive:ident, $kind:ident)),* $(,)?) => {
            [$( (stringify!($primitive), ScalarKind::$kind) ),*]
                .into_iter()
                .filter(|(_, kind)| kind.is_primary_key_component_encodable())
                .map(|(name, _)| name)
                .collect::<HashSet<_>>()
        };
    }
    let expected = authoring_primitive_registry!(admitted_primitives);
    assert_eq!(documented, expected);
}

#[test]
fn all_scalar_kind_variants_are_audited() {
    for kind in EXPECTED_SCALAR_KINDS {
        assert_variant_is_known(kind);
    }
}

#[test]
fn all_scalar_kinds_metadata_is_available() {
    for kind in ALL_SCALAR_KINDS {
        let metadata = kind.metadata();

        assert_eq!(kind.coercion_family(), metadata.family());
        assert_eq!(kind.is_numeric_value(), metadata.is_numeric_value());
        assert_eq!(
            kind.supports_numeric_coercion(),
            metadata.supports_numeric_coercion(),
        );
        assert_eq!(kind.supports_arithmetic(), metadata.supports_arithmetic());
        assert_eq!(kind.supports_equality(), metadata.supports_equality());
        assert_eq!(kind.supports_ordering(), metadata.supports_ordering());
        assert_eq!(kind.is_keyable(), metadata.is_keyable());
        assert_eq!(
            kind.is_primary_key_component_encodable(),
            metadata.is_primary_key_component_encodable(),
        );
    }
}

#[test]
fn enum_capabilities_are_equality_only() {
    assert!(ScalarKind::Enum.supports_equality());
    assert!(!ScalarKind::Enum.supports_ordering());
    assert!(!ScalarKind::Enum.is_keyable());
}

#[test]
fn unit_capabilities_include_trivial_total_order() {
    let metadata = ScalarKind::Unit.metadata();

    assert!(metadata.supports_equality());
    assert!(metadata.supports_ordering());
    assert!(metadata.is_keyable());
    assert!(metadata.is_primary_key_component_encodable());
    assert!(!metadata.supports_numeric_coercion());
    assert!(!metadata.supports_arithmetic());
}

fn assert_variant_is_known(kind: ScalarKind) {
    match kind {
        ScalarKind::Account
        | ScalarKind::Blob
        | ScalarKind::Bool
        | ScalarKind::Date
        | ScalarKind::Decimal
        | ScalarKind::Duration
        | ScalarKind::Enum
        | ScalarKind::Float32
        | ScalarKind::Float64
        | ScalarKind::Int
        | ScalarKind::Int128
        | ScalarKind::Principal
        | ScalarKind::Subaccount
        | ScalarKind::Text
        | ScalarKind::Timestamp
        | ScalarKind::Nat
        | ScalarKind::Nat128
        | ScalarKind::U256
        | ScalarKind::Ulid
        | ScalarKind::Unit => {}
    }
}
