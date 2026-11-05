use super::{Newtype, primitive_supports_full_arithmetic, primitive_supports_neg};
use crate::prelude::*;
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

const ALL_PRIMITIVES: [Primitive; 25] = [
    Primitive::Account,
    Primitive::Blob,
    Primitive::Bool,
    Primitive::Date,
    Primitive::Decimal,
    Primitive::Duration,
    Primitive::Float32,
    Primitive::Float64,
    Primitive::Int8,
    Primitive::Int16,
    Primitive::Int32,
    Primitive::Int64,
    Primitive::Int128,
    Primitive::Nat8,
    Primitive::Nat16,
    Primitive::Nat32,
    Primitive::Nat64,
    Primitive::Nat128,
    Primitive::Principal,
    Primitive::Subaccount,
    Primitive::Text,
    Primitive::Timestamp,
    Primitive::U256,
    Primitive::Ulid,
    Primitive::Unit,
];

const ADDITIVE_TRAITS: [TraitKind; 4] = [
    TraitKind::Add,
    TraitKind::AddAssign,
    TraitKind::Sub,
    TraitKind::SubAssign,
];

const MULTIPLICATIVE_AND_FOLD_TRAITS: [TraitKind; 6] = [
    TraitKind::Div,
    TraitKind::DivAssign,
    TraitKind::Mul,
    TraitKind::MulAssign,
    TraitKind::Product,
    TraitKind::Sum,
];

#[test]
fn blob_newtype_defaults_reject_nonhex_before_generation() {
    for (text, valid) in [
        ("", true),
        ("DeAdBeEf", true),
        ("+1", false),
        ("raw text", false),
    ] {
        let mut node = newtype_with_primitive(Primitive::Blob);
        node.def = Def::new(
            syn::parse_quote!(
                pub struct TestBlob;
            ),
            CratePaths::test(),
        );
        node.item.unbounded = true;
        node.default = Some(Arg::String(syn::LitStr::new(
            text,
            proc_macro2::Span::call_site(),
        )));
        assert_eq!(node.validate().is_ok(), valid);
    }
}

#[test]
fn from_list_parses_nested_item_primitive() {
    let args = NestedMeta::parse_meta_list(quote!(item(prim = "Decimal")))
        .expect("newtype args should parse");

    let node = Newtype::from_list(&args).expect("newtype meta should lower");

    assert_eq!(node.item.primitive, Some(Primitive::Decimal));
}

fn newtype_with_primitive(primitive: Primitive) -> Newtype {
    Newtype {
        def: Def::default(),
        name: None,
        item: Item {
            primitive: Some(primitive),
            ..Default::default()
        },
        default: None,
        ty: Type::default(),
        traits: TraitBuilder::default(),
    }
}

#[test]
fn primitive_newtypes_use_rust_hash_and_order_capabilities() {
    for primitive in ALL_PRIMITIVES {
        let newtype = newtype_with_primitive(primitive);
        let traits = newtype.traits();

        for trait_kind in [TraitKind::Hash, TraitKind::Ord, TraitKind::PartialOrd] {
            assert!(
                traits.contains(&trait_kind),
                "{primitive:?} should generate {trait_kind:?}"
            );
        }
    }
}

#[test]
fn duration_newtype_generates_only_its_supported_additive_operators() {
    let traits = newtype_with_primitive(Primitive::Duration).traits();

    for trait_kind in ADDITIVE_TRAITS {
        assert!(traits.contains(&trait_kind));
    }
    for trait_kind in MULTIPLICATIVE_AND_FOLD_TRAITS {
        assert!(!traits.contains(&trait_kind));
    }
    assert!(!traits.contains(&TraitKind::Rem));
    assert!(!traits.contains(&TraitKind::RemAssign));
    assert!(!traits.contains(&TraitKind::Neg));
}

#[test]
fn remainder_assignment_matches_remainder_capabilities() {
    for primitive in ALL_PRIMITIVES {
        let traits = newtype_with_primitive(primitive).traits();
        assert_eq!(
            traits.contains(&TraitKind::Rem),
            traits.contains(&TraitKind::RemAssign),
            "{primitive:?} remainder capability should be internally consistent"
        );
    }
}

#[test]
fn product_matches_full_arithmetic_capabilities() {
    for primitive in ALL_PRIMITIVES {
        let traits = newtype_with_primitive(primitive).traits();
        assert_eq!(
            primitive_supports_full_arithmetic(primitive),
            traits.contains(&TraitKind::Product),
            "{primitive:?} product capability should match its arithmetic domain"
        );
    }
}

#[test]
fn neg_is_generated_only_for_signed_exact_numeric_primitives() {
    for primitive in ALL_PRIMITIVES {
        let traits = newtype_with_primitive(primitive).traits();
        assert_eq!(
            primitive_supports_neg(primitive),
            traits.contains(&TraitKind::Neg),
            "{primitive:?} signed-negation capability should be exact"
        );
    }
}
