use super::{enum_adapter_tokens, record_adapter_tokens};
use crate::node::{Def, Enum, Record};
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

#[test]
fn enum_adapter_uses_model_owned_source_bound_context() {
    let args = NestedMeta::parse_meta_list(quote!(
        name = "ChoiceSource",
        variant(name = "Empty"),
        variant(name = "Count", value(item(prim = "Nat64")))
    ))
    .expect("enum adapter arguments should parse");
    let mut node = Enum::from_list(&args).expect("enum adapter node should lower");
    node.def = Def::new(
        syn::parse_quote!(
            pub struct Choice;
        ),
        crate::crate_path::CratePaths::test(),
    );

    let tokens = enum_adapter_tokens(&node, &crate::crate_path::CratePaths::test()).to_string();

    for expected in [
        "impl :: icydb_model :: TypedNamedType for Choice",
        "const SOURCE_KEY : & 'static str = \"ChoiceSource\"",
        "input_enum (\"ChoiceSource\" , \"Empty\"",
        "input_enum (\"ChoiceSource\" , \"Count\"",
        "TypedEnumDescriptor",
        "type_source_key : \"ChoiceSource\"",
        "variants : & [\"Empty\" , \"Count\"]",
        "context . output_enum (& DESCRIPTOR , value)",
        "match selected . ordinal",
        "TypedInputValue for Choice",
        "TypedOutputValue for Choice",
    ] {
        assert!(
            tokens.contains(expected),
            "expected generated enum adapter contract `{expected}` in: {tokens}",
        );
    }
    assert_eq!(tokens.matches("output_enum").count(), 1);
    assert!(!tokens.contains(":: icydb ::"));
}

#[test]
fn record_adapter_uses_source_bound_members_and_context() {
    let args = NestedMeta::parse_meta_list(quote!(
        name = "ProfileSource",
        fields(
            field(name = "label", value(item(prim = "Text", unbounded))),
            field(name = "count", value(item(prim = "Nat64")))
        )
    ))
    .expect("record adapter arguments should parse");
    let mut node = Record::from_list(&args).expect("record adapter node should lower");
    node.def = Def::new(
        syn::parse_quote!(
            pub struct Profile;
        ),
        crate::crate_path::CratePaths::test(),
    );

    let tokens = record_adapter_tokens(&node, &crate::crate_path::CratePaths::test()).to_string();

    for expected in [
        "impl :: icydb_model :: TypedNamedType for Profile",
        "input_record (\"ProfileSource\"",
        "output_record (\"ProfileSource\"",
        "TypedInputValue for Profile",
        "TypedOutputValue for Profile",
    ] {
        assert!(
            tokens.contains(expected),
            "expected generated record adapter contract `{expected}` in: {tokens}",
        );
    }
    assert!(!tokens.contains(":: icydb ::"));
}
