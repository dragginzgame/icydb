//! Module: node::field::tests
//! Responsibility: current application-field declaration regression coverage.
//! Does not own: accepted default encoding or runtime write admission.
//! Boundary: macro input validation and generated application expressions.

use super::{Field, FieldGeneration, FieldWriteManagement, Value};
use crate::{
    authoring_types::Primitive,
    node::{Arg, Item},
};
use darling::{FromMeta, ast::NestedMeta};
use quote::{format_ident, quote};
use syn::parse_quote;

fn field(ident: &str, primitive: Primitive) -> Field {
    Field {
        name: format_ident!("{ident}"),
        value: Value {
            item: Item {
                primitive: Some(primitive),
                unbounded: matches!(primitive, Primitive::Blob | Primitive::Text),
                ..Item::default()
            },
            ..Value::default()
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

#[test]
fn relation_fields_require_canonical_identity_suffixes() {
    let mut relation = field("user", Primitive::Ulid);
    relation.value.item.relation = Some(parse_quote!(User));

    let error = relation
        .validate()
        .expect_err("one relation without the identity suffix must reject");
    assert!(error.to_string().contains("must end with '_id'"));

    relation.name = format_ident!("user_id");
    relation
        .validate()
        .expect("canonical relation identity suffix should validate");
}

#[test]
fn rust_default_matching_is_application_only() {
    let mut text = field("name", Primitive::Text);
    text.default = Some(Arg::FuncPath(parse_quote!(String::new)));

    assert!(text.default_matches_implicit_default());
    assert_eq!(
        text.rust_default_expr()
            .expect("default should exist")
            .to_string(),
        quote!(String::new().into()).to_string(),
    );
}

#[test]
fn authored_enum_defaults_preserve_type_and_variant_names() {
    let mut status = field("status", Primitive::Unit);
    status.value.item.primitive = None;
    status.value.item.is = Some(parse_quote!(Status));
    status.default = Some(Arg::ConstPath(parse_quote!(Status::Active)));

    status
        .validate()
        .expect("matching unit-enum default should validate");

    status.default = Some(Arg::ConstPath(parse_quote!(OtherStatus::Active)));
    let error = status
        .validate()
        .expect_err("a default from another enum must reject");
    assert!(
        error
            .to_string()
            .contains("does not match field type Status")
    );
}

#[test]
fn generated_ulid_is_schema_owned_and_shape_checked() {
    let mut id = field("id", Primitive::Ulid);
    id.generated = Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
        Ulid::generate
    ))));
    id.validate()
        .expect("the admitted Ulid generator should validate");

    id.value.item.primitive = Some(Primitive::Nat64);
    id.value.item.unbounded = false;
    let error = id
        .validate()
        .expect_err("a generator on the wrong primitive must reject");
    assert!(
        error
            .to_string()
            .contains("requires a primitive Ulid field")
    );
}

#[test]
fn generated_clause_requires_a_quoted_function_path() {
    let generated = FieldGeneration::from_list(&[NestedMeta::Meta(syn::Meta::NameValue(
        parse_quote!(insert = "Timestamp::now"),
    ))])
    .expect("a quoted generator path should parse");

    let FieldGeneration::Insert(Arg::FuncPath(path)) = generated else {
        panic!("generated insert must retain the authored function path");
    };
    assert_eq!(
        quote!(#path).to_string(),
        quote!(Timestamp::now).to_string()
    );
}

#[test]
fn identity_like_defaults_require_explicit_persisted_literals() {
    let mut id = field("id", Primitive::Ulid);
    id.default = Some(Arg::FuncPath(parse_quote!(Ulid::default)));

    let error = id
        .validate()
        .expect_err("identity-like implicit defaults must reject");
    assert!(error.to_string().contains("identity-like"));
}

#[test]
fn managed_fields_do_not_accept_insert_generators() {
    let mut created_at = field("created_at", Primitive::Timestamp);
    created_at.write_management = Some(FieldWriteManagement::CreatedAt);
    created_at.generated = Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
        Timestamp::now
    ))));

    let error = created_at
        .validate()
        .expect_err("managed and generated ownership must not overlap");
    assert!(error.to_string().contains("auto-managed"));
}
