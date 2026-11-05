use crate::node::index::{Index, IndexExpressionSpec, IndexKeyItemSpec};
use darling::{FromMeta, ast::NestedMeta};
use proc_macro2::Span;
use quote::quote;
use syn::LitStr;

fn field_list(values: &[&str]) -> Vec<LitStr> {
    values
        .iter()
        .map(|value| LitStr::new(value, Span::call_site()))
        .collect()
}

fn parse_index(tokens: proc_macro2::TokenStream) -> Result<Index, darling::Error> {
    let args = NestedMeta::parse_meta_list(tokens).expect("test meta should parse");
    Index::from_list(&args)
}

#[test]
fn automatic_names_preserve_parsed_predicates_and_ignore_cosmetic_sql() {
    let resolve = |sql: &str| {
        parse_index(quote!(field = "email", predicate = #sql))
            .unwrap()
            .resolved_name("User")
            .unwrap()
    };
    let expected = resolve("active = true AND email IS NOT NULL");
    for sql in [
        " ( active=TRUE and email is not null ) ",
        "active = true AND (email IS NOT NULL)",
    ] {
        assert_eq!(resolve(sql), expected);
    }
    assert_ne!(resolve("active = false AND email IS NOT NULL"), expected);
    assert_ne!(resolve("active = true OR email IS NOT NULL"), expected);
    assert_ne!(resolve("email = 'a'"), resolve("email = 'A'"));
    assert_ne!(resolve("email = other"), resolve("email = 'other'"));
    assert_eq!(resolve("1 < score"), resolve("score > 1"));
    assert_ne!(resolve("score IN (1, 2)"), resolve("score NOT IN (1, 2)"));
}

#[test]
fn explicit_names_are_optional_and_independent_of_the_definition() {
    let index = parse_index(quote!(name = "chosen", field = "email")).unwrap();
    assert_eq!(index.resolved_name("User").unwrap(), "chosen");
    assert_eq!(index.resolved_name("Other").unwrap(), "chosen");
    let changed = parse_index(quote!(
        name = "chosen",
        field = "score",
        unique,
        predicate = "active = true"
    ))
    .unwrap();
    assert_eq!(changed.resolved_name("User").unwrap(), "chosen");
    let automatic = parse_index(quote!(field = "email")).unwrap();
    assert_eq!(
        automatic.resolved_name("User").unwrap(),
        icydb_schema::canonical_index_name("User", &["email"], false, None)
    );
}

#[test]
fn from_list_parses_scalar_field_shorthand() {
    let index = parse_index(quote!(field = "email")).expect("scalar index shorthand should parse");

    assert_eq!(
        index.fields.iter().map(LitStr::value).collect::<Vec<_>>(),
        ["email"],
    );
    assert!(!index.unique);
    assert_eq!(index.predicate, None);
}

#[test]
fn from_list_parses_fields_unique_and_predicate() {
    let index = parse_index(quote!(
        fields = ["tenant_id", "LOWER(email)"],
        unique,
        predicate = "active = true"
    ))
    .expect("index fields syntax should parse");

    assert_eq!(
        index.fields.iter().map(LitStr::value).collect::<Vec<_>>(),
        ["tenant_id", "LOWER(email)"],
    );
    assert!(index.unique);
    assert_eq!(index.predicate.as_deref(), Some("active = true"));
}

#[test]
fn from_list_rejects_mixed_field_and_fields_syntax() {
    let err = parse_index(quote!(field = "email", fields = ["tenant_id", "email"]))
        .expect_err("index field and fields syntax should be mutually exclusive");

    assert!(
        err.to_string().contains(
            "index(...) accepts either one field = \"...\" argument or one fields = [...] argument"
        ),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_comma_string_fields() {
    let err = parse_index(quote!(fields = "tenant_id, email"))
        .expect_err("index fields should reject comma strings");

    assert!(
        err.to_string().contains("not a comma-string"),
        "unexpected error: {err}",
    );
}

#[test]
fn parsed_key_items_accept_supported_expression_and_field_mix() {
    let index = Index {
        name: None,
        fields: field_list(&["tenant_id", "LOWER(email)"]),
        unique: true,
        predicate: None,
    };

    let key_items = index
        .parsed_key_items()
        .expect("supported index fields should parse");

    assert_eq!(
        key_items,
        vec![
            IndexKeyItemSpec::Field(syn::parse_quote!(tenant_id)),
            IndexKeyItemSpec::Expression(IndexExpressionSpec::Lower(syn::parse_quote!(email))),
        ],
    );
}

#[test]
fn generated_name_uses_expression_key_item_canonical_text() {
    let index = Index {
        name: None,
        fields: field_list(&["LOWER(email)"]),
        unique: false,
        predicate: None,
    };

    assert_eq!(
        index.resolved_name("User").unwrap(),
        icydb_schema::canonical_index_name("User", &["LOWER(email)"], false, None)
    );
}
