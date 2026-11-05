use super::{PrimaryKey, PrimaryKeySource};
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

fn parse_primary_key(tokens: proc_macro2::TokenStream) -> Result<PrimaryKey, darling::Error> {
    let args = NestedMeta::parse_meta_list(tokens).expect("test meta should parse");
    PrimaryKey::from_list(&args)
}

#[test]
fn from_list_parses_scalar_fields_syntax() {
    let primary_key = parse_primary_key(quote!(fields = ["id"]))
        .expect("scalar primary-key fields syntax should parse");

    assert_eq!(primary_key.scalar_field().to_string(), "id");
    assert_eq!(primary_key.fields().len(), 1);
    assert_eq!(primary_key.source, PrimaryKeySource::Internal);
}

#[test]
fn from_list_parses_explicit_external_source() {
    let primary_key = parse_primary_key(quote!(fields = ["pid"], source = "external"))
        .expect("explicit external primary-key source should parse");

    assert_eq!(primary_key.scalar_field().to_string(), "pid");
    assert_eq!(primary_key.fields().len(), 1);
    assert_eq!(primary_key.source, PrimaryKeySource::External);
}

#[test]
fn from_list_parses_scalar_field_shorthand() {
    let primary_key = parse_primary_key(quote!(field = "id"))
        .expect("scalar primary-key field shorthand should parse");

    assert_eq!(primary_key.scalar_field().to_string(), "id");
    assert_eq!(primary_key.fields().len(), 1);
    assert_eq!(primary_key.source, PrimaryKeySource::Internal);
}

#[test]
fn from_list_rejects_mixed_field_and_fields_syntax() {
    let err = parse_primary_key(quote!(field = "id", fields = ["tenant_id", "id"]))
        .expect_err("primary-key field and fields syntax should be mutually exclusive");

    assert!(
        err.to_string().contains(
            "pk(...) accepts either one field = \"...\" argument or one fields = [...] argument"
        ),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_comma_string_fields() {
    let err = parse_primary_key(quote!(fields = "id, name"))
        .expect_err("primary-key fields should require array syntax");

    assert!(
        err.to_string().contains("not a comma-string"),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_empty_fields() {
    let err =
        parse_primary_key(quote!(fields = [])).expect_err("empty primary-key fields should reject");

    assert!(
        err.to_string().contains("at least one field"),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_duplicate_fields() {
    let err = parse_primary_key(quote!(fields = ["tenant_id", "tenant_id"]))
        .expect_err("duplicate primary-key fields should reject");

    assert!(
        err.to_string()
            .contains("primary key field 'tenant_id' is declared more than once"),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_too_many_fields() {
    let err = parse_primary_key(quote!(fields = ["a", "b", "c", "d", "e"]))
        .expect_err("too many primary-key fields should reject");

    assert!(
        err.to_string().contains("supports at most 4 fields"),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_parses_composite_with_external_default() {
    let primary_key = parse_primary_key(quote!(fields = ["tenant_id", "local_id"]))
        .expect("composite primary-key fields syntax should parse");

    assert_eq!(
        primary_key
            .fields()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["tenant_id", "local_id"],
    );
    assert_eq!(primary_key.source, PrimaryKeySource::External);
}

#[test]
fn from_list_parses_explicit_external_composite_source() {
    let primary_key = parse_primary_key(quote!(
        fields = ["tenant_id", "local_id"],
        source = "external"
    ))
    .expect("explicit external composite primary-key source should parse");

    assert_eq!(primary_key.fields().len(), 2);
    assert_eq!(primary_key.source, PrimaryKeySource::External);
}

#[test]
fn from_list_rejects_explicit_internal_composite_source() {
    let err = parse_primary_key(quote!(
        fields = ["tenant_id", "local_id"],
        source = "internal"
    ))
    .expect_err("internal composite primary-key source should reject");

    assert!(
        err.to_string()
            .contains("composite primary keys are external-only"),
        "unexpected error: {err}",
    );
}
