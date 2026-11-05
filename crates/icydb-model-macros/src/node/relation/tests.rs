use super::*;

fn args(tokens: TokenStream) -> Vec<NestedMeta> {
    NestedMeta::parse_meta_list(quote!(
        #tokens
    ))
    .expect("relation args should parse")
}

fn field_with_cardinality(ident: &str, opt: bool, many: bool) -> Field {
    Field {
        name: format_ident!("{ident}"),
        value: Value {
            opt,
            many,
            item: Item {
                primitive: Some(Primitive::Ulid),
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

fn scalar_field(ident: &str) -> Field {
    field_with_cardinality(ident, false, false)
}

#[test]
fn from_list_requires_name() {
    let raw = NestedMeta::parse_meta_list(quote!(rel = "User", field = "author_id"))
        .expect("relation args should parse");

    let error = Relation::from_list(&raw).expect_err("relation name must be explicit");

    assert!(error.to_string().contains("requires name"));
}

fn optional_field(ident: &str) -> Field {
    field_with_cardinality(ident, true, false)
}

#[test]
fn from_list_accepts_scalar_field_shorthand() {
    let relation = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        field = "author_id"
    )))
    .expect("scalar relation field shorthand should parse");

    assert_eq!(relation.name.value(), "author");
    assert_eq!(relation.fields.len(), 1);
    assert_eq!(relation.fields[0].value(), "author_id");
}

#[test]
fn from_list_accepts_ordered_field_list() {
    let relation = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        fields = ["tenant_id", "author_id"]
    )))
    .expect("relation field list should parse");

    assert_eq!(
        relation
            .fields
            .iter()
            .map(LitStr::value)
            .collect::<Vec<_>>(),
        ["tenant_id", "author_id"],
    );
}

#[test]
fn from_list_rejects_duplicate_field_and_fields() {
    let err = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        field = "author_id",
        fields = ["tenant_id", "author_id"]
    )))
    .expect_err("relation should reject field and fields together");

    assert!(
        err.to_string().contains("field") && err.to_string().contains("fields"),
        "unexpected error: {err}",
    );
}

#[test]
fn from_list_rejects_comma_string_fields() {
    let err = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        fields = "tenant_id, author_id"
    )))
    .expect_err("relation fields must use array syntax");

    assert!(
        err.to_string().contains("fields"),
        "unexpected error: {err}",
    );
}

#[test]
fn validate_rejects_missing_local_field() {
    let relation = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        field = "author_id"
    )))
    .expect("relation should parse");
    let fields = FieldList {
        fields: vec![scalar_field("id")],
    };

    let err = relation
        .validate(&fields, &crate::crate_path::CratePaths::test())
        .expect_err("relation should reject missing local component field");

    assert!(
        err.to_string()
            .contains("relation field 'author_id' not found"),
        "unexpected error: {err}",
    );
}

#[test]
fn validate_rejects_mixed_local_field_cardinality() {
    let relation = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        fields = ["tenant_id", "author_id"]
    )))
    .expect("relation should parse");
    let fields = FieldList {
        fields: vec![scalar_field("tenant_id"), optional_field("author_id")],
    };

    let err = relation
        .validate(&fields, &crate::crate_path::CratePaths::test())
        .expect_err("relation should reject mixed required/optional tuple components");

    assert!(
        err.to_string().contains("all required or all optional"),
        "unexpected error: {err}",
    );
}

#[test]
fn schema_part_lowers_to_relation_edge() {
    let relation = Relation::from_list(&args(quote!(
        name = "author",
        rel = "User",
        fields = ["tenant_id", "author_id"]
    )))
    .expect("relation should parse");

    let tokens = relation
        .schema_part(&crate::crate_path::CratePaths::test())
        .to_string();

    assert!(
        tokens.contains("RelationEdge :: new"),
        "unexpected schema tokens: {tokens}",
    );
    assert!(
        tokens.contains("< User as :: icydb_model :: Path > :: PATH"),
        "unexpected schema tokens: {tokens}",
    );
}
