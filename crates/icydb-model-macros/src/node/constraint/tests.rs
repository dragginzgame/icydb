use super::Constraint;
use darling::{FromMeta, ast::NestedMeta};
use quote::quote;

fn parse(tokens: proc_macro2::TokenStream) -> Result<Constraint, darling::Error> {
    let args = NestedMeta::parse_meta_list(tokens)?;
    Constraint::from_list(args.as_slice())
}

#[test]
fn authored_constraint_namespace_is_reserved_exactly() {
    for name in [
        "__icydb_primary_key",
        "__icydb_not_null_2",
        "__icydb_custom",
    ] {
        assert!(super::validate_constraint_name(name).is_err());
    }
    for name in ["__icydb", "_icydb_user", "__ICYDB_user", "user__icydb_name"] {
        super::validate_constraint_name(name).unwrap();
    }
}

#[test]
fn parses_named_check_declaration() {
    let constraint = parse(quote!(name = "positive_balance", check = "balance >= 0"))
        .expect("named check should parse");

    assert_eq!(constraint.name.value(), "positive_balance");
    assert_eq!(constraint.check.value(), "balance >= 0");
}

#[test]
fn rejects_missing_or_duplicate_arguments() {
    assert!(parse(quote!(name = "missing_check")).is_err());
    assert!(parse(quote!(name = "a", name = "b", check = "active = true")).is_err());
}
