use super::*;
use quote::quote;

fn parse(tokens: TokenStream) -> Result<MigrationPlan, DarlingError> {
    let plan = MigrationPlan::from_list(&NestedMeta::parse_meta_list(tokens)?)?;
    plan.validate(&CratePaths::test())?;
    Ok(plan)
}

#[test]
fn complete_plan_grammar_parses_and_emits_one_constructor() {
    let plan = parse(quote!(
        entity_migration(
            entity = "Account",
            from = 1,
            from_name = "User",
            renames(
                field(from = "email", to = "primary_email"),
                named_type(from = "Status", to = "AccountStatus"),
                variant(named_type = "Status", from = "Active", to = "Enabled"),
                record_field(named_type = "Profile", from = "display", to = "display_name"),
                constraint(from = "old", to = "new"),
                rule(named_type = "Rating", from = "range", to = "valid_range"),
                relation(from = "author", to = "creator")
            ),
            transforms(
                rewrite(from = "age", to = "age2", checked_cast(to = "Nat16")),
                fill(to = "status", literal(enum(named_type = "AccountStatus", variant = "Enabled")))
            )
        )
    )).expect("frozen grammar should parse");
    let tokens = plan
        .constructor_tokens(&crate::crate_path::CratePaths::test())
        .to_string();
    assert!(tokens.contains("SchemaMigrationPlan :: try_new"));
    assert!(tokens.contains("SchemaMigrationRename :: EnumVariant"));
    assert!(tokens.contains("SchemaMigrationTransform :: CheckedCast"));
}

#[test]
fn empty_and_unknown_plan_forms_reject() {
    assert!(parse(quote!()).is_err());
    assert!(parse(quote!(unknown(entity = "User", from = 1))).is_err());
}

#[test]
fn dependency_only_transition_parses() {
    let plan = parse(quote!(entity_migration(entity = "Holder", from = 1)))
        .expect("dependency-only transition should parse");
    assert_eq!(plan.transitions(&CratePaths::test()).unwrap().len(), 1);
}

#[test]
fn decimal_cast_target_accepts_only_the_frozen_parenthesized_spelling() {
    assert!(
        scalar_type_tokens(
            &LitStr::new("Decimal(8)", Span::call_site()),
            &crate::crate_path::CratePaths::test()
        )
        .is_ok()
    );
    assert!(
        scalar_type_tokens(
            &LitStr::new("Decimal:8", Span::call_site()),
            &crate::crate_path::CratePaths::test()
        )
        .is_err()
    );
}
