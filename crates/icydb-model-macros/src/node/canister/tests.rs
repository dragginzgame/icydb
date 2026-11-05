use super::*;

#[test]
fn migration_attempt_is_validated_and_emitted_at_authoring_boundary() {
    let item: syn::ItemStruct = syn::parse_quote! { pub struct RetryCanister; };
    let meta: syn::Meta = syn::parse_quote! {
        canister(memory_namespace = "retry", migration_attempt = 7,
            migrations(entity_migration(entity = "Item", from = 1,
                transforms(fill(to = "score", literal(nat = 1))))))
    };
    let paths = crate::crate_path::CratePaths::test();
    let mut canister = Canister::from_meta(&meta).unwrap();
    canister.def = Def::new(item, crate::crate_path::CratePaths::test());
    canister.validate().unwrap();
    assert!(
        canister
            .schema_part(&paths)
            .to_string()
            .contains("with_migration_attempt (7u64)")
    );
    canister.migrations = None;
    assert!(canister.validate().is_err());
}

#[test]
fn generated_commit_stable_key_matches_schema_formatter() {
    let item: syn::ItemStruct = syn::parse_quote! {
        pub struct DemoCanister;
    };
    let canister = Canister {
        def: Def::new(item, crate::crate_path::CratePaths::test()),
        memory_namespace: "demo_rpg".to_string(),
        migrations: None,
        migration_attempt: 0,
    };
    assert_eq!(
        canister.commit_stable_key(),
        "icydb.demo_rpg.commit.control.v1",
    );
    assert_eq!(
        canister.integrity_progress_stable_key(),
        "icydb.demo_rpg.integrity.progress.v1",
    );
    assert_eq!(
        canister.startup_stable_key(),
        "icydb.demo_rpg.startup.control.v1",
    );
}
