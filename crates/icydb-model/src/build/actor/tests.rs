use std::sync::Arc;

use crate::node::{Canister, Def, Schema};
use proc_macro2::TokenStream;

use super::ActorBuilder;

fn compact_tokens(tokens: &TokenStream) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn actor_builder() -> ActorBuilder {
    ActorBuilder::new(
        Arc::new(Schema::new()),
        Canister::new(Def::new("test", "Canister"), "test", None),
        &icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
            .expect("empty test fragment should admit"),
        None,
        quote::quote!(::icydb),
    )
}

fn migration_target_fragment(version: u32) -> icydb_schema::SchemaFragment {
    use icydb_schema::{
        DeclaredEntityVersion, EntityFragment, FieldFragment, FieldInsertPolicy, FieldSourceKey,
        FieldType, ScalarType, SchemaFragment, SchemaName,
    };
    let field = FieldFragment::new(
        SchemaName::try_new("primary_email").unwrap(),
        FieldType::Scalar(ScalarType::Text { max_len: None }),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let entity = EntityFragment::try_new(
        SchemaName::try_new("Account").unwrap(),
        DeclaredEntityVersion::try_new(version).unwrap(),
        vec![
            field,
            FieldFragment::new(
                SchemaName::try_new("id").unwrap(),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![FieldSourceKey::try_new("id").unwrap()],
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    SchemaFragment::try_new(vec![entity], vec![]).unwrap()
}

fn actor_builder_with_migration() -> ActorBuilder {
    use icydb_schema::{
        DeclaredEntityVersion, EntityMigration, EntitySourceKey, FieldSourceKey,
        SchemaMigrationPlan, SchemaMigrationRename,
    };

    let migration = SchemaMigrationPlan::try_new(vec![
        EntityMigration::try_new(
            EntitySourceKey::try_new("Account").expect("entity source should admit"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            Some(EntitySourceKey::try_new("User").expect("entity source should admit")),
            vec![SchemaMigrationRename::Field {
                from: FieldSourceKey::try_new("email").expect("field source should admit"),
                to: FieldSourceKey::try_new("primary_email").expect("field source should admit"),
            }],
            Vec::new(),
        )
        .expect("transition should admit"),
    ])
    .expect("plan should admit");
    ActorBuilder::new(
        Arc::new(Schema::new()),
        Canister::new(Def::new("test", "Canister"), "test", None),
        &migration_target_fragment(2),
        Some(&migration),
        quote::quote!(::icydb),
    )
}

#[test]
fn generated_endpoint_runtime_is_private_and_contains_no_export_attributes() {
    let surface = compact_tokens(&super::generate_endpoint_runtime(&quote::quote!(::icydb)));

    for handler in [
        "metrics",
        "metrics_reset",
        "snapshot",
        "schema",
        "schema_migrate",
        "schema_migration",
        "sql_query_public",
        "sql_query_trusted",
    ] {
        assert!(surface.contains(handler));
    }
    for forbidden in [
        "ic_cdk::query",
        "ic_cdk::update",
        "export_name",
        "no_mangle",
    ] {
        assert!(!surface.contains(forbidden));
    }
}

#[test]
fn complete_generated_actor_cannot_create_a_public_endpoint() {
    let surface = compact_tokens(&actor_builder().generate());

    assert!(surface.contains("pub(crate)modendpoint_handlers"));
    for forbidden in [
        "ic_cdk::query",
        "ic_cdk::update",
        "export_name",
        "no_mangle",
    ] {
        assert!(!surface.contains(forbidden));
    }
}

#[test]
fn actor_generation_checks_migration_target_before_emitting_bootstrap() {
    let builder = actor_builder_with_migration();
    let plan = icydb_schema::decode_schema_migration_plan(
        builder.schema_migration_plan_bytes.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(
        plan.validate_target_fragment(&migration_target_fragment(2)),
        Ok(())
    );
    assert_eq!(
        plan.validate_target_fragment(&migration_target_fragment(3)),
        Err(icydb_schema::SchemaContractError::MigrationVersionGap)
    );
    let empty = icydb_schema::SchemaFragment::try_new(vec![], vec![]).unwrap();
    assert_eq!(
        plan.validate_target_fragment(&empty),
        Err(icydb_schema::SchemaContractError::InvalidMigrationReference)
    );
    assert!(
        std::panic::catch_unwind(|| ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", None),
            &migration_target_fragment(3),
            Some(&plan),
            quote::quote!(::icydb),
        ))
        .is_err()
    );
}

#[test]
fn generated_readiness_binds_assignments_and_modes_but_not_unused_stores() {
    use crate::node::{
        Entity, FieldList, PrimaryKey, PrimaryKeySource, SchemaNode, Store, StoreHeapConfig,
        StoreJournaledMemoryConfig, Type,
    };
    let key = |path: &'static str, journal: bool, unused: bool| {
        let mut schema = Schema::new();
        let store = if journal {
            Store::new_journaled(
                Def::new("test", "A"),
                "test::Canister",
                StoreJournaledMemoryConfig::new("a"),
            )
        } else {
            Store::new_heap(
                Def::new("test", "A"),
                "test::Canister",
                StoreHeapConfig::new(),
            )
        };
        schema.insert_node(SchemaNode::Store(store));
        schema.insert_node(SchemaNode::Store(Store::new_heap(
            Def::new("test", "B"),
            "test::Canister",
            StoreHeapConfig::new(),
        )));
        if unused {
            schema.insert_node(SchemaNode::Store(Store::new_heap(
                Def::new("test", "Unused"),
                "test::Canister",
                StoreHeapConfig::new(),
            )));
        }
        schema.insert_node(SchemaNode::Entity(Entity::new(
            Def::new("test", "Item"),
            path,
            1,
            PrimaryKey::new(&["id"], PrimaryKeySource::Internal),
            &[],
            &[],
            &[],
            FieldList::new(&[]),
            Type::new(&[], &[], &[]),
        )));
        ActorBuilder::new(
            Arc::new(schema),
            Canister::new(Def::new("test", "Canister"), "test", None),
            &migration_target_fragment(1),
            None,
            quote::quote!(::icydb),
        )
        .schema_submission_key
    };
    let original = key("test::A", true, false);
    assert_eq!(original, key("test::A", true, true));
    assert_ne!(original, key("test::B", true, false));
    assert_ne!(original, key("test::A", false, false));
}

#[test]
fn migration_attempt_changes_only_generated_submission_identity() {
    let original = actor_builder_with_migration();
    let next = ActorBuilder::new(
        original.schema.clone(),
        original.canister.clone().with_migration_attempt(1),
        &icydb_schema::decode_schema_fragment(&original.schema_fragment_bytes).unwrap(),
        original
            .schema_migration_plan_bytes
            .as_ref()
            .map(|bytes| icydb_schema::decode_schema_migration_plan(bytes).unwrap())
            .as_ref(),
        original.icydb.clone(),
    );
    assert_ne!(original.schema_submission_key, next.schema_submission_key);
    assert_eq!(original.schema_fragment_bytes, next.schema_fragment_bytes);
    assert_eq!(
        original.schema_migration_plan_bytes,
        next.schema_migration_plan_bytes
    );
    let expected = next.schema_submission_key.clone();
    assert!(compact_tokens(&next.generate()).contains(&expected));
}

#[test]
fn source_migration_plan_is_bound_into_private_actor_bootstrap_and_submission_identity() {
    let without = actor_builder();
    let without_key = without.schema_submission_key.clone();
    let without_surface = compact_tokens(&without.generate());
    let with = actor_builder_with_migration();
    let with_key = with.schema_submission_key.clone();
    let with_surface = compact_tokens(&with.generate());

    assert_ne!(with_key, without_key);
    assert!(!without_surface.contains("__icydb_require_migration_capability"));
    assert!(with_surface.contains("__icydb_require_migration_capability"));
    assert!(with_surface.contains("ICYDB_SCHEMA_MIGRATION_PLAN"));
    assert!(without_surface.contains("apply_generated_schema_fragment"));
    assert!(!without_surface.contains("ensure_generated_schema_fragment"));
    assert!(with_surface.contains("ensure_generated_schema_fragment"));
}
#[test]
fn renamed_runtime_path_is_used_by_every_actor_surface() {
    let mut builder = actor_builder_with_migration();
    builder.icydb = super::crate_path::resolve_path(Some("::runtime_api"));
    let tokens = builder.generate();
    syn::parse2::<syn::File>(tokens.clone()).expect("renamed actor remains valid Rust");
    let tokens = compact_tokens(&tokens);
    for surface in [
        "::runtime_api::Error",
        "::runtime_api::db::DbSession",
        "::runtime_api::__icydb_with_sql_items",
        "::runtime_api::__icydb_with_migration_items",
    ] {
        assert!(
            tokens.contains(surface),
            "missing runtime-owned surface {surface}"
        );
    }
    assert!(
        tokens.contains("icydb.test"),
        "memory identity is authored data"
    );
}
