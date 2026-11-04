//! Module: build::actor
//! Responsibility: host-side generated actor code construction for IcyDB canisters.
//! Does not own: schema validation, runtime session semantics, or build config parsing.
//! Boundary: turns validated schema nodes into generated private Rust capabilities.

mod crate_path;
mod db;

use std::sync::Arc;

use crate::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use icydb_schema::{SchemaMigrationPlan, encode_schema_fragment, encode_schema_migration_plan};
use proc_macro2::TokenStream;
use quote::quote;

/// Generate private canister actor code for the given authored schema path.
///
/// # Panics
///
/// Panics if the process-global schema has not validated successfully,
/// `canister_path` does not resolve to a canister node, or the consuming
/// package's `icydb` dependency path cannot be resolved, or its generated
/// migration plan does not match the complete target fragment.
#[must_use]
pub fn generate(canister_path: &str) -> String {
    generate_with_crate_path(canister_path, None)
}

fn generate_with_crate_path(canister_path: &str, icydb_crate_path: Option<&str>) -> String {
    // Load the validated schema and resolve the requested canister node.
    let schema = get_schema().expect("schema must be valid before codegen");
    let canister = schema
        .cast_node::<Canister>(canister_path)
        .expect("canister path must resolve to a canister node");
    let fragment = schema
        .schema_fragment_for_canister(canister_path)
        .expect("sealed canister database closure must lower into a schema fragment");
    let migration_plan = canister
        .migration_plan()
        .expect("source migration plan must satisfy the public schema contract");

    // Render the canister actor glue from the schema-owned metadata.
    let code = ActorBuilder::new(
        Arc::new(schema.clone()),
        canister.clone(),
        fragment,
        migration_plan,
        crate_path::resolve_path(icydb_crate_path),
    );
    drop(schema);
    let tokens = code.generate();

    tokens.to_string()
}

///
/// ActorBuilder
///
/// Internal codegen helper that renders one canister's generated runtime
/// module from the validated schema graph.
///

pub(crate) struct ActorBuilder {
    pub(crate) icydb: TokenStream,
    pub(crate) schema: Arc<Schema>,
    pub(crate) canister: Canister,
    pub(crate) schema_fragment_bytes: Vec<u8>,
    pub(crate) schema_migration_plan_bytes: Option<Vec<u8>>,
    pub(crate) schema_submission_key: String,
}

impl ActorBuilder {
    /// Create an actor builder for a specific canister.
    ///
    /// # Panics
    ///
    /// Panics if a sealed fragment/plan cannot encode, the plan does not match
    /// its target, or validated entity metadata has lost its declared store.
    #[must_use]
    pub fn new(
        schema: Arc<Schema>,
        canister: Canister,
        fragment: icydb_schema::SchemaFragment,
        migration_plan: Option<SchemaMigrationPlan>,
        icydb: TokenStream,
    ) -> Self {
        if let Some(plan) = &migration_plan {
            plan.validate_target_fragment(&fragment)
                .expect("generated migration target must satisfy the schema proposal contract");
        }
        let schema_fragment_bytes =
            encode_schema_fragment(&fragment).expect("sealed schema fragment must encode");
        let schema_migration_plan_bytes = migration_plan.as_ref().map(|plan| {
            encode_schema_migration_plan(plan).expect("sealed migration plan must encode")
        });
        let mut builder = Self {
            icydb,
            schema,
            canister,
            schema_fragment_bytes,
            schema_migration_plan_bytes,
            schema_submission_key: String::new(),
        };
        let assignments = builder
            .get_entities()
            .into_iter()
            .map(|(_, entity)| {
                let store = builder
                    .schema
                    .cast_node::<Store>(entity.store())
                    .expect("validated entity must retain its declared store");
                (
                    entity.name().to_owned(),
                    entity.store().to_owned(),
                    store
                        .journaled_memory_config()
                        .map(|config| config.key().to_owned()),
                )
            })
            .collect::<Vec<_>>();
        let assignments = assignments
            .iter()
            .map(|(entity, store, key)| (entity.as_str(), store.as_str(), key.as_deref()))
            .collect::<Vec<_>>();
        builder.schema_submission_key = icydb_schema::generated_schema_submission_key(
            &builder.schema_fragment_bytes,
            builder.schema_migration_plan_bytes.as_deref(),
            builder.canister.migration_attempt(),
            &assignments,
        );
        builder
    }

    /// Generate the full actor module (db/metrics/query glue).
    #[must_use]
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // Emit private runtime wiring and endpoint capabilities only. Standard
        // names in these tokens must stay qualified in the consumer's scope.
        tokens.extend(db::generate(&self));
        tokens.extend(generate_endpoint_runtime(&self.icydb));

        quote! {
            #tokens
        }
    }

    /// All stores belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_stores(&self) -> Vec<(String, Store)> {
        let canister_path = self.canister_path();

        self.schema
            .filter_nodes::<Store>(|node| node.canister() == canister_path)
            .map(|(path, store)| (path.to_string(), store.clone()))
            .collect()
    }

    /// All entities belonging to the current canister, keyed by path.
    #[must_use]
    pub fn get_entities(&self) -> Vec<(String, Entity)> {
        let canister_path = self.canister_path();

        self.schema
            .get_nodes::<Entity>()
            .filter_map(|(path, entity)| {
                let store = self.schema.cast_node::<Store>(entity.store()).ok()?;
                if store.canister() == canister_path {
                    Some((path.to_string(), entity.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn canister_path(&self) -> String {
        self.canister.def().path()
    }
}

fn generate_endpoint_runtime(icydb: &TokenStream) -> TokenStream {
    quote! {
        #[allow(
            unused_imports,
            reason = "downstream endpoint declarations select a feature-dependent handler subset"
        )]
        pub(crate) mod endpoint_handlers {
            pub(crate) use super::__icydb_endpoint_handler_schema as schema;

            #icydb::__icydb_with_sql_items! {
                pub(crate) use super::__icydb_endpoint_handler_fixtures_load as fixtures_load;
                pub(crate) use super::__icydb_endpoint_handler_fixtures_reset as fixtures_reset;
                pub(crate) use super::__icydb_endpoint_handler_sql_ddl as sql_ddl;
                pub(crate) use super::__icydb_endpoint_handler_sql_integrity as sql_integrity;
                pub(crate) use super::__icydb_endpoint_handler_sql_query as sql_query;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_bounded as sql_update_bounded;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_primary_key as sql_update_primary_key;
            }

            #icydb::__icydb_with_migration_items! {
                pub(crate) fn schema_migrate(
                    command: #icydb::db::SchemaMigrationCommand,
                ) -> ::std::result::Result<#icydb::db::SchemaMigrationStatusPage, #icydb::Error> {
                    let session = #icydb::db::DbSession::new(super::core_db()?);
                    session.migrate_generated_schema(
                        super::ICYDB_SCHEMA_FRAGMENT,
                        super::ICYDB_SCHEMA_MIGRATION_PLAN,
                        super::ICYDB_SCHEMA_SUBMISSION_KEY,
                        super::ICYDB_SCHEMA_ENTITY_STORES,
                        command,
                    )
                }

                pub(crate) fn schema_migration(
                    request: &#icydb::db::SchemaMigrationStatusRequest,
                ) -> ::std::result::Result<#icydb::db::SchemaMigrationStatusPage, #icydb::Error> {
                    let session = #icydb::db::DbSession::new(super::core_db()?);
                    session.generated_schema_migration_status(
                        super::ICYDB_SCHEMA_FRAGMENT,
                        super::ICYDB_SCHEMA_MIGRATION_PLAN,
                        super::ICYDB_SCHEMA_SUBMISSION_KEY,
                        super::ICYDB_SCHEMA_ENTITY_STORES,
                        request,
                    )
                }
            }

            #icydb::__icydb_with_metrics_items! {
                pub(crate) fn metrics(
                ) -> ::std::result::Result<#icydb::metrics::MetricsReport, #icydb::Error> {
                    // The read-only startup boundary qualifies and selects the
                    // current database without requiring ordinary-work readiness.
                    super::startup_state().map_err(|failure| failure.error().clone())?;
                    #icydb::metrics::metrics_report()
                }

                #[allow(
                    clippy::unnecessary_wraps,
                    reason = "generated endpoint handlers retain one uniform fallible dispatch contract"
                )]
                pub(crate) fn metrics_reset() -> ::std::result::Result<(), #icydb::Error> {
                    #icydb::metrics::metrics_reset_all();
                    ::std::result::Result::Ok(())
                }
            }

            pub(crate) fn snapshot(
            ) -> ::std::result::Result<#icydb::db::StorageReport, #icydb::Error> {
                #icydb::__macro::execute_generated_storage_report(&super::db()?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::node::{Canister, Def, Schema};
    use proc_macro2::TokenStream;

    use super::ActorBuilder;

    fn compact_tokens(tokens: TokenStream) -> String {
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
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
            None,
            quote::quote!(::icydb),
        )
    }

    fn migration_target_fragment(version: u32) -> icydb_schema::SchemaFragment {
        use icydb_schema::{
            DeclaredEntityVersion, EntityFragment, FieldFragment, FieldInsertPolicy,
            FieldSourceKey, FieldType, ScalarType, SchemaFragment, SchemaName,
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
                    to: FieldSourceKey::try_new("primary_email")
                        .expect("field source should admit"),
                }],
                Vec::new(),
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit");
        ActorBuilder::new(
            Arc::new(Schema::new()),
            Canister::new(Def::new("test", "Canister"), "test", None),
            migration_target_fragment(2),
            Some(migration),
            quote::quote!(::icydb),
        )
    }

    #[test]
    fn generated_endpoint_runtime_is_private_and_contains_no_export_attributes() {
        let surface = compact_tokens(super::generate_endpoint_runtime(&quote::quote!(::icydb)));

        for handler in [
            "metrics",
            "metrics_reset",
            "snapshot",
            "schema",
            "schema_migrate",
            "schema_migration",
            "sql_query",
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
        let surface = compact_tokens(actor_builder().generate());

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
                migration_target_fragment(3),
                Some(plan),
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
                migration_target_fragment(1),
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
            icydb_schema::decode_schema_fragment(&original.schema_fragment_bytes).unwrap(),
            original
                .schema_migration_plan_bytes
                .as_ref()
                .map(|bytes| icydb_schema::decode_schema_migration_plan(bytes).unwrap()),
            original.icydb.clone(),
        );
        assert_ne!(original.schema_submission_key, next.schema_submission_key);
        assert_eq!(original.schema_fragment_bytes, next.schema_fragment_bytes);
        assert_eq!(
            original.schema_migration_plan_bytes,
            next.schema_migration_plan_bytes
        );
        let expected = next.schema_submission_key.clone();
        assert!(compact_tokens(next.generate()).contains(&expected));
    }

    #[test]
    fn source_migration_plan_is_bound_into_private_actor_bootstrap_and_submission_identity() {
        let without = actor_builder();
        let without_key = without.schema_submission_key.clone();
        let without_surface = compact_tokens(without.generate());
        let with = actor_builder_with_migration();
        let with_key = with.schema_submission_key.clone();
        let with_surface = compact_tokens(with.generate());

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
        let tokens = compact_tokens(tokens);
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
}
