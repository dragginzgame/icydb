//! Module: build::actor
//! Responsibility: host-side generated actor code construction for IcyDB canisters.
//! Does not own: schema validation, runtime session semantics, or build config parsing.
//! Boundary: turns validated schema nodes into generated private Rust capabilities.

mod crate_path;
mod db;
mod endpoint;

use std::sync::Arc;

use crate::{
    build::get_schema,
    node::{Canister, Entity, Schema, Store},
};
use icydb_schema::{SchemaMigrationPlan, encode_schema_fragment, encode_schema_migration_plan};
use proc_macro2::TokenStream;
use quote::quote;
use sha2::{Digest, Sha256};

/// Generate private canister actor code for the given authored schema path.
///
/// # Panics
///
/// Panics if the process-global schema has not validated successfully,
/// `canister_path` does not resolve to a canister node, or the consuming
/// package's `icydb` dependency path cannot be resolved.
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
    );
    drop(schema);
    let tokens = crate_path::rewrite_icydb_path(code.generate(), icydb_crate_path);

    tokens.to_string()
}

///
/// ActorBuilder
///
/// Internal codegen helper that renders one canister's generated runtime
/// module from the validated schema graph.
///

pub(crate) struct ActorBuilder {
    pub(crate) schema: Arc<Schema>,
    pub(crate) canister: Canister,
    pub(crate) schema_fragment_bytes: Vec<u8>,
    pub(crate) schema_migration_plan_bytes: Option<Vec<u8>>,
    pub(crate) schema_submission_key: String,
}

impl ActorBuilder {
    /// Create an actor builder for a specific canister.
    #[must_use]
    pub fn new(
        schema: Arc<Schema>,
        canister: Canister,
        fragment: icydb_schema::SchemaFragment,
        migration_plan: Option<SchemaMigrationPlan>,
    ) -> Self {
        let schema_fragment_bytes =
            encode_schema_fragment(&fragment).expect("sealed schema fragment must encode");
        let schema_migration_plan_bytes = migration_plan.as_ref().map(|plan| {
            encode_schema_migration_plan(plan).expect("sealed migration plan must encode")
        });
        let mut hasher = Sha256::new();
        hasher.update(b"icydb.generated-schema-submission.v2");
        hasher.update((schema_fragment_bytes.len() as u64).to_be_bytes());
        hasher.update(schema_fragment_bytes.as_slice());
        if let Some(bytes) = &schema_migration_plan_bytes {
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        } else {
            hasher.update(0_u64.to_be_bytes());
        }
        let digest = hasher.finalize();
        let schema_submission_key = format!("generated/{}", hex_bytes(digest.as_slice()));

        Self {
            schema,
            canister,
            schema_fragment_bytes,
            schema_migration_plan_bytes,
            schema_submission_key,
        }
    }

    /// Generate the full actor module (db/metrics/query glue).
    #[must_use]
    pub fn generate(self) -> TokenStream {
        let mut tokens = quote!();

        // Emit private runtime wiring and endpoint capabilities only.
        tokens.extend(db::generate(&self));
        tokens.extend(generate_endpoint_runtime());

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

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Render private operational capabilities and authorization policies used by
/// source-declared endpoint wrappers.
fn generate_endpoint_runtime() -> TokenStream {
    let authorization = endpoint::emit_endpoint_authorization_helpers();

    quote! {
        #authorization

        #[allow(
            unused_imports,
            reason = "downstream endpoint declarations select a feature-dependent handler subset"
        )]
        pub(crate) mod endpoint_handlers {
            pub(crate) use super::__icydb_endpoint_handler_schema as schema;

            ::icydb::__icydb_with_sql_items! {
                pub(crate) use super::__icydb_endpoint_handler_fixtures_load as fixtures_load;
                pub(crate) use super::__icydb_endpoint_handler_fixtures_reset as fixtures_reset;
                pub(crate) use super::__icydb_endpoint_handler_sql_ddl as sql_ddl;
                pub(crate) use super::__icydb_endpoint_handler_sql_integrity as sql_integrity;
                pub(crate) use super::__icydb_endpoint_handler_sql_query as sql_query;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_bounded as sql_update_bounded;
                pub(crate) use super::__icydb_endpoint_handler_sql_update_primary_key as sql_update_primary_key;
            }

            ::icydb::__icydb_with_migration_items! {
                pub(crate) fn schema_migrate(
                    command: ::icydb::db::SchemaMigrationCommand,
                ) -> Result<::icydb::db::SchemaMigrationStatusPage, ::icydb::Error> {
                    let session = ::icydb::db::DbSession::new(super::core_db()?);
                    session.migrate_generated_schema(
                        super::ICYDB_SCHEMA_FRAGMENT,
                        super::ICYDB_SCHEMA_MIGRATION_PLAN,
                        super::ICYDB_SCHEMA_SUBMISSION_KEY,
                        super::ICYDB_SCHEMA_ENTITY_STORES,
                        command,
                    )
                }

                pub(crate) fn schema_migration(
                    request: &::icydb::db::SchemaMigrationStatusRequest,
                ) -> Result<::icydb::db::SchemaMigrationStatusPage, ::icydb::Error> {
                    let session = ::icydb::db::DbSession::new(super::core_db()?);
                    session.generated_schema_migration_status(
                        super::ICYDB_SCHEMA_FRAGMENT,
                        super::ICYDB_SCHEMA_MIGRATION_PLAN,
                        super::ICYDB_SCHEMA_SUBMISSION_KEY,
                        super::ICYDB_SCHEMA_ENTITY_STORES,
                        request,
                    )
                }
            }

            #[allow(
                clippy::unnecessary_wraps,
                reason = "generated endpoint handlers retain one uniform fallible dispatch contract"
            )]
            pub(crate) fn metrics(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::CompactMetricsReport, ::icydb::Error> {
                Ok(::icydb::metrics::compact_metrics_report(window_start_ms))
            }

            #[allow(
                clippy::unnecessary_wraps,
                reason = "generated endpoint handlers retain one uniform fallible dispatch contract"
            )]
            pub(crate) fn metrics_extended(
                window_start_ms: Option<u64>,
            ) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
                Ok(::icydb::metrics::metrics_report(window_start_ms))
            }

            #[allow(
                clippy::unnecessary_wraps,
                reason = "generated endpoint handlers retain one uniform fallible dispatch contract"
            )]
            pub(crate) fn metrics_reset() -> Result<(), ::icydb::Error> {
                ::icydb::metrics::metrics_reset_all();
                Ok(())
            }

            pub(crate) fn snapshot(
            ) -> Result<::icydb::db::StorageReport, ::icydb::Error> {
                ::icydb::__macro::execute_generated_storage_report(&super::db()?)
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
            Canister::new(Def::new("test", "Canister"), "test", 0, 1, 2, 4, 3, None),
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
            None,
        )
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
            Canister::new(Def::new("test", "Canister"), "test", 0, 1, 2, 4, 3, None),
            icydb_schema::SchemaFragment::try_new(Vec::new(), Vec::new())
                .expect("empty test fragment should admit"),
            Some(migration),
        )
    }

    #[test]
    fn generated_endpoint_runtime_is_private_and_contains_no_export_attributes() {
        let surface = compact_tokens(super::generate_endpoint_runtime());

        for handler in [
            "metrics",
            "metrics_extended",
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
        assert!(surface.contains("pub(crate)modendpoint_authorization"));
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
}
