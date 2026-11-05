#![expect(clippy::missing_const_for_fn, clippy::no_effect_underscore_binding)]

icydb::__icydb_require_migration_capability!();

use icydb::{db::DbSession, traits::CanisterKind};

#[expect(dead_code)]
fn handwritten_migration_capabilities_compile<C: CanisterKind>() {
    let _migrate = DbSession::<C>::migrate_schema;
    let _status = DbSession::<C>::schema_migration_status;
}

#[test]
fn source_migration_capability_compile_contract() {}

mod declared_retry {
    use icydb::model::prelude::*;

    #[canister(
        memory_namespace = "retry_compile",
        migration_attempt = 1,
        migrations(entity_migration(
            entity = "RetryItem",
            from = 1,
            transforms(fill(to = "score", literal(nat = 1)))
        ))
    )]
    pub struct RetryCanister;

    #[store(canister = "RetryCanister", storage(heap()))]
    pub struct RetryStore;

    #[entity(
        store = "RetryStore",
        version = 2,
        pk(field = "id"),
        fields(
            field(name = "id", value(item(prim = "Nat64"))),
            field(name = "score", value(item(prim = "Nat64")))
        )
    )]
    pub struct RetryItem;

    #[test]
    fn declared_retry_compiles_through_the_public_macro() {
        let _ = std::any::TypeId::of::<RetryCanister>();
    }
}
