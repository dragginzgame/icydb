#![allow(clippy::missing_const_for_fn, clippy::no_effect_underscore_binding)]

icydb::__icydb_require_migration_capability!();

use icydb::{db::DbSession, traits::CanisterKind};

#[allow(dead_code)]
fn handwritten_migration_capabilities_compile<C: CanisterKind>() {
    let _migrate = DbSession::<C>::migrate_schema;
    let _status = DbSession::<C>::schema_migration_status;
}

#[test]
fn source_migration_capability_compile_contract() {}
