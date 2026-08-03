icydb::__icydb_require_migration_capability!();

use icydb::{db::DbSession, traits::CanisterKind};

fn handwritten_migration_capabilities_compile<C: CanisterKind>() {
    let _migrate = DbSession::<C>::migrate_schema;
    let _status = DbSession::<C>::schema_migration_status;
}

fn main() {}
