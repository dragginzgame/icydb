use canic_icydb::__private::cdk as __icydb_host_sdk;
use icydb as __icydb_facade;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn schema_migrate(
            _: icydb::db::SchemaMigrationCommand,
        ) -> Result<icydb::db::SchemaMigrationStatusPage, icydb::Error> {
            unreachable!()
        }

        pub(crate) fn schema_migration(
            _: &icydb::db::SchemaMigrationStatusRequest,
        ) -> Result<icydb::db::SchemaMigrationStatusPage, icydb::Error> {
            unreachable!()
        }
    }
}

canic_icydb::mount! {
    icydb_schema_migrate;
    icydb_schema_migration;
}

#[test]
fn migration_endpoint_compile_contract() {}
