#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_operational_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

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

icydb::endpoints! {
    icydb_schema_migrate;
    icydb_schema_migration;
}

#[test]
fn migration_endpoint_compile_contract() {}
