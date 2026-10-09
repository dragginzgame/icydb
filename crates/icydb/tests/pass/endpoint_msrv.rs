// Included by the external MSRV consumer under ordinary and renamed dependencies.
// Handler stand-ins isolate endpoint expansion; no lifecycle behavior is claimed.
mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    #[cfg(any(feature = "metrics", feature = "migration"))]
    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_operational_controller() -> Result<(), runtime_api::Error> {
            Ok(())
        }
    }

    pub(crate) mod endpoint_handlers {
        #[cfg(feature = "metrics")]
        pub(crate) fn metrics() -> Result<runtime_api::metrics::MetricsReport, runtime_api::Error> {
            Ok(runtime_api::metrics::MetricsReport::default())
        }

        #[cfg(feature = "sql")]
        pub(crate) fn sql_query<const INTROSPECTION: bool>(
            _: String,
        ) -> Result<runtime_api::db::sql::SqlQueryResult, runtime_api::Error> {
            let _ = INTROSPECTION;
            Ok(runtime_api::db::sql::SqlQueryResult::Count {
                entity: String::new(),
                row_count: 0,
            })
        }

        #[cfg(feature = "migration")]
        pub(crate) fn schema_migrate(
            _: runtime_api::db::SchemaMigrationCommand,
        ) -> Result<runtime_api::db::SchemaMigrationStatusPage, runtime_api::Error> {
            unreachable!("compile-only migration handler")
        }
    }
}

#[cfg(feature = "sql")]
fn allow_read(_: runtime_api::ReadAuthorizationContext) -> runtime_api::ReadAuthorizationDecision {
    runtime_api::ReadAuthorizationDecision::Allow
}

runtime_api::endpoints! {
    #[cfg(feature = "metrics")]
    icydb_metrics(authorization = controller);
    #[cfg(feature = "sql")]
    icydb_sql_query(introspection = true, authorization = guard(allow_read));
    #[cfg(feature = "migration")]
    icydb_schema_migrate;
}
