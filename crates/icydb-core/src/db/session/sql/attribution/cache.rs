//! SQL compiled-command and shared query-plan cache attribution DTOs.
//! Does not own: cache lookup or cache mutation.

use crate::db::session::sql::cache::SqlCacheAttribution;
use candid::CandidType;
use serde::Deserialize;

///
/// SqlQueryCacheAttribution
///
/// Candid diagnostics payload for SQL compiled-command and shared query-plan
/// cache counters observed during one SQL query call.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryCacheAttribution {
    pub sql_compiled_command_hits: u64,
    pub sql_compiled_command_misses: u64,
    pub shared_query_plan_hits: u64,
    pub shared_query_plan_misses: u64,
}

impl SqlQueryCacheAttribution {
    pub(in crate::db::session::sql) const fn from_phases(
        compile: SqlCacheAttribution,
        execute: SqlCacheAttribution,
    ) -> Self {
        let merged = compile.merge(execute);

        Self {
            sql_compiled_command_hits: merged.sql_compiled_command_cache_hits,
            sql_compiled_command_misses: merged.sql_compiled_command_cache_misses,
            shared_query_plan_hits: merged.shared_query_plan_cache_hits,
            shared_query_plan_misses: merged.shared_query_plan_cache_misses,
        }
    }
}
