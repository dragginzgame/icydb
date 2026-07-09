//! SQL compile-phase diagnostics DTOs.
//! Does not own: execute-phase or top-level query attribution assembly.

use crate::db::session::sql::compile::SqlCompilePhaseAttribution;
use candid::CandidType;
use serde::Deserialize;

///
/// SqlCompileAttribution
///
/// Candid diagnostics payload for SQL front-end compile counters.
/// The short field names are scoped by the `compile` parent field on
/// `SqlQueryExecutionAttribution`.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlCompileAttribution {
    pub cache_key_local_instructions: u64,
    pub cache_lookup_local_instructions: u64,
    pub parse_local_instructions: u64,
    pub parse_tokenize_local_instructions: u64,
    pub parse_select_local_instructions: u64,
    pub parse_expr_local_instructions: u64,
    pub parse_predicate_local_instructions: u64,
    pub aggregate_lane_check_local_instructions: u64,
    pub prepare_local_instructions: u64,
    pub lower_local_instructions: u64,
    pub bind_local_instructions: u64,
    pub cache_insert_local_instructions: u64,
}

impl SqlCompileAttribution {
    pub(in crate::db::session::sql) const fn from_phase(phase: SqlCompilePhaseAttribution) -> Self {
        Self {
            cache_key_local_instructions: phase.cache_key,
            cache_lookup_local_instructions: phase.cache_lookup,
            parse_local_instructions: phase.parse,
            parse_tokenize_local_instructions: phase.parse_tokenize,
            parse_select_local_instructions: phase.parse_select,
            parse_expr_local_instructions: phase.parse_expr,
            parse_predicate_local_instructions: phase.parse_predicate,
            aggregate_lane_check_local_instructions: phase.aggregate_lane_check,
            prepare_local_instructions: phase.prepare,
            lower_local_instructions: phase.lower,
            bind_local_instructions: phase.bind,
            cache_insert_local_instructions: phase.cache_insert,
        }
    }
}
