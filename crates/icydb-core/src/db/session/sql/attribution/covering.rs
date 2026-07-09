//! SQL covering-read diagnostics DTOs.
//! Does not own: projection execution or top-level query attribution assembly.

use crate::db::session::sql::projection::SqlProjectionMaterializationMetrics;
use candid::CandidType;
use serde::Deserialize;

///
/// SqlPureCoveringAttribution
///
/// Candid diagnostics payload for pure covering projection counters.
/// The value is optional on the top-level SQL attribution because most query
/// shapes do not enter this projection path.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlPureCoveringAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

impl SqlPureCoveringAttribution {
    pub(in crate::db::session::sql) const fn from_local_instructions(
        decode_local_instructions: u64,
        row_assembly_local_instructions: u64,
    ) -> Option<Self> {
        if decode_local_instructions == 0 && row_assembly_local_instructions == 0 {
            return None;
        }

        Some(Self {
            decode_local_instructions,
            row_assembly_local_instructions,
        })
    }
}

///
/// SqlHybridCoveringAttribution
///
/// Candid diagnostics payload for hybrid covering projection counters.
/// Hybrid covering reads use index/primary-key values where possible and sparse
/// row reads only for uncovered projected fields.
///

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlHybridCoveringAttribution {
    pub path_hits: u64,
    pub index_field_accesses: u64,
    pub row_field_accesses: u64,
}

impl SqlHybridCoveringAttribution {
    pub(in crate::db::session::sql) const fn from_projection_metrics(
        metrics: SqlProjectionMaterializationMetrics,
    ) -> Option<Self> {
        if metrics.has_hybrid_covering_work() {
            Some(Self {
                path_hits: metrics.hybrid_covering_path_hits,
                index_field_accesses: metrics.hybrid_covering_index_field_accesses,
                row_field_accesses: metrics.hybrid_covering_row_field_accesses,
            })
        } else {
            None
        }
    }
}
