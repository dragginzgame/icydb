//! Module: sql_generator::shard
//! Responsibility: canonical scheduled SQL scenario-to-shard assignment.
//! Does not own: scenario selection, execution, receipts, or artifact merging.
//! Boundary: maps one stable scenario identity into the fixed shared eight-shard contract.

use std::{
    error::Error,
    fmt::{self, Display},
};

/// Fixed shard count shared by scheduled SQL correctness and performance lanes.
pub const SQL_SCHEDULED_SHARD_COUNT: u8 = 8;

/// Domain separator for the sole current scheduled SQL shard mapping.
const SQL_SHARD_DOMAIN: &[u8] = b"icydb-sql-shard/v1";

///
/// ScenarioShardError
///
/// Typed failure to encode or represent one scheduled SQL shard assignment.
/// Owned by the shared SQL evidence boundary and consumed by lane-specific errors.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScenarioShardError {
    /// A scenario identity was empty and therefore not stable evidence.
    EmptyScenarioId,

    /// A scenario identity length cannot be represented by the canonical encoding.
    ScenarioIdTooLong {
        /// Observed UTF-8 byte length.
        observed_bytes: usize,
    },
}

impl Display for ScenarioShardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyScenarioId => formatter.write_str("SQL scenario ID must not be empty"),
            Self::ScenarioIdTooLong { observed_bytes } => write!(
                formatter,
                "SQL scenario ID has {observed_bytes} bytes, exceeding the u32 canonical length bound",
            ),
        }
    }
}

impl Error for ScenarioShardError {}

/// Map one stable SQL scenario identity into the shared scheduled shard contract.
///
/// The mapping hashes the versioned domain, big-endian UTF-8 byte length, and
/// identity bytes, then interprets the first eight digest bytes as little-endian.
///
/// # Errors
///
/// Returns a typed error for an empty identity or a length above `u32`.
pub fn scheduled_sql_scenario_shard(scenario_id: &str) -> Result<u8, ScenarioShardError> {
    if scenario_id.is_empty() {
        return Err(ScenarioShardError::EmptyScenarioId);
    }
    let length =
        u32::try_from(scenario_id.len()).map_err(|_| ScenarioShardError::ScenarioIdTooLong {
            observed_bytes: scenario_id.len(),
        })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(SQL_SHARD_DOMAIN);
    hasher.update(&length.to_be_bytes());
    hasher.update(scenario_id.as_bytes());
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    let shard = u64::from_le_bytes(prefix) % u64::from(SQL_SCHEDULED_SHARD_COUNT);

    Ok(shard.to_le_bytes()[0])
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{SQL_SCHEDULED_SHARD_COUNT, ScenarioShardError, scheduled_sql_scenario_shard};

    #[test]
    fn scheduled_sharding_matches_shared_golden_assignments() {
        let assignments = ["scenario.a", "scenario.b", "scenario.c"]
            .map(|id| scheduled_sql_scenario_shard(id).expect("scenario should shard"));

        assert_eq!(SQL_SCHEDULED_SHARD_COUNT, 8);
        assert_eq!(assignments, [0, 6, 7]);
    }

    #[test]
    fn scheduled_sharding_rejects_empty_identity() {
        assert_eq!(
            scheduled_sql_scenario_shard(""),
            Err(ScenarioShardError::EmptyScenarioId)
        );
    }
}
