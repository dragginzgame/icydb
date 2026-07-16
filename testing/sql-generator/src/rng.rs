//! Module: sql_generator::rng
//! Responsibility: versioned SplitMix64 streams and BLAKE3 family/case sub-seeds.
//! Does not own: generation choices, schema ordering, or case rendering.
//! Boundary: makes every family and case independent of enumeration and insertion order.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind};

const SUB_SEED_DOMAIN: &[u8] = b"icydb-sql-0.204/subseed/v1";
const SPLITMIX64_INCREMENT: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLITMIX64_MIX_ONE: u64 = 0xbf58_476d_1ce4_e5b9;
const SPLITMIX64_MIX_TWO: u64 = 0x94d0_49bb_1331_11eb;

/// Current hard-cut SELECT generator format and semantic version.
pub const SELECT_GENERATOR_VERSION: u32 = 2;

///
/// SplitMix64
///
/// Fixed test-owned random stream used by SQL generation and fixture creation.
/// The implementation follows the exact wrapping transition in the 0.204 design.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Start one deterministic stream from its independently derived seed.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Advance the fixed wrapping SplitMix64 transition once.
    #[must_use]
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(SPLITMIX64_INCREMENT);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(SPLITMIX64_MIX_ONE);
        value = (value ^ (value >> 27)).wrapping_mul(SPLITMIX64_MIX_TWO);
        value ^ (value >> 31)
    }

    /// Select an unbiased value from `0..bound` using rejection sampling.
    ///
    /// # Errors
    ///
    /// Returns a typed random-choice error when `bound` is zero.
    pub fn bounded(&mut self, bound: u64) -> Result<u64, SqlGeneratorError> {
        if bound == 0 {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::RandomChoice,
                "bounded SplitMix64 choice requires a non-zero bound",
            ));
        }

        let threshold = 0_u64.wrapping_sub(bound) % bound;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return Ok(value % bound);
            }
        }
    }

    /// Select one weight index from checked cumulative half-open ranges.
    ///
    /// # Errors
    ///
    /// Returns a typed random-choice error for an empty/all-zero set or when
    /// the total weight overflows `u64`.
    pub fn weighted_index(&mut self, weights: &[u64]) -> Result<usize, SqlGeneratorError> {
        if weights.is_empty() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::RandomChoice,
                "weighted SplitMix64 choice requires at least one weight",
            ));
        }
        let total = weights.iter().try_fold(0_u64, |total, weight| {
            total.checked_add(*weight).ok_or_else(|| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::RandomChoice,
                    "weighted SplitMix64 choice overflowed its total weight",
                )
            })
        })?;
        if total == 0 {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::RandomChoice,
                "weighted SplitMix64 choice requires a non-zero total weight",
            ));
        }

        let selected = self.bounded(total)?;
        let mut cumulative = 0_u64;
        for (index, weight) in weights.iter().copied().enumerate() {
            cumulative = cumulative.checked_add(weight).ok_or_else(|| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::RandomChoice,
                    "weighted SplitMix64 cumulative range overflowed",
                )
            })?;
            if selected < cumulative {
                return Ok(index);
            }
        }

        Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::RandomChoice,
            "weighted SplitMix64 selection escaped its checked ranges",
        ))
    }
}

/// Derive one family/case stream from the exact 0.204 BLAKE3 input contract.
///
/// # Errors
///
/// Returns a typed invalid-case error when the family identifier cannot fit in
/// the required unsigned 32-bit length prefix.
pub(crate) fn derive_sql_sub_seed(
    generator_version: u32,
    root_seed: u64,
    family_id: &str,
    case_index: u64,
) -> Result<u64, SqlGeneratorError> {
    let family_len = u32::try_from(family_id.len()).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "SQL generator family identifier exceeds the u32 length contract",
        )
    })?;
    let mut input = Vec::with_capacity(SUB_SEED_DOMAIN.len() + 24 + family_id.len());
    input.extend_from_slice(SUB_SEED_DOMAIN);
    input.extend_from_slice(&generator_version.to_be_bytes());
    input.extend_from_slice(&root_seed.to_be_bytes());
    input.extend_from_slice(&family_len.to_be_bytes());
    input.extend_from_slice(family_id.as_bytes());
    input.extend_from_slice(&case_index.to_be_bytes());
    let hash = blake3::hash(&input);
    let mut first_eight = [0_u8; 8];
    first_eight.copy_from_slice(&hash.as_bytes()[..8]);

    Ok(u64::from_le_bytes(first_eight))
}
