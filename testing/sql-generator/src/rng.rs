//! Module: sql_generator::rng
//! Responsibility: versioned SplitMix64 streams and BLAKE3 witness/repetition sub-seeds.
//! Does not own: generation choices, schema ordering, or case rendering.
//! Boundary: makes every witness repetition independent of enumeration and insertion order.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind};

const SELECT_WITNESS_SUB_SEED_DOMAIN: &[u8] = b"icydb-sql-0.215/select-witness-subseed/v3";
const MUTATION_WITNESS_SUB_SEED_DOMAIN: &[u8] = b"icydb-sql-0.215/mutation-witness-subseed/v3";
const SPLITMIX64_INCREMENT: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLITMIX64_MIX_ONE: u64 = 0xbf58_476d_1ce4_e5b9;
const SPLITMIX64_MIX_TWO: u64 = 0x94d0_49bb_1331_11eb;

/// Current hard-cut SELECT generator format and semantic version.
pub const SELECT_GENERATOR_VERSION: u32 = 1;

///
/// SplitMix64
///
/// Fixed test-owned random stream used by SQL generation and fixture creation.
/// The wrapping transition is part of the deterministic generator contract.
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
}

/// Derive one current SELECT witness/repetition stream.
///
/// # Errors
///
/// Returns a typed invalid-case error when the witness identifier cannot fit in
/// the required unsigned 32-bit length prefix.
pub(crate) fn derive_select_witness_sub_seed(
    generator_version: u32,
    root_seed: u64,
    witness_id: &str,
    repetition: u64,
) -> Result<u64, SqlGeneratorError> {
    derive_sub_seed(
        SELECT_WITNESS_SUB_SEED_DOMAIN,
        generator_version,
        root_seed,
        witness_id,
        repetition,
    )
}

/// Derive one current mutation witness/repetition stream.
///
/// # Errors
///
/// Returns a typed invalid-case error when the witness identifier cannot fit in
/// the required unsigned 32-bit length prefix.
pub(crate) fn derive_mutation_witness_sub_seed(
    generator_version: u32,
    root_seed: u64,
    witness_id: &str,
    repetition: u64,
) -> Result<u64, SqlGeneratorError> {
    derive_sub_seed(
        MUTATION_WITNESS_SUB_SEED_DOMAIN,
        generator_version,
        root_seed,
        witness_id,
        repetition,
    )
}

fn derive_sub_seed(
    domain: &[u8],
    generator_version: u32,
    root_seed: u64,
    stream_id: &str,
    ordinal: u64,
) -> Result<u64, SqlGeneratorError> {
    let witness_len = u32::try_from(stream_id.len()).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "SQL generator stream identifier exceeds the u32 length contract",
        )
    })?;
    let mut input = Vec::with_capacity(domain.len() + 24 + stream_id.len());
    input.extend_from_slice(domain);
    input.extend_from_slice(&generator_version.to_be_bytes());
    input.extend_from_slice(&root_seed.to_be_bytes());
    input.extend_from_slice(&witness_len.to_be_bytes());
    input.extend_from_slice(stream_id.as_bytes());
    input.extend_from_slice(&ordinal.to_be_bytes());
    let hash = blake3::hash(&input);
    let mut first_eight = [0_u8; 8];
    first_eight.copy_from_slice(&hash.as_bytes()[..8]);

    Ok(u64::from_le_bytes(first_eight))
}
