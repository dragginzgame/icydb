//! Module: sql_perf_receipt
//! Responsibility: deterministic P1 shard membership and receipt completeness.
//! Does not own: scenario construction, execution, P2 selection, or report rendering.
//! Boundary: turns declared and observed scenario identities into exact eight-shard evidence.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

use crate::sql_perf_profile::{PerformanceProfile, PerformanceProfileError, scenario_set_hash};

/// One complete deterministic P1 broad-scan shard receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P1ShardReceipt {
    /// Checked-in performance profile version.
    pub(crate) performance_profile_version: u32,
    /// Full expected scenario-set identity shared by every shard.
    pub(crate) expected_scenario_set_hash: String,
    /// Zero-based deterministic shard index.
    pub(crate) shard_index: u8,
    /// Total required shard count.
    pub(crate) shard_count: u8,
    /// Number of declared scenarios assigned to this shard.
    pub(crate) expected_scenario_count: usize,
    /// Number of successful or failed outcomes recorded by this shard.
    pub(crate) observed_scenario_count: usize,
    /// Number of successful outcomes recorded by this shard.
    pub(crate) successful_scenario_count: usize,
    /// Number of typed failure outcomes recorded by this shard.
    pub(crate) failed_scenario_count: usize,
    /// Canonical identity of the declared shard membership.
    pub(crate) expected_shard_hash: String,
    /// Canonical identity of the observed shard membership.
    pub(crate) observed_shard_hash: String,
    /// Whether declared and observed membership match exactly.
    pub(crate) complete: bool,
}

/// Build and validate all required P1 receipts from one complete broad scan.
///
/// # Errors
///
/// Returns a typed receipt error when the declared profile, aggregate outcomes,
/// deterministic assignment, per-shard membership, or receipt set is incomplete.
pub(crate) fn build_p1_shard_receipts(
    profile: PerformanceProfile,
    declared_ids: &[&str],
    successful_ids: &[&str],
    failed_ids: &[&str],
) -> Result<Vec<P1ShardReceipt>, P1ReceiptError> {
    profile
        .validate_scenario_set(declared_ids.iter().copied())
        .map_err(P1ReceiptError::InvalidDeclaredScenarioSet)?;
    profile
        .validate_scenario_set(
            successful_ids
                .iter()
                .copied()
                .chain(failed_ids.iter().copied()),
        )
        .map_err(P1ReceiptError::InvalidOutcomeScenarioSet)?;

    let mut receipts = Vec::with_capacity(usize::from(profile.shard_count()));
    for shard_index in 0..profile.shard_count() {
        let shard_successes = ids_for_shard(profile, shard_index, successful_ids)?;
        let shard_failures = ids_for_shard(profile, shard_index, failed_ids)?;
        receipts.push(p1_shard_receipt(
            profile,
            shard_index,
            declared_ids,
            &shard_successes,
            &shard_failures,
        )?);
    }
    validate_p1_shard_receipts(profile, &receipts)?;

    Ok(receipts)
}

/// Build one P1 shard receipt from the full declaration and this shard's outcomes.
///
/// # Errors
///
/// Returns a typed receipt error for an invalid declaration, shard index,
/// misassigned outcome, duplicate ID, or incomplete membership.
pub(crate) fn p1_shard_receipt(
    profile: PerformanceProfile,
    shard_index: u8,
    declared_ids: &[&str],
    successful_ids: &[&str],
    failed_ids: &[&str],
) -> Result<P1ShardReceipt, P1ReceiptError> {
    profile
        .validate_scenario_set(declared_ids.iter().copied())
        .map_err(P1ReceiptError::InvalidDeclaredScenarioSet)?;
    if shard_index >= profile.shard_count() {
        return Err(P1ReceiptError::InvalidShardIndex {
            shard_index,
            shard_count: profile.shard_count(),
        });
    }

    let expected_ids = ids_for_shard(profile, shard_index, declared_ids)?;
    validate_observed_assignments(profile, shard_index, successful_ids)?;
    validate_observed_assignments(profile, shard_index, failed_ids)?;
    let observed_ids = successful_ids
        .iter()
        .copied()
        .chain(failed_ids.iter().copied())
        .collect::<Vec<_>>();
    let expected_shard_hash = scenario_set_hash(expected_ids.iter().copied())
        .map_err(P1ReceiptError::InvalidDeclaredScenarioSet)?;
    let observed_shard_hash = scenario_set_hash(observed_ids.iter().copied())
        .map_err(P1ReceiptError::InvalidOutcomeScenarioSet)?;
    let expected = expected_ids.iter().copied().collect::<BTreeSet<_>>();
    let observed = observed_ids.iter().copied().collect::<BTreeSet<_>>();
    if expected != observed {
        return Err(P1ReceiptError::ScenarioMembershipMismatch {
            shard_index,
            missing: expected
                .difference(&observed)
                .map(|id| (*id).to_string())
                .collect(),
            unexpected: observed
                .difference(&expected)
                .map(|id| (*id).to_string())
                .collect(),
        });
    }

    let receipt = P1ShardReceipt {
        performance_profile_version: profile.version(),
        expected_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
        shard_index,
        shard_count: profile.shard_count(),
        expected_scenario_count: expected_ids.len(),
        observed_scenario_count: observed_ids.len(),
        successful_scenario_count: successful_ids.len(),
        failed_scenario_count: failed_ids.len(),
        expected_shard_hash,
        observed_shard_hash,
        complete: true,
    };
    validate_receipt_identity(profile, &receipt)?;

    Ok(receipt)
}

/// Validate that a P1 stage contains every required receipt exactly once.
///
/// # Errors
///
/// Returns a typed receipt error for missing, duplicate, inconsistent, or
/// aggregate-incomplete shard evidence.
pub(crate) fn validate_p1_shard_receipts(
    profile: PerformanceProfile,
    receipts: &[P1ShardReceipt],
) -> Result<(), P1ReceiptError> {
    profile.validate().map_err(P1ReceiptError::InvalidProfile)?;
    if receipts.len() != usize::from(profile.shard_count()) {
        return Err(P1ReceiptError::ReceiptCountMismatch {
            expected: profile.shard_count(),
            actual: receipts.len(),
        });
    }

    let mut by_shard = BTreeMap::new();
    for receipt in receipts {
        if by_shard.insert(receipt.shard_index, receipt).is_some() {
            return Err(P1ReceiptError::DuplicateReceipt(receipt.shard_index));
        }
    }
    let mut aggregate_count = 0_usize;
    for shard_index in 0..profile.shard_count() {
        let receipt = by_shard
            .get(&shard_index)
            .copied()
            .ok_or(P1ReceiptError::MissingReceipt(shard_index))?;
        validate_receipt_identity(profile, receipt)?;
        aggregate_count = aggregate_count
            .checked_add(receipt.observed_scenario_count)
            .ok_or(P1ReceiptError::ScenarioCountOverflow(shard_index))?;
    }
    if aggregate_count != profile.expected_scenario_count() {
        return Err(P1ReceiptError::AggregateScenarioCountMismatch {
            expected: profile.expected_scenario_count(),
            actual: aggregate_count,
        });
    }

    Ok(())
}

fn ids_for_shard<'a>(
    profile: PerformanceProfile,
    shard_index: u8,
    ids: &[&'a str],
) -> Result<Vec<&'a str>, P1ReceiptError> {
    let mut selected = Vec::new();
    for id in ids {
        let assigned = profile
            .scenario_shard(id)
            .map_err(P1ReceiptError::InvalidProfile)?;
        if assigned == shard_index {
            selected.push(*id);
        }
    }

    Ok(selected)
}

fn validate_observed_assignments(
    profile: PerformanceProfile,
    shard_index: u8,
    ids: &[&str],
) -> Result<(), P1ReceiptError> {
    for id in ids {
        let assigned = profile
            .scenario_shard(id)
            .map_err(P1ReceiptError::InvalidProfile)?;
        if assigned != shard_index {
            return Err(P1ReceiptError::ScenarioAssignedToDifferentShard {
                scenario_id: (*id).to_string(),
                expected_shard: shard_index,
                actual_shard: assigned,
            });
        }
    }

    Ok(())
}

fn validate_receipt_identity(
    profile: PerformanceProfile,
    receipt: &P1ShardReceipt,
) -> Result<(), P1ReceiptError> {
    let recorded_scenario_count = receipt
        .successful_scenario_count
        .checked_add(receipt.failed_scenario_count);
    let consistent = receipt.performance_profile_version == profile.version()
        && receipt.expected_scenario_set_hash == profile.expected_scenario_set_hash()
        && receipt.shard_index < profile.shard_count()
        && receipt.shard_count == profile.shard_count()
        && receipt.expected_scenario_count == receipt.observed_scenario_count
        && recorded_scenario_count == Some(receipt.observed_scenario_count)
        && receipt.expected_shard_hash == receipt.observed_shard_hash
        && receipt.complete;
    if !consistent {
        return Err(P1ReceiptError::InconsistentReceipt(receipt.shard_index));
    }

    Ok(())
}

/// Typed failure while constructing or merging deterministic P1 receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum P1ReceiptError {
    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),
    /// The complete declared scenario set does not match the profile.
    InvalidDeclaredScenarioSet(PerformanceProfileError),
    /// The complete observed outcome set does not match the profile.
    InvalidOutcomeScenarioSet(PerformanceProfileError),
    /// A requested shard index is outside the checked-in shard range.
    InvalidShardIndex {
        /// Requested zero-based shard index.
        shard_index: u8,
        /// Checked-in shard count.
        shard_count: u8,
    },
    /// An outcome was recorded by a shard other than its deterministic owner.
    ScenarioAssignedToDifferentShard {
        /// Misassigned scenario identity.
        scenario_id: String,
        /// Receipt shard that attempted to record the outcome.
        expected_shard: u8,
        /// Deterministic shard derived from the scenario identity.
        actual_shard: u8,
    },
    /// A shard's observed identities differ from its declared membership.
    ScenarioMembershipMismatch {
        /// Zero-based shard index.
        shard_index: u8,
        /// Declared identities without an outcome.
        missing: Vec<String>,
        /// Observed identities not declared for this shard.
        unexpected: Vec<String>,
    },
    /// The merged receipt count differs from the checked-in shard count.
    ReceiptCountMismatch {
        /// Checked-in required receipt count.
        expected: u8,
        /// Observed receipt count.
        actual: usize,
    },
    /// More than one receipt claims the same shard.
    DuplicateReceipt(u8),
    /// One required shard has no receipt.
    MissingReceipt(u8),
    /// A receipt's profile, membership, counts, hashes, or complete flag drifted.
    InconsistentReceipt(u8),
    /// Per-shard scenario counts overflowed while receipts were merged.
    ScenarioCountOverflow(u8),
    /// The merged receipts do not account for the complete profile.
    AggregateScenarioCountMismatch {
        /// Checked-in complete scenario count.
        expected: usize,
        /// Sum of observed per-shard outcomes.
        actual: usize,
    },
}

impl Display for P1ReceiptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(error) => {
                write!(formatter, "invalid performance profile: {error}")
            }
            Self::InvalidDeclaredScenarioSet(error) => {
                write!(formatter, "invalid declared P1 scenario set: {error}")
            }
            Self::InvalidOutcomeScenarioSet(error) => {
                write!(formatter, "invalid observed P1 outcome set: {error}")
            }
            Self::InvalidShardIndex {
                shard_index,
                shard_count,
            } => write!(
                formatter,
                "P1 shard index {shard_index} is outside shard count {shard_count}",
            ),
            Self::ScenarioAssignedToDifferentShard {
                scenario_id,
                expected_shard,
                actual_shard,
            } => write!(
                formatter,
                "P1 scenario {scenario_id:?} was recorded by shard {expected_shard}, but belongs to shard {actual_shard}",
            ),
            Self::ScenarioMembershipMismatch {
                shard_index,
                missing,
                unexpected,
            } => write!(
                formatter,
                "P1 shard {shard_index} membership drifted: missing {missing:?}, unexpected {unexpected:?}",
            ),
            Self::ReceiptCountMismatch { expected, actual } => write!(
                formatter,
                "P1 receipt count drifted: expected {expected}, observed {actual}",
            ),
            Self::DuplicateReceipt(shard_index) => {
                write!(formatter, "duplicate P1 receipt for shard {shard_index}")
            }
            Self::MissingReceipt(shard_index) => {
                write!(formatter, "missing P1 receipt for shard {shard_index}")
            }
            Self::InconsistentReceipt(shard_index) => {
                write!(formatter, "inconsistent P1 receipt for shard {shard_index}")
            }
            Self::ScenarioCountOverflow(shard_index) => {
                write!(
                    formatter,
                    "P1 scenario count overflowed at shard {shard_index}"
                )
            }
            Self::AggregateScenarioCountMismatch { expected, actual } => write!(
                formatter,
                "P1 receipt outcomes drifted: expected {expected}, observed {actual}",
            ),
        }
    }
}

impl Error for P1ReceiptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidProfile(error)
            | Self::InvalidDeclaredScenarioSet(error)
            | Self::InvalidOutcomeScenarioSet(error) => Some(error),
            Self::InvalidShardIndex { .. }
            | Self::ScenarioAssignedToDifferentShard { .. }
            | Self::ScenarioMembershipMismatch { .. }
            | Self::ReceiptCountMismatch { .. }
            | Self::DuplicateReceipt(_)
            | Self::MissingReceipt(_)
            | Self::InconsistentReceipt(_)
            | Self::ScenarioCountOverflow(_)
            | Self::AggregateScenarioCountMismatch { .. } => None,
        }
    }
}
