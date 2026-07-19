//! Module: sql_perf_p2_shard
//! Responsibility: independently executable P2 confirmation shards and exact merge authority.
//! Does not own: P1 ranking, PocketIC sampling, baseline comparison, or final rendering.
//! Boundary: validates every selected candidate exactly once across eight complete shard receipts.

use crate::{
    MatrixScenario, StatementFamily, expected_phase_reconciliations, limit_stop_after_for_scenario,
    route_fact_for_scenario,
    sql_perf_environment::PerfEnvironmentIdentity,
    sql_perf_measurement::{PerformanceMeasurementCoverage, current_measurement_coverage},
    sql_perf_p2::{
        P2BaselineBasis, P2CandidateSelection, P2SelectionError, validate_p2_candidate_selection,
    },
    sql_perf_p2_confirmation::{
        P2ConfirmationError, P2ScenarioConfirmation, P2WarmEvidence, P2WarmNotApplicableReason,
        require_stable_p2_confirmation, validate_p2_confirmation,
    },
    sql_perf_phase::{PhaseOwnershipTable, current_phase_ownership},
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError, scenario_set_hash},
};

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

///
/// P2ShardReceipt
///
/// Exact expected and observed candidate membership for one P2 shard.
/// Owned by the P2 shard boundary and recomputed from every serialized report.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2ShardReceipt {
    /// Deterministic zero-based shard index.
    pub(crate) shard_index: u8,

    /// Number of selected candidates assigned to the shard.
    pub(crate) expected_candidate_count: usize,

    /// Number of retained confirmations in the shard report.
    pub(crate) observed_confirmation_count: usize,

    /// Canonical identity of selected candidate IDs assigned to the shard.
    pub(crate) expected_shard_hash: String,

    /// Canonical identity of retained confirmation IDs.
    pub(crate) observed_shard_hash: String,
}

///
/// P2ShardReport
///
/// One independently executable P2 shard with complete cold/warm confirmations.
/// Owned by the P2 shard boundary and consumed only by the exact merge authority.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct P2ShardReport {
    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Complete P1 scenario-set identity used for candidate discovery.
    p1_scenario_set_hash: String,

    /// Exact selected P2 scenario-set identity.
    p2_scenario_set_hash: String,

    /// Required canister build profile.
    canister_wasm_profile: String,

    /// Versioned phase-ownership contract used by every retained sample.
    phase_ownership: PhaseOwnershipTable,

    /// Canonical measured and explicitly unmeasured resource dimensions.
    measurement_coverage: PerformanceMeasurementCoverage,

    /// Complete environment captured by this independent P2 shard run.
    environment: PerfEnvironmentIdentity,

    /// Exact expected and observed shard membership.
    receipt: P2ShardReceipt,

    /// Complete confirmations ordered by stable scenario identity.
    confirmations: Vec<P2ScenarioConfirmation>,
}

///
/// MergedP2ShardReports
///
/// Complete stable P2 evidence produced by the sole eight-shard merge authority.
/// Owned by P2 sharding and consumed by baseline and verdict construction.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergedP2ShardReports {
    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Complete P1 scenario-set identity used for candidate discovery.
    p1_scenario_set_hash: String,

    /// Exact selected P2 scenario-set identity.
    p2_scenario_set_hash: String,

    /// Required canister build profile.
    canister_wasm_profile: String,

    /// Versioned phase-ownership contract used by every retained sample.
    phase_ownership: PhaseOwnershipTable,

    /// Canonical measured and explicitly unmeasured resource dimensions.
    measurement_coverage: PerformanceMeasurementCoverage,

    /// Exact environment inherited from candidate selection and every shard.
    pub(crate) environment: PerfEnvironmentIdentity,

    /// Comparable-baseline or explicit initial-calibration selection basis.
    pub(crate) baseline_basis: P2BaselineBasis,

    /// Exact receipts ordered by zero-based shard index.
    pub(crate) receipts: Vec<P2ShardReceipt>,

    /// Stable confirmations ordered by scenario identity.
    pub(crate) confirmations: Vec<P2ScenarioConfirmation>,
}

impl MergedP2ShardReports {
    /// Borrow the exact selected P2 scenario-set identity.
    pub(crate) fn p2_scenario_set_hash(&self) -> &str {
        &self.p2_scenario_set_hash
    }

    /// Borrow the exact baseline or calibration basis used for candidate discovery.
    pub(crate) const fn baseline_basis(&self) -> &P2BaselineBasis {
        &self.baseline_basis
    }
}

/// Build one current-format P2 shard report from complete confirmations.
///
/// # Errors
///
/// Returns a typed validation error for selection, shard, candidate, phase,
/// cache, stability-summary, or receipt drift.
pub(crate) fn build_p2_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    environment: PerfEnvironmentIdentity,
    scenarios: &[MatrixScenario],
    selection: &P2CandidateSelection,
    shard_index: u8,
    mut confirmations: Vec<P2ScenarioConfirmation>,
) -> Result<P2ShardReport, P2ShardReportValidationError> {
    confirmations
        .sort_by(|left, right| left.candidate.scenario_id.cmp(&right.candidate.scenario_id));
    let receipt = p2_shard_receipt(profile, selection, shard_index, &confirmations)?;
    let report = P2ShardReport {
        performance_profile_version: profile.version(),
        p1_scenario_set_hash: selection.p1_scenario_set_hash.clone(),
        p2_scenario_set_hash: selection.p2_scenario_set_hash.clone(),
        canister_wasm_profile: required_wasm_profile.to_string(),
        phase_ownership: current_phase_ownership(),
        measurement_coverage: current_measurement_coverage(),
        environment,
        receipt,
        confirmations,
    };
    validate_p2_shard_report(
        profile,
        required_wasm_profile,
        scenarios,
        selection,
        &report,
    )?;

    Ok(report)
}

/// Validate one P2 shard against the exact current candidate selection.
///
/// # Errors
///
/// Returns a typed error for identity, build profile, phase ownership, shard
/// membership, declaration, confirmation, or receipt drift.
pub(crate) fn validate_p2_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    selection: &P2CandidateSelection,
    report: &P2ShardReport,
) -> Result<(), P2ShardReportValidationError> {
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    validate_p2_candidate_selection(profile, &declared_ids, selection)
        .map_err(P2ShardReportValidationError::InvalidSelection)?;
    if report.performance_profile_version != profile.version() {
        return Err(P2ShardReportValidationError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.p1_scenario_set_hash != selection.p1_scenario_set_hash {
        return Err(P2ShardReportValidationError::P1ScenarioSetHash);
    }
    if report.p2_scenario_set_hash != selection.p2_scenario_set_hash {
        return Err(P2ShardReportValidationError::P2ScenarioSetHash);
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(P2ShardReportValidationError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.phase_ownership != current_phase_ownership() {
        return Err(P2ShardReportValidationError::PhaseOwnershipDrift);
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(P2ShardReportValidationError::MeasurementCoverageDrift);
    }
    if report.environment != selection.environment {
        return Err(P2ShardReportValidationError::EnvironmentDrift);
    }
    if report.receipt.shard_index >= profile.shard_count() {
        return Err(P2ShardReportValidationError::ShardOutOfRange {
            shard_index: report.receipt.shard_index,
            shard_count: profile.shard_count(),
        });
    }

    let expected_candidates = selection
        .candidates
        .iter()
        .filter(|candidate| candidate.shard_index == report.receipt.shard_index)
        .collect::<Vec<_>>();
    if expected_candidates.len() != report.confirmations.len() {
        return Err(P2ShardReportValidationError::ConfirmationCount {
            shard_index: report.receipt.shard_index,
            expected: expected_candidates.len(),
            actual: report.confirmations.len(),
        });
    }
    let declarations = scenarios
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    for (expected_candidate, confirmation) in
        expected_candidates.into_iter().zip(&report.confirmations)
    {
        if &confirmation.candidate != expected_candidate {
            return Err(P2ShardReportValidationError::CandidateDrift(
                confirmation.candidate.scenario_id.clone(),
            ));
        }
        validate_p2_confirmation(profile, confirmation).map_err(|source| {
            P2ShardReportValidationError::InvalidConfirmation {
                scenario_id: confirmation.candidate.scenario_id.clone(),
                source,
            }
        })?;
        let scenario = declarations
            .get(confirmation.candidate.scenario_id.as_str())
            .copied()
            .ok_or_else(|| {
                P2ShardReportValidationError::UnknownDeclaration(
                    confirmation.candidate.scenario_id.clone(),
                )
            })?;
        validate_confirmation_declaration(scenario, confirmation)?;
    }

    let expected_receipt = p2_shard_receipt(
        profile,
        selection,
        report.receipt.shard_index,
        &report.confirmations,
    )?;
    if report.receipt != expected_receipt {
        return Err(P2ShardReportValidationError::ReceiptDrift(
            report.receipt.shard_index,
        ));
    }

    Ok(())
}

/// Merge exactly one stable report for every deterministic P2 shard.
///
/// # Errors
///
/// Returns a typed merge error for an incomplete, duplicate, invalid,
/// unstable, or aggregate-inconsistent report set.
pub(crate) fn merge_p2_shard_reports(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    selection: &P2CandidateSelection,
    reports: Vec<P2ShardReport>,
) -> Result<MergedP2ShardReports, P2ShardMergeError> {
    if reports.len() != usize::from(profile.shard_count()) {
        return Err(P2ShardMergeError::ReportCountMismatch {
            expected: profile.shard_count(),
            actual: reports.len(),
        });
    }

    let mut by_shard = BTreeMap::new();
    for report in reports {
        let shard_index = report.receipt.shard_index;
        validate_p2_shard_report(
            profile,
            required_wasm_profile,
            scenarios,
            selection,
            &report,
        )
        .map_err(|source| P2ShardMergeError::InvalidReport {
            shard_index,
            source,
        })?;
        if by_shard.insert(shard_index, report).is_some() {
            return Err(P2ShardMergeError::DuplicateReport(shard_index));
        }
    }

    let mut receipts = Vec::with_capacity(usize::from(profile.shard_count()));
    let mut confirmations = Vec::with_capacity(selection.candidate_count);
    for shard_index in 0..profile.shard_count() {
        let report = by_shard
            .remove(&shard_index)
            .ok_or(P2ShardMergeError::MissingReport(shard_index))?;
        receipts.push(report.receipt);
        confirmations.extend(report.confirmations);
    }
    confirmations
        .sort_by(|left, right| left.candidate.scenario_id.cmp(&right.candidate.scenario_id));
    if confirmations.len() != selection.candidates.len()
        || confirmations
            .iter()
            .zip(&selection.candidates)
            .any(|(confirmation, candidate)| &confirmation.candidate != candidate)
    {
        return Err(P2ShardMergeError::AggregateCandidateDrift);
    }
    for confirmation in &confirmations {
        require_stable_p2_confirmation(confirmation)
            .map_err(P2ShardMergeError::UnstableConfirmation)?;
    }

    let merged = MergedP2ShardReports {
        performance_profile_version: profile.version(),
        p1_scenario_set_hash: selection.p1_scenario_set_hash.clone(),
        p2_scenario_set_hash: selection.p2_scenario_set_hash.clone(),
        canister_wasm_profile: required_wasm_profile.to_string(),
        phase_ownership: current_phase_ownership(),
        measurement_coverage: current_measurement_coverage(),
        environment: selection.environment.clone(),
        baseline_basis: selection.baseline_basis.clone(),
        receipts,
        confirmations,
    };
    if merged_p2_selection(&merged) != *selection {
        return Err(P2ShardMergeError::AggregateCandidateDrift);
    }
    validate_merged_p2_report(profile, required_wasm_profile, scenarios, &merged)?;

    Ok(merged)
}

/// Validate one merged P2 report independently from its source shard files.
///
/// # Errors
///
/// Returns a typed error for selection, identity, receipt, confirmation,
/// environment, or stability drift.
pub(crate) fn validate_merged_p2_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    report: &MergedP2ShardReports,
) -> Result<(), P2ShardMergeError> {
    let selection = merged_p2_selection(report);
    let declared_ids = scenarios
        .iter()
        .map(|scenario| scenario.key.as_str())
        .collect::<Vec<_>>();
    validate_p2_candidate_selection(profile, &declared_ids, &selection)
        .map_err(P2ShardMergeError::InvalidSelection)?;
    if report.performance_profile_version != profile.version() {
        return Err(P2ShardMergeError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(P2ShardMergeError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.phase_ownership != current_phase_ownership() {
        return Err(P2ShardMergeError::PhaseOwnershipDrift);
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(P2ShardMergeError::MeasurementCoverageDrift);
    }
    if report.receipts.len() != usize::from(profile.shard_count()) {
        return Err(P2ShardMergeError::ReceiptCount {
            expected: profile.shard_count(),
            actual: report.receipts.len(),
        });
    }
    for shard_index in 0..profile.shard_count() {
        let confirmations = report
            .confirmations
            .iter()
            .filter(|confirmation| confirmation.candidate.shard_index == shard_index)
            .cloned()
            .collect::<Vec<_>>();
        let shard_report = P2ShardReport {
            performance_profile_version: report.performance_profile_version,
            p1_scenario_set_hash: report.p1_scenario_set_hash.clone(),
            p2_scenario_set_hash: report.p2_scenario_set_hash.clone(),
            canister_wasm_profile: report.canister_wasm_profile.clone(),
            phase_ownership: report.phase_ownership.clone(),
            measurement_coverage: report.measurement_coverage,
            environment: report.environment.clone(),
            receipt: report.receipts[usize::from(shard_index)].clone(),
            confirmations,
        };
        validate_p2_shard_report(
            profile,
            required_wasm_profile,
            scenarios,
            &selection,
            &shard_report,
        )
        .map_err(|source| P2ShardMergeError::InvalidReport {
            shard_index,
            source,
        })?;
    }
    for confirmation in &report.confirmations {
        require_stable_p2_confirmation(confirmation)
            .map_err(P2ShardMergeError::UnstableConfirmation)?;
    }

    Ok(())
}

/// Write one independently validated merged P2 artifact.
///
/// # Errors
///
/// Returns a typed error for invalid merged evidence, encoding, size, directory,
/// or write failure.
pub(crate) fn write_merged_p2_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    report: &MergedP2ShardReports,
) -> Result<(), P2MergedArtifactError> {
    validate_merged_p2_report(profile, required_wasm_profile, scenarios, report)
        .map_err(P2MergedArtifactError::InvalidReport)?;
    let encoded =
        serde_json::to_vec_pretty(report).map_err(|source| P2MergedArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_p2_merged_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| P2MergedArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| P2MergedArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and independently validate one strict bounded merged P2 artifact.
///
/// # Errors
///
/// Returns a typed error for open, read, size, strict-decoding, or current
/// merged-evidence validation failure.
pub(crate) fn read_merged_p2_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
) -> Result<MergedP2ShardReports, P2MergedArtifactError> {
    let file = fs::File::open(path).map_err(|source| P2MergedArtifactError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| P2MergedArtifactError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_p2_merged_artifact_size(path, bytes.len(), max_bytes)?;
    let report =
        serde_json::from_slice(&bytes).map_err(|source| P2MergedArtifactError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_merged_p2_report(profile, required_wasm_profile, scenarios, &report)
        .map_err(P2MergedArtifactError::InvalidReport)?;

    Ok(report)
}

fn merged_p2_selection(report: &MergedP2ShardReports) -> P2CandidateSelection {
    P2CandidateSelection {
        performance_profile_version: report.performance_profile_version,
        p1_scenario_set_hash: report.p1_scenario_set_hash.clone(),
        p2_scenario_set_hash: report.p2_scenario_set_hash.clone(),
        environment: report.environment.clone(),
        baseline_basis: report.baseline_basis.clone(),
        candidate_count: report.confirmations.len(),
        candidates: report
            .confirmations
            .iter()
            .map(|confirmation| confirmation.candidate.clone())
            .collect(),
    }
}

fn validate_p2_merged_artifact_size(
    path: &Path,
    observed_bytes: usize,
    max_bytes: usize,
) -> Result<(), P2MergedArtifactError> {
    if observed_bytes > max_bytes {
        return Err(P2MergedArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

/// Write one validated P2 shard artifact using the strict bounded format.
///
/// # Errors
///
/// Returns a typed artifact error for invalid evidence, encoding, size-budget,
/// directory, or write failure.
pub(crate) fn write_p2_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    selection: &P2CandidateSelection,
    report: &P2ShardReport,
) -> Result<(), P2ShardArtifactError> {
    validate_p2_shard_report(profile, required_wasm_profile, scenarios, selection, report)
        .map_err(P2ShardArtifactError::InvalidReport)?;
    let encoded =
        serde_json::to_vec_pretty(report).map_err(|source| P2ShardArtifactError::Encode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_p2_shard_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| P2ShardArtifactError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| P2ShardArtifactError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one strict bounded P2 shard artifact.
///
/// # Errors
///
/// Returns a typed artifact error for open, read, size, strict-decoding, or
/// current-profile validation failure.
pub(crate) fn read_p2_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    scenarios: &[MatrixScenario],
    selection: &P2CandidateSelection,
) -> Result<P2ShardReport, P2ShardArtifactError> {
    let file = fs::File::open(path).map_err(|source| P2ShardArtifactError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| P2ShardArtifactError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_p2_shard_artifact_size(path, bytes.len(), max_bytes)?;
    let report = serde_json::from_slice(&bytes).map_err(|source| P2ShardArtifactError::Decode {
        path: path.to_path_buf(),
        source,
    })?;
    validate_p2_shard_report(
        profile,
        required_wasm_profile,
        scenarios,
        selection,
        &report,
    )
    .map_err(P2ShardArtifactError::InvalidReport)?;

    Ok(report)
}

/// Enforce the checked-in byte budget for one P2 shard artifact.
///
/// # Errors
///
/// Returns a typed oversize error when the observed artifact exceeds the limit.
pub(crate) fn validate_p2_shard_artifact_size(
    path: &Path,
    observed_bytes: usize,
    max_bytes: usize,
) -> Result<(), P2ShardArtifactError> {
    if observed_bytes > max_bytes {
        return Err(P2ShardArtifactError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

fn p2_shard_receipt(
    profile: PerformanceProfile,
    selection: &P2CandidateSelection,
    shard_index: u8,
    confirmations: &[P2ScenarioConfirmation],
) -> Result<P2ShardReceipt, P2ShardReportValidationError> {
    if shard_index >= profile.shard_count() {
        return Err(P2ShardReportValidationError::ShardOutOfRange {
            shard_index,
            shard_count: profile.shard_count(),
        });
    }
    let expected_ids = selection
        .candidates
        .iter()
        .filter(|candidate| candidate.shard_index == shard_index)
        .map(|candidate| candidate.scenario_id.as_str())
        .collect::<Vec<_>>();
    let observed_ids = confirmations
        .iter()
        .map(|confirmation| confirmation.candidate.scenario_id.as_str())
        .collect::<Vec<_>>();
    let expected_shard_hash = scenario_set_hash(expected_ids.iter().copied())
        .map_err(P2ShardReportValidationError::InvalidScenarioSet)?;
    let observed_shard_hash = scenario_set_hash(observed_ids.iter().copied())
        .map_err(P2ShardReportValidationError::InvalidScenarioSet)?;

    Ok(P2ShardReceipt {
        shard_index,
        expected_candidate_count: expected_ids.len(),
        observed_confirmation_count: observed_ids.len(),
        expected_shard_hash,
        observed_shard_hash,
    })
}

fn validate_confirmation_declaration(
    scenario: &MatrixScenario,
    confirmation: &P2ScenarioConfirmation,
) -> Result<(), P2ShardReportValidationError> {
    let warm_samples = match (scenario.metadata.statement, &confirmation.warm) {
        (StatementFamily::Select, P2WarmEvidence::Confirmed(warm)) => warm.samples.as_slice(),
        (
            StatementFamily::Describe | StatementFamily::Explain | StatementFamily::Show,
            P2WarmEvidence::NotApplicable(P2WarmNotApplicableReason::NonSelectStatement),
        ) => &[],
        _ => {
            return Err(P2ShardReportValidationError::WarmEligibilityDrift(
                scenario.key.clone(),
            ));
        }
    };
    for sample in confirmation.cold.samples.iter().chain(warm_samples) {
        if sample.surface != scenario.surface.label()
            || sample.family != scenario.family
            || sample.sql != scenario.sql
            || sample.fixture_row_count != scenario.surface.fixture_row_count()
        {
            return Err(P2ShardReportValidationError::DeclarationDrift(
                scenario.key.clone(),
            ));
        }
        let route = route_fact_for_scenario(scenario, sample);
        if sample.route_family != route.family.code()
            || sample.route_outcome != route.outcome.code()
            || sample.route_reason.as_deref() != Some(route.reason.code())
            || sample.limit_stop_after != limit_stop_after_for_scenario(scenario, sample, route)
        {
            return Err(P2ShardReportValidationError::RouteDrift(
                scenario.key.clone(),
            ));
        }
        let observed = [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ];
        if observed != expected_phase_reconciliations(sample) {
            return Err(P2ShardReportValidationError::PhaseReconciliationDrift(
                scenario.key.clone(),
            ));
        }
    }

    Ok(())
}

///
/// P2ShardReportValidationError
///
/// Typed failure when one P2 shard is not exact current confirmation evidence.
/// Owned by the P2 shard boundary and retained by artifact and merge errors.
///

#[derive(Debug)]
pub(crate) enum P2ShardReportValidationError {
    /// A retained candidate differs from the selected candidate at that position.
    CandidateDrift(String),

    /// The shard confirmation count differs from deterministic membership.
    ConfirmationCount {
        /// Deterministic shard index.
        shard_index: u8,
        /// Selected candidate count.
        expected: usize,
        /// Retained confirmation count.
        actual: usize,
    },

    /// One sample's declaration fields differ from the selected matrix scenario.
    DeclarationDrift(String),

    /// The shard was measured under a different environment than its selection.
    EnvironmentDrift,

    /// One confirmation is not valid current cold/warm evidence.
    InvalidConfirmation {
        /// Stable selected scenario identity.
        scenario_id: String,
        /// Typed confirmation cause.
        source: P2ConfirmationError,
    },

    /// A receipt scenario set cannot be encoded canonically.
    InvalidScenarioSet(PerformanceProfileError),

    /// The selected candidate artifact is not valid current profile evidence.
    InvalidSelection(P2SelectionError),

    /// The report's P1 scenario-set identity differs from selection authority.
    P1ScenarioSetHash,

    /// The report's P2 scenario-set identity differs from selection authority.
    P2ScenarioSetHash,

    /// The report's phase-ownership table differs from the current schema.
    PhaseOwnershipDrift,

    /// The report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,

    /// One sample's serialized phase reconciliation differs from raw counters.
    PhaseReconciliationDrift(String),

    /// The report names a performance profile version other than the current one.
    ProfileVersion {
        /// Current checked-in version.
        expected: u32,
        /// Serialized version.
        actual: u32,
    },

    /// The serialized receipt differs from its selected candidates and confirmations.
    ReceiptDrift(u8),

    /// One sample's route facts differ from current typed classification.
    RouteDrift(String),

    /// The report names a shard outside the checked-in range.
    ShardOutOfRange {
        /// Serialized zero-based shard index.
        shard_index: u8,
        /// Checked-in shard count.
        shard_count: u8,
    },

    /// One confirmation has no current matrix declaration.
    UnknownDeclaration(String),

    /// The report was not measured with the required canister profile.
    UnsupportedWasmProfile(String),

    /// Warm evidence disagrees with the declared statement family's eligibility.
    WarmEligibilityDrift(String),
}

impl Display for P2ShardReportValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateDrift(scenario_id) => {
                write!(formatter, "P2 candidate drifted at {scenario_id:?}")
            }
            Self::ConfirmationCount {
                shard_index,
                expected,
                actual,
            } => write!(
                formatter,
                "P2 shard {shard_index} confirmation count drifted: expected {expected}, observed {actual}",
            ),
            Self::DeclarationDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} differs from its current declaration",
            ),
            Self::EnvironmentDrift => {
                formatter.write_str("P2 shard environment differs from candidate selection")
            }
            Self::InvalidConfirmation {
                scenario_id,
                source,
            } => write!(
                formatter,
                "invalid P2 confirmation {scenario_id:?}: {source}",
            ),
            Self::InvalidScenarioSet(error) => {
                write!(formatter, "invalid P2 shard scenario set: {error}")
            }
            Self::InvalidSelection(error) => write!(formatter, "invalid P2 selection: {error}"),
            Self::P1ScenarioSetHash => {
                formatter.write_str("P2 shard P1 scenario-set identity drifted")
            }
            Self::P2ScenarioSetHash => {
                formatter.write_str("P2 shard candidate scenario-set identity drifted")
            }
            Self::PhaseOwnershipDrift => {
                formatter.write_str("P2 shard phase-ownership table drifted")
            }
            Self::MeasurementCoverageDrift => {
                formatter.write_str("P2 shard measurement coverage drifted")
            }
            Self::PhaseReconciliationDrift(scenario_id) => write!(
                formatter,
                "P2 shard phase reconciliation drifted for scenario {scenario_id:?}",
            ),
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "P2 shard profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ReceiptDrift(shard_index) => write!(
                formatter,
                "P2 shard {shard_index} receipt differs from its serialized confirmations",
            ),
            Self::RouteDrift(scenario_id) => {
                write!(formatter, "P2 scenario {scenario_id:?} route facts drifted")
            }
            Self::ShardOutOfRange {
                shard_index,
                shard_count,
            } => write!(
                formatter,
                "P2 shard index {shard_index} is outside checked-in shard count {shard_count}",
            ),
            Self::UnknownDeclaration(scenario_id) => {
                write!(formatter, "P2 scenario {scenario_id:?} has no declaration")
            }
            Self::UnsupportedWasmProfile(profile) => {
                write!(formatter, "unsupported P2 shard wasm profile {profile:?}")
            }
            Self::WarmEligibilityDrift(scenario_id) => write!(
                formatter,
                "P2 scenario {scenario_id:?} warm evidence disagrees with statement eligibility",
            ),
        }
    }
}

impl Error for P2ShardReportValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfirmation { source, .. } => Some(source),
            Self::InvalidScenarioSet(error) => Some(error),
            Self::InvalidSelection(error) => Some(error),
            Self::CandidateDrift(_)
            | Self::ConfirmationCount { .. }
            | Self::DeclarationDrift(_)
            | Self::EnvironmentDrift
            | Self::P1ScenarioSetHash
            | Self::P2ScenarioSetHash
            | Self::PhaseOwnershipDrift
            | Self::MeasurementCoverageDrift
            | Self::PhaseReconciliationDrift(_)
            | Self::ProfileVersion { .. }
            | Self::ReceiptDrift(_)
            | Self::RouteDrift(_)
            | Self::ShardOutOfRange { .. }
            | Self::UnknownDeclaration(_)
            | Self::UnsupportedWasmProfile(_)
            | Self::WarmEligibilityDrift(_) => None,
        }
    }
}

///
/// P2ShardMergeError
///
/// Typed failure while merging all independently produced P2 shards.
/// Owned by the P2 merge boundary and used as the only aggregate verdict input.
///

#[derive(Debug)]
pub(crate) enum P2ShardMergeError {
    /// Merged confirmations differ from the exact selected candidate set.
    AggregateCandidateDrift,

    /// More than one report claims the same deterministic shard.
    DuplicateReport(u8),

    /// One shard report failed current-profile validation.
    InvalidReport {
        /// Shard index claimed by the invalid report.
        shard_index: u8,
        /// Typed validation cause.
        source: P2ShardReportValidationError,
    },

    /// The selected candidate artifact is not valid current profile evidence.
    InvalidSelection(P2SelectionError),

    /// One required deterministic shard has no report.
    MissingReport(u8),

    /// The merged report's phase-ownership table differs from current authority.
    PhaseOwnershipDrift,

    /// The merged report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,

    /// The merged report names a performance profile version other than current.
    ProfileVersion {
        /// Current checked-in version.
        expected: u32,
        /// Serialized version.
        actual: u32,
    },

    /// The merged report does not retain exactly one receipt per shard.
    ReceiptCount {
        /// Required receipt count.
        expected: u8,
        /// Serialized receipt count.
        actual: usize,
    },

    /// The merge input count differs from the checked-in shard count.
    ReportCountMismatch {
        /// Required shard count.
        expected: u8,
        /// Observed report count.
        actual: usize,
    },

    /// One required cold or warm sample set is unstable.
    UnstableConfirmation(P2ConfirmationError),

    /// The merged report was not measured with the required canister profile.
    UnsupportedWasmProfile(String),
}

impl Display for P2ShardMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AggregateCandidateDrift => {
                formatter.write_str("merged P2 confirmations differ from candidate selection")
            }
            Self::DuplicateReport(shard_index) => {
                write!(formatter, "duplicate P2 shard report {shard_index}")
            }
            Self::InvalidReport {
                shard_index,
                source,
            } => write!(formatter, "invalid P2 shard report {shard_index}: {source}"),
            Self::InvalidSelection(error) => write!(formatter, "invalid P2 selection: {error}"),
            Self::MissingReport(shard_index) => {
                write!(formatter, "missing P2 shard report {shard_index}")
            }
            Self::PhaseOwnershipDrift => {
                formatter.write_str("merged P2 phase-ownership table drifted")
            }
            Self::MeasurementCoverageDrift => {
                formatter.write_str("merged P2 measurement coverage drifted")
            }
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "merged P2 profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ReceiptCount { expected, actual } => write!(
                formatter,
                "merged P2 receipt count drifted: expected {expected}, observed {actual}",
            ),
            Self::ReportCountMismatch { expected, actual } => write!(
                formatter,
                "P2 shard report count drifted: expected {expected}, observed {actual}",
            ),
            Self::UnstableConfirmation(error) => {
                write!(formatter, "unstable required P2 confirmation: {error}")
            }
            Self::UnsupportedWasmProfile(profile) => {
                write!(formatter, "unsupported merged P2 wasm profile {profile:?}")
            }
        }
    }
}

impl Error for P2ShardMergeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidReport { source, .. } => Some(source),
            Self::InvalidSelection(error) => Some(error),
            Self::UnstableConfirmation(error) => Some(error),
            Self::AggregateCandidateDrift
            | Self::DuplicateReport(_)
            | Self::MissingReport(_)
            | Self::PhaseOwnershipDrift
            | Self::MeasurementCoverageDrift
            | Self::ProfileVersion { .. }
            | Self::ReceiptCount { .. }
            | Self::ReportCountMismatch { .. }
            | Self::UnsupportedWasmProfile(_) => None,
        }
    }
}

///
/// P2ShardArtifactError
///
/// Typed failure while encoding, publishing, or reading one P2 shard artifact.
/// Owned by P2 artifact I/O and preserves validation, JSON, and filesystem causes.
///

#[derive(Debug)]
pub(crate) enum P2ShardArtifactError {
    /// The artifact is not the one current strict JSON shape.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON decoding cause.
        source: serde_json::Error,
    },

    /// The in-memory report could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// The in-memory or decoded report is not complete current-profile evidence.
    InvalidReport(P2ShardReportValidationError),

    /// One artifact filesystem operation failed.
    Io {
        /// Artifact path.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// Filesystem cause.
        source: io::Error,
    },

    /// The artifact exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Observed bytes, capped at one byte beyond the limit while reading.
        observed_bytes: usize,
        /// Checked-in maximum bytes.
        max_bytes: usize,
    },
}

impl Display for P2ShardArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { path, source } => write!(
                formatter,
                "P2 shard artifact {} could not be decoded: {source}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "P2 shard artifact {} could not be encoded: {source}",
                path.display(),
            ),
            Self::InvalidReport(error) => write!(formatter, "invalid P2 shard report: {error}"),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "P2 shard artifact {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "P2 shard artifact {} exceeds its byte budget: observed at least {observed_bytes}, maximum {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for P2ShardArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::InvalidReport(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

///
/// P2MergedArtifactError
///
/// Typed failure while reading or writing the sole merged P2 artifact.
/// Owned by merged P2 artifact I/O and preserves validation, JSON, and filesystem causes.
///

#[derive(Debug)]
pub(crate) enum P2MergedArtifactError {
    /// The artifact is not the one current strict JSON shape.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON decoding cause.
        source: serde_json::Error,
    },

    /// The in-memory report could not be encoded as current JSON.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// The in-memory or decoded report is not complete current P2 evidence.
    InvalidReport(P2ShardMergeError),

    /// One artifact filesystem operation failed.
    Io {
        /// Affected path.
        path: PathBuf,
        /// Stable operation description.
        operation: &'static str,
        /// Filesystem cause.
        source: io::Error,
    },

    /// The artifact exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Observed bytes, capped at one byte beyond the limit while reading.
        observed_bytes: usize,
        /// Checked-in maximum bytes.
        max_bytes: usize,
    },
}

impl Display for P2MergedArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode { path, source } => write!(
                formatter,
                "merged P2 artifact {} could not be decoded: {source}",
                path.display(),
            ),
            Self::Encode { path, source } => write!(
                formatter,
                "merged P2 artifact {} could not be encoded: {source}",
                path.display(),
            ),
            Self::InvalidReport(error) => write!(formatter, "invalid merged P2 report: {error}"),
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "merged P2 artifact {} could not be {operation}: {source}",
                path.display(),
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "merged P2 artifact {} exceeds its byte budget: observed at least {observed_bytes}, maximum {max_bytes}",
                path.display(),
            ),
        }
    }
}

impl Error for P2MergedArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::InvalidReport(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::TooLarge { .. } => None,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        fill_matrix_phase_reconciliation, report_matrix_sample,
        sql_perf_p2::{P2BaselineBasis, P2Candidate, P2CandidateReason, P2RawMetric},
        sql_perf_p2_confirmation::{P2SampleMode, P2WarmSampleInput, build_p2_confirmation},
        sql_perf_profile::SQL_PERFORMANCE_PROFILE,
    };

    use std::collections::BTreeSet;

    use super::*;

    fn test_selection(scenarios: &[MatrixScenario]) -> P2CandidateSelection {
        let mut reasons = scenarios
            .iter()
            .take(32)
            .map(|scenario| {
                (
                    scenario.key.clone(),
                    BTreeSet::from([P2CandidateReason::RawMetric {
                        metric: P2RawMetric::Total,
                        rank: 1,
                    }]),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for scenario_id in SQL_PERFORMANCE_PROFILE.focused_hotspot_scenario_ids() {
            reasons
                .entry((*scenario_id).to_string())
                .or_default()
                .insert(P2CandidateReason::FocusedHotspot);
        }
        for scenario_id in SQL_PERFORMANCE_PROFILE.regression_sentinel_scenario_ids() {
            reasons
                .entry((*scenario_id).to_string())
                .or_default()
                .insert(P2CandidateReason::RegressionSentinel);
        }
        for scenario_id in SQL_PERFORMANCE_PROFILE.contract_sentinel_scenario_ids() {
            reasons
                .entry((*scenario_id).to_string())
                .or_default()
                .insert(P2CandidateReason::ContractSentinel);
        }
        let candidates = reasons
            .into_iter()
            .map(|(scenario_id, reasons)| P2Candidate {
                shard_index: SQL_PERFORMANCE_PROFILE
                    .scenario_shard(&scenario_id)
                    .expect("test candidate should shard"),
                scenario_id,
                reasons: reasons.into_iter().collect(),
            })
            .collect::<Vec<_>>();
        let p2_scenario_set_hash = scenario_set_hash(
            candidates
                .iter()
                .map(|candidate| candidate.scenario_id.as_str()),
        )
        .expect("test candidate IDs should hash");

        P2CandidateSelection {
            performance_profile_version: SQL_PERFORMANCE_PROFILE.version(),
            p1_scenario_set_hash: SQL_PERFORMANCE_PROFILE
                .expected_scenario_set_hash()
                .to_string(),
            p2_scenario_set_hash,
            environment: crate::sql_perf_environment::tests::identity(),
            baseline_basis: P2BaselineBasis::comparable(
                crate::sql_perf_environment::tests::identity(),
                0,
            ),
            candidate_count: candidates.len(),
            candidates,
        }
    }

    fn test_sample(
        scenario: &MatrixScenario,
        mode: P2SampleMode,
        total: u64,
    ) -> crate::MatrixSample {
        let mut sample = report_matrix_sample(
            &scenario.key,
            scenario.surface.label(),
            total,
            100,
            &scenario.sql,
        );
        sample.family.clone_from(&scenario.family);
        sample.fixture_row_count = scenario.surface.fixture_row_count();
        sample.result_signature = Some(format!("test|{}|1", scenario.surface.table()));
        sample.order_by_idx_hint = scenario.metadata.window.order_hint.map(str::to_string);
        match mode {
            P2SampleMode::Cold => {
                sample.sql_compiled_command_hits = 0;
                sample.sql_compiled_command_misses = 1;
                sample.shared_query_plan_hits = 0;
                sample.shared_query_plan_misses = 1;
            }
            P2SampleMode::Warm => {
                sample.sql_compiled_command_hits = 1;
                sample.sql_compiled_command_misses = 0;
                sample.shared_query_plan_hits = 1;
                sample.shared_query_plan_misses = 0;
            }
        }
        let route = route_fact_for_scenario(scenario, &sample);
        sample.route_family = route.family.code().to_string();
        sample.route_outcome = route.outcome.code().to_string();
        sample.route_reason = Some(route.reason.code().to_string());
        sample.limit_stop_after = limit_stop_after_for_scenario(scenario, &sample, route);
        fill_matrix_phase_reconciliation(&mut sample);

        sample
    }

    fn test_confirmation(
        scenario: &MatrixScenario,
        candidate: P2Candidate,
        unstable: bool,
    ) -> P2ScenarioConfirmation {
        let cold = (0_u64..5)
            .map(|offset| {
                let total = if unstable && offset == 4 {
                    110_001
                } else {
                    100_000
                };
                test_sample(scenario, P2SampleMode::Cold, total)
            })
            .collect();
        let warm = match scenario.metadata.statement {
            StatementFamily::Select => P2WarmSampleInput::Required(
                (0..5)
                    .map(|_| test_sample(scenario, P2SampleMode::Warm, 80_000))
                    .collect(),
            ),
            StatementFamily::Delete
            | StatementFamily::Describe
            | StatementFamily::Explain
            | StatementFamily::Insert
            | StatementFamily::Show
            | StatementFamily::Update => {
                P2WarmSampleInput::NotApplicable(P2WarmNotApplicableReason::NonSelectStatement)
            }
        };

        build_p2_confirmation(SQL_PERFORMANCE_PROFILE, candidate, cold, warm)
            .expect("test confirmation should build")
    }

    fn test_reports(
        scenarios: &[MatrixScenario],
        selection: &P2CandidateSelection,
        unstable_scenario: Option<&str>,
    ) -> Vec<P2ShardReport> {
        let declarations = scenarios
            .iter()
            .map(|scenario| (scenario.key.as_str(), scenario))
            .collect::<BTreeMap<_, _>>();

        (0..SQL_PERFORMANCE_PROFILE.shard_count())
            .map(|shard_index| {
                let confirmations = selection
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.shard_index == shard_index)
                    .cloned()
                    .map(|candidate| {
                        let scenario = declarations
                            .get(candidate.scenario_id.as_str())
                            .copied()
                            .expect("test candidate should have a declaration");
                        let unstable = unstable_scenario == Some(candidate.scenario_id.as_str());
                        test_confirmation(scenario, candidate, unstable)
                    })
                    .collect();
                build_p2_shard_report(
                    SQL_PERFORMANCE_PROFILE,
                    "wasm-release",
                    selection.environment.clone(),
                    scenarios,
                    selection,
                    shard_index,
                    confirmations,
                )
                .expect("complete test shard should build")
            })
            .collect()
    }

    /// Build one complete stable merged P2 report for sibling unit tests.
    pub(crate) fn complete_report(scenarios: &[MatrixScenario]) -> MergedP2ShardReports {
        let selection = test_selection(scenarios);
        let reports = test_reports(scenarios, &selection, None);

        merge_p2_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            scenarios,
            &selection,
            reports,
        )
        .expect("complete test P2 evidence should merge")
    }

    #[test]
    fn p2_shards_are_strict_bounded_and_merge_exactly() {
        let scenarios = crate::deterministic_matrix();
        let selection = test_selection(&scenarios);
        let reports = test_reports(&scenarios, &selection, None);
        let merged = merge_p2_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &selection,
            reports.clone(),
        )
        .expect("all exact stable P2 shards should merge");

        assert_eq!(merged.receipts.len(), 8);
        assert_eq!(merged.confirmations.len(), selection.candidate_count);
        let merged_path =
            std::env::temp_dir().join(format!("icydb-merged-p2-{}.json", std::process::id()));
        write_merged_p2_report(
            &merged_path,
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
            &merged,
        )
        .expect("complete merged P2 report should write");
        let decoded = read_merged_p2_report(
            &merged_path,
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &scenarios,
        )
        .expect("written merged P2 report should read");
        fs::remove_file(&merged_path).expect("temporary merged P2 report should be removed");
        assert_eq!(decoded, merged);
        let mut coverage_drifted = merged.clone();
        coverage_drifted.measurement_coverage.peak_heap_bytes =
            crate::sql_perf_measurement::PerformanceMeasurementStatus::Measured;
        assert!(matches!(
            validate_merged_p2_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &coverage_drifted,
            ),
            Err(P2ShardMergeError::MeasurementCoverageDrift)
        ));
        let mut unknown_field =
            serde_json::to_value(&reports[0]).expect("current P2 shard should serialize");
        unknown_field
            .as_object_mut()
            .expect("P2 shard should be a JSON object")
            .insert("legacy_confirmations".to_string(), serde_json::json!([]));
        assert!(
            serde_json::from_value::<P2ShardReport>(unknown_field).is_err(),
            "unknown P2 shard fields must fail current-format decoding",
        );
        let mut unknown_merged =
            serde_json::to_value(&merged).expect("current merged P2 report should serialize");
        unknown_merged
            .as_object_mut()
            .expect("merged P2 report should be a JSON object")
            .insert("legacy_summary".to_string(), serde_json::json!({}));
        assert!(
            serde_json::from_value::<MergedP2ShardReports>(unknown_merged).is_err(),
            "unknown merged P2 fields must fail current-format decoding",
        );
        let max_bytes = SQL_PERFORMANCE_PROFILE.max_artifact_bytes();
        assert!(
            validate_p2_shard_artifact_size(Path::new("p2-shard.json"), max_bytes, max_bytes)
                .is_ok()
        );
        assert!(matches!(
            validate_p2_shard_artifact_size(Path::new("p2-shard.json"), max_bytes + 1, max_bytes,),
            Err(P2ShardArtifactError::TooLarge { .. })
        ));
    }

    #[test]
    fn p2_merge_rejects_missing_tampered_and_unstable_shards() {
        let scenarios = crate::deterministic_matrix();
        let selection = test_selection(&scenarios);
        let reports = test_reports(&scenarios, &selection, None);
        assert!(matches!(
            merge_p2_shard_reports(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &selection,
                reports[..7].to_vec(),
            ),
            Err(P2ShardMergeError::ReportCountMismatch {
                expected: 8,
                actual: 7,
            })
        ));

        let mut tampered = reports;
        tampered[0].receipt.observed_shard_hash = "0".repeat(64);
        assert!(matches!(
            merge_p2_shard_reports(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &selection,
                tampered,
            ),
            Err(P2ShardMergeError::InvalidReport { shard_index: 0, .. })
        ));

        let unstable_id = selection.candidates[0].scenario_id.as_str();
        let unstable = test_reports(&scenarios, &selection, Some(unstable_id));
        assert!(matches!(
            merge_p2_shard_reports(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &scenarios,
                &selection,
                unstable,
            ),
            Err(P2ShardMergeError::UnstableConfirmation(_))
        ));
    }
}
