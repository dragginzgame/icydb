//! Module: sql_perf_scale_shard
//! Responsibility: independently executable scale shards and exact merge authority.
//! Does not own: fixture generation, query execution, P1 ranking, or baseline verdicts.
//! Boundary: validates every exact-cardinality scale observation once across eight shards.

use crate::{
    MatrixScenario, ScalePayloadProfile, expected_phase_reconciliations,
    sql_perf_environment::{
        PerfEnvironmentError, PerfEnvironmentIdentity, validate_perf_environment,
    },
    sql_perf_measurement::{PerformanceMeasurementCoverage, current_measurement_coverage},
    sql_perf_p2::P2ScaleRepresentative,
    sql_perf_phase::{PhaseOwnershipTable, current_phase_ownership},
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError, scenario_set_hash},
    sql_perf_scale::{
        AdjacentScaleSlope, ScaleEvidenceError, ScaleNormalizedObservation, ScaleObservation,
        ScaleProfileError, ScaleScenarioDeclaration, adjacent_scale_slopes, scale_normalized_costs,
        scale_scenario_declarations, validate_scale_observation,
    },
};

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

///
/// ScaleShardReceipt
///
/// Exact expected and observed scale-scenario membership for one scheduled shard.
/// Owned by the scale shard boundary and recomputed from every serialized report.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleShardReceipt {
    /// Deterministic zero-based shard index.
    pub(crate) shard_index: u8,

    /// Total required shard count.
    pub(crate) shard_count: u8,

    /// Number of exact-cardinality declarations assigned to this shard.
    pub(crate) expected_observation_count: usize,

    /// Number of retained observations in this shard.
    pub(crate) observed_observation_count: usize,

    /// Canonical identity of declarations assigned to this shard.
    pub(crate) expected_shard_hash: String,

    /// Canonical identity of retained observation IDs.
    pub(crate) observed_shard_hash: String,
}

///
/// ScaleShardReport
///
/// One independently executable scale shard with complete attributed observations.
/// Owned by the scale shard boundary and consumed only by the exact merge authority.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleShardReport {
    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Complete P1 scenario-set identity that owns the cloned declarations.
    p1_scenario_set_hash: String,

    /// Exact scale scenario-set identity shared by all shards.
    scale_scenario_set_hash: String,

    /// Required canister build profile.
    canister_wasm_profile: String,

    /// Versioned phase-ownership contract used by every retained sample.
    phase_ownership: PhaseOwnershipTable,

    /// Canonical measured and explicitly unmeasured resource dimensions.
    measurement_coverage: PerformanceMeasurementCoverage,

    /// Complete comparable environment and measured subject identity.
    environment: PerfEnvironmentIdentity,

    /// Exact expected and observed shard membership.
    receipt: ScaleShardReceipt,

    /// Complete observations ordered by stable exact-cardinality scenario identity.
    observations: Vec<ScaleObservation>,
}

///
/// MergedScaleShardReports
///
/// Complete scale evidence produced by the sole eight-shard merge authority.
/// Owned by scale sharding and consumed by P2 selection and baseline construction.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergedScaleShardReports {
    /// Checked-in performance profile version.
    performance_profile_version: u32,

    /// Complete P1 scenario-set identity that owns the cloned declarations.
    p1_scenario_set_hash: String,

    /// Exact scale scenario-set identity.
    scale_scenario_set_hash: String,

    /// Required canister build profile.
    canister_wasm_profile: String,

    /// Versioned phase-ownership contract used by every retained sample.
    phase_ownership: PhaseOwnershipTable,

    /// Canonical measured and explicitly unmeasured resource dimensions.
    measurement_coverage: PerformanceMeasurementCoverage,

    /// Exact environment shared by every independently produced shard.
    pub(crate) environment: PerfEnvironmentIdentity,

    /// Exact receipts ordered by zero-based shard index.
    pub(crate) receipts: Vec<ScaleShardReceipt>,

    /// Every exact-cardinality observation ordered by stable scenario identity.
    pub(crate) observations: Vec<ScaleObservation>,

    /// Exact adjacent-cardinality slopes ordered by sentinel identity.
    pub(crate) slopes: Vec<AdjacentScaleSlope>,

    /// Every eligible exact nonzero normalized-cost projection.
    pub(crate) normalized_costs: Vec<ScaleNormalizedObservation>,

    /// Worst P1 representative for every reviewed scale stratum.
    pub(crate) p2_representatives: Vec<P2ScaleRepresentative>,
}

/// Build one current-format scale shard report from complete observations.
///
/// # Errors
///
/// Returns a typed error for profile, declaration, shard, membership, phase,
/// fixture, result, route, or receipt drift.
pub(crate) fn build_scale_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    environment: PerfEnvironmentIdentity,
    p1_scenarios: &[MatrixScenario],
    shard_index: u8,
    mut observations: Vec<ScaleObservation>,
) -> Result<ScaleShardReport, ScaleShardError> {
    observations.sort_by(|left, right| left.scenario_id.cmp(&right.scenario_id));
    let declarations = scale_scenario_declarations(profile, p1_scenarios)
        .map_err(ScaleShardError::InvalidScaleProfile)?;
    let scale_scenario_set_hash = scale_declaration_hash(&declarations)?;
    let receipt = scale_shard_receipt(profile, &declarations, shard_index, &observations)?;
    let report = ScaleShardReport {
        performance_profile_version: profile.version(),
        p1_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
        scale_scenario_set_hash,
        canister_wasm_profile: required_wasm_profile.to_string(),
        phase_ownership: current_phase_ownership(),
        measurement_coverage: current_measurement_coverage(),
        environment,
        receipt,
        observations,
    };
    validate_scale_shard_report(profile, required_wasm_profile, p1_scenarios, &report)?;

    Ok(report)
}

/// Validate one scale shard against the exact current declaration set.
///
/// # Errors
///
/// Returns a typed error for identity, build profile, phase ownership, shard
/// membership, declaration, evidence, reconciliation, or receipt drift.
pub(crate) fn validate_scale_shard_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
    report: &ScaleShardReport,
) -> Result<(), ScaleShardError> {
    let declarations = scale_scenario_declarations(profile, p1_scenarios)
        .map_err(ScaleShardError::InvalidScaleProfile)?;
    let shard_index =
        validate_scale_shard_identity(profile, required_wasm_profile, &declarations, report)?;

    let mut expected = BTreeMap::new();
    for declaration in &declarations {
        let assigned = profile
            .scenario_shard(&declaration.scenario.key)
            .map_err(ScaleShardError::InvalidScenarioSet)?;
        if assigned == shard_index {
            expected.insert(declaration.scenario.key.as_str(), declaration);
        }
    }
    if expected.len() != report.observations.len() {
        return Err(ScaleShardError::ObservationCount {
            shard_index,
            expected: expected.len(),
            actual: report.observations.len(),
        });
    }
    let observed_ids = report
        .observations
        .iter()
        .map(|observation| observation.scenario_id.as_str())
        .collect::<Vec<_>>();
    scenario_set_hash(observed_ids.iter().copied()).map_err(ScaleShardError::InvalidScenarioSet)?;
    let observed = observed_ids.iter().copied().collect::<BTreeSet<_>>();
    let expected_ids = expected.keys().copied().collect::<BTreeSet<_>>();
    if observed != expected_ids {
        return Err(ScaleShardError::MembershipDrift {
            shard_index,
            missing: expected_ids
                .difference(&observed)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
            unexpected: observed
                .difference(&expected_ids)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
        });
    }
    for observation in &report.observations {
        let declaration = expected
            .get(observation.scenario_id.as_str())
            .copied()
            .ok_or_else(|| ScaleShardError::UnknownDeclaration(observation.scenario_id.clone()))?;
        validate_scale_observation(declaration, observation).map_err(|source| {
            ScaleShardError::InvalidObservation {
                scenario_id: observation.scenario_id.clone(),
                source,
            }
        })?;
        let sample = &observation.sample;
        let observed_reconciliation = [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ];
        if observed_reconciliation != expected_phase_reconciliations(sample) {
            return Err(ScaleShardError::PhaseReconciliationDrift(
                observation.scenario_id.clone(),
            ));
        }
    }

    let expected_receipt =
        scale_shard_receipt(profile, &declarations, shard_index, &report.observations)?;
    if report.receipt != expected_receipt {
        return Err(ScaleShardError::ReceiptDrift(shard_index));
    }

    Ok(())
}

fn validate_scale_shard_identity(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declarations: &[ScaleScenarioDeclaration],
    report: &ScaleShardReport,
) -> Result<u8, ScaleShardError> {
    if report.performance_profile_version != profile.version() {
        return Err(ScaleShardError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.p1_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(ScaleShardError::P1ScenarioSetHash);
    }
    let expected_scale_hash = scale_declaration_hash(declarations)?;
    if report.scale_scenario_set_hash != expected_scale_hash {
        return Err(ScaleShardError::ScaleScenarioSetHash {
            expected: expected_scale_hash,
            actual: report.scale_scenario_set_hash.clone(),
        });
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(ScaleShardError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.phase_ownership != current_phase_ownership() {
        return Err(ScaleShardError::PhaseOwnershipDrift);
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(ScaleShardError::MeasurementCoverageDrift);
    }
    validate_perf_environment(profile, &report.environment)
        .map_err(ScaleShardError::InvalidEnvironment)?;
    let shard_index = report.receipt.shard_index;
    if shard_index >= profile.shard_count() {
        return Err(ScaleShardError::ShardOutOfRange {
            shard_index,
            shard_count: profile.shard_count(),
        });
    }

    Ok(shard_index)
}

/// Merge exactly one complete report for every deterministic scale shard.
///
/// # Errors
///
/// Returns a typed error for incomplete, duplicate, invalid, or aggregate-
/// inconsistent reports, or for incomplete adjacent-cardinality evidence.
pub(crate) fn merge_scale_shard_reports(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
    reports: Vec<ScaleShardReport>,
) -> Result<MergedScaleShardReports, ScaleShardError> {
    if reports.len() != usize::from(profile.shard_count()) {
        return Err(ScaleShardError::ReportCount {
            expected: profile.shard_count(),
            actual: reports.len(),
        });
    }
    let mut by_shard = BTreeMap::new();
    let mut environment = None;
    for report in reports {
        let shard_index = report.receipt.shard_index;
        validate_scale_shard_report(profile, required_wasm_profile, p1_scenarios, &report)?;
        if environment
            .as_ref()
            .is_some_and(|expected| expected != &report.environment)
        {
            return Err(ScaleShardError::EnvironmentDrift(shard_index));
        }
        environment.get_or_insert_with(|| report.environment.clone());
        if by_shard.insert(shard_index, report).is_some() {
            return Err(ScaleShardError::DuplicateReport(shard_index));
        }
    }

    let mut receipts = Vec::with_capacity(usize::from(profile.shard_count()));
    let mut observations = Vec::new();
    for shard_index in 0..profile.shard_count() {
        let report = by_shard
            .remove(&shard_index)
            .ok_or(ScaleShardError::MissingReport(shard_index))?;
        receipts.push(report.receipt);
        observations.extend(report.observations);
    }
    observations.sort_by(|left, right| left.scenario_id.cmp(&right.scenario_id));
    let declarations = scale_scenario_declarations(profile, p1_scenarios)
        .map_err(ScaleShardError::InvalidScaleProfile)?;
    if observations.len() != declarations.len() {
        return Err(ScaleShardError::AggregateObservationCount {
            expected: declarations.len(),
            actual: observations.len(),
        });
    }
    let slopes = adjacent_scale_slopes(profile.scale_row_cardinalities(), &observations)
        .map_err(ScaleShardError::InvalidScaleEvidence)?;
    let normalized_costs = scale_normalized_costs(&observations);
    let p2_representatives = scale_p2_representatives(&observations);

    Ok(MergedScaleShardReports {
        performance_profile_version: profile.version(),
        p1_scenario_set_hash: profile.expected_scenario_set_hash().to_string(),
        scale_scenario_set_hash: scale_declaration_hash(&declarations)?,
        canister_wasm_profile: required_wasm_profile.to_string(),
        phase_ownership: current_phase_ownership(),
        measurement_coverage: current_measurement_coverage(),
        environment: environment.ok_or(ScaleShardError::MissingEnvironment)?,
        receipts,
        observations,
        slopes,
        normalized_costs,
        p2_representatives,
    })
}

/// Validate one merged scale report against current declarations and derived projections.
///
/// # Errors
///
/// Returns a typed error for identity, receipt, observation, phase, normalized,
/// slope, or P2-representative drift.
pub(crate) fn validate_merged_scale_report(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    let declarations = scale_scenario_declarations(profile, p1_scenarios)
        .map_err(ScaleShardError::InvalidScaleProfile)?;
    validate_merged_scale_identity(profile, required_wasm_profile, &declarations, report)?;
    validate_merged_scale_observations(&declarations, report)?;
    validate_merged_scale_receipts(profile, &declarations, report)?;
    validate_merged_scale_projections(profile, report)?;

    Ok(())
}

fn validate_merged_scale_identity(
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    declarations: &[ScaleScenarioDeclaration],
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    if report.performance_profile_version != profile.version() {
        return Err(ScaleShardError::ProfileVersion {
            expected: profile.version(),
            actual: report.performance_profile_version,
        });
    }
    if report.p1_scenario_set_hash != profile.expected_scenario_set_hash() {
        return Err(ScaleShardError::P1ScenarioSetHash);
    }
    let expected_scale_hash = scale_declaration_hash(declarations)?;
    if report.scale_scenario_set_hash != expected_scale_hash {
        return Err(ScaleShardError::ScaleScenarioSetHash {
            expected: expected_scale_hash,
            actual: report.scale_scenario_set_hash.clone(),
        });
    }
    if report.canister_wasm_profile != required_wasm_profile {
        return Err(ScaleShardError::UnsupportedWasmProfile(
            report.canister_wasm_profile.clone(),
        ));
    }
    if report.phase_ownership != current_phase_ownership() {
        return Err(ScaleShardError::PhaseOwnershipDrift);
    }
    if report.measurement_coverage != current_measurement_coverage() {
        return Err(ScaleShardError::MeasurementCoverageDrift);
    }
    validate_perf_environment(profile, &report.environment)
        .map_err(ScaleShardError::InvalidEnvironment)?;
    if report.observations.len() != declarations.len() {
        return Err(ScaleShardError::AggregateObservationCount {
            expected: declarations.len(),
            actual: report.observations.len(),
        });
    }

    Ok(())
}

fn validate_merged_scale_observations(
    declarations: &[ScaleScenarioDeclaration],
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    let declarations_by_id = declarations
        .iter()
        .map(|declaration| (declaration.scenario.key.as_str(), declaration))
        .collect::<BTreeMap<_, _>>();
    let observed_ids = report
        .observations
        .iter()
        .map(|observation| observation.scenario_id.as_str())
        .collect::<Vec<_>>();
    let observed_hash = scenario_set_hash(observed_ids.iter().copied())
        .map_err(ScaleShardError::InvalidScenarioSet)?;
    if observed_hash != report.scale_scenario_set_hash {
        return Err(ScaleShardError::ScaleScenarioSetHash {
            expected: report.scale_scenario_set_hash.clone(),
            actual: observed_hash,
        });
    }
    for observation in &report.observations {
        let declaration = declarations_by_id
            .get(observation.scenario_id.as_str())
            .copied()
            .ok_or_else(|| ScaleShardError::UnknownDeclaration(observation.scenario_id.clone()))?;
        validate_scale_observation(declaration, observation).map_err(|source| {
            ScaleShardError::InvalidObservation {
                scenario_id: observation.scenario_id.clone(),
                source,
            }
        })?;
        let sample = &observation.sample;
        if [
            sample.total_phase_reconciliation,
            sample.compile_phase_reconciliation,
            sample.execute_phase_reconciliation,
            sample.planner_phase_reconciliation,
            sample.executor_invocation_phase_reconciliation,
        ] != expected_phase_reconciliations(sample)
        {
            return Err(ScaleShardError::PhaseReconciliationDrift(
                observation.scenario_id.clone(),
            ));
        }
    }

    Ok(())
}

fn validate_merged_scale_receipts(
    profile: PerformanceProfile,
    declarations: &[ScaleScenarioDeclaration],
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    if report.receipts.len() != usize::from(profile.shard_count()) {
        return Err(ScaleShardError::ReceiptCount {
            expected: profile.shard_count(),
            actual: report.receipts.len(),
        });
    }
    for shard_index in 0..profile.shard_count() {
        let mut observations = Vec::new();
        for observation in &report.observations {
            let assigned = profile
                .scenario_shard(&observation.scenario_id)
                .map_err(ScaleShardError::InvalidScenarioSet)?;
            if assigned == shard_index {
                observations.push(observation.clone());
            }
        }
        let expected_receipt =
            scale_shard_receipt(profile, declarations, shard_index, &observations)?;
        if report.receipts.get(usize::from(shard_index)) != Some(&expected_receipt) {
            return Err(ScaleShardError::ReceiptDrift(shard_index));
        }
    }

    Ok(())
}

fn validate_merged_scale_projections(
    profile: PerformanceProfile,
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    let expected_slopes =
        adjacent_scale_slopes(profile.scale_row_cardinalities(), &report.observations)
            .map_err(ScaleShardError::InvalidScaleEvidence)?;
    if report.slopes != expected_slopes {
        return Err(ScaleShardError::MergedProjectionDrift("adjacent slopes"));
    }
    if report.normalized_costs != scale_normalized_costs(&report.observations) {
        return Err(ScaleShardError::MergedProjectionDrift("normalized costs"));
    }
    if report.p2_representatives != scale_p2_representatives(&report.observations) {
        return Err(ScaleShardError::MergedProjectionDrift(
            "P2 scale representatives",
        ));
    }

    Ok(())
}

/// Write one validated strict bounded merged scale artifact.
///
/// # Errors
///
/// Returns a typed error for invalid evidence, encoding, size, directory, or write failure.
pub(crate) fn write_merged_scale_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
    report: &MergedScaleShardReports,
) -> Result<(), ScaleShardError> {
    validate_merged_scale_report(profile, required_wasm_profile, p1_scenarios, report)?;
    let encoded = serde_json::to_vec_pretty(report).map_err(|source| ScaleShardError::Encode {
        path: path.to_path_buf(),
        source,
    })?;
    validate_scale_shard_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ScaleShardError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| ScaleShardError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one strict bounded merged scale artifact.
///
/// # Errors
///
/// Returns a typed error for open, read, size, strict decode, or validation failure.
pub(crate) fn read_merged_scale_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
) -> Result<MergedScaleShardReports, ScaleShardError> {
    let file = fs::File::open(path).map_err(|source| ScaleShardError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| ScaleShardError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_scale_shard_artifact_size(path, bytes.len(), max_bytes)?;
    let report: MergedScaleShardReports =
        serde_json::from_slice(&bytes).map_err(|source| ScaleShardError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
    validate_merged_scale_report(profile, required_wasm_profile, p1_scenarios, &report)?;

    Ok(report)
}

/// Write one validated strict bounded scale shard artifact.
///
/// # Errors
///
/// Returns a typed error for invalid evidence, encoding, size, directory, or write failure.
pub(crate) fn write_scale_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
    report: &ScaleShardReport,
) -> Result<(), ScaleShardError> {
    validate_scale_shard_report(profile, required_wasm_profile, p1_scenarios, report)?;
    let encoded = serde_json::to_vec_pretty(report).map_err(|source| ScaleShardError::Encode {
        path: path.to_path_buf(),
        source,
    })?;
    validate_scale_shard_artifact_size(path, encoded.len(), profile.max_artifact_bytes())?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ScaleShardError::Io {
            path: parent.to_path_buf(),
            operation: "prepared",
            source,
        })?;
    }
    fs::write(path, encoded).map_err(|source| ScaleShardError::Io {
        path: path.to_path_buf(),
        operation: "written",
        source,
    })
}

/// Read and validate one strict bounded scale shard artifact.
///
/// # Errors
///
/// Returns a typed error for open, read, size, strict decode, or validation failure.
pub(crate) fn read_scale_shard_report(
    path: &Path,
    profile: PerformanceProfile,
    required_wasm_profile: &str,
    p1_scenarios: &[MatrixScenario],
) -> Result<ScaleShardReport, ScaleShardError> {
    let file = fs::File::open(path).map_err(|source| ScaleShardError::Io {
        path: path.to_path_buf(),
        operation: "opened",
        source,
    })?;
    let max_bytes = profile.max_artifact_bytes();
    let read_limit = u64::try_from(max_bytes).map_or(u64::MAX, |maximum| maximum.saturating_add(1));
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| ScaleShardError::Io {
            path: path.to_path_buf(),
            operation: "read",
            source,
        })?;
    validate_scale_shard_artifact_size(path, bytes.len(), max_bytes)?;
    let report = serde_json::from_slice(&bytes).map_err(|source| ScaleShardError::Decode {
        path: path.to_path_buf(),
        source,
    })?;
    validate_scale_shard_report(profile, required_wasm_profile, p1_scenarios, &report)?;

    Ok(report)
}

/// Enforce the checked-in byte budget for one scale shard artifact.
///
/// # Errors
///
/// Returns a typed oversize error when the observed artifact exceeds the limit.
pub(crate) fn validate_scale_shard_artifact_size(
    path: &Path,
    observed_bytes: usize,
    max_bytes: usize,
) -> Result<(), ScaleShardError> {
    if observed_bytes > max_bytes {
        return Err(ScaleShardError::TooLarge {
            path: path.to_path_buf(),
            observed_bytes,
            max_bytes,
        });
    }

    Ok(())
}

fn scale_declaration_hash(
    declarations: &[ScaleScenarioDeclaration],
) -> Result<String, ScaleShardError> {
    scenario_set_hash(
        declarations
            .iter()
            .map(|declaration| declaration.scenario.key.as_str()),
    )
    .map_err(ScaleShardError::InvalidScenarioSet)
}

fn scale_shard_receipt(
    profile: PerformanceProfile,
    declarations: &[ScaleScenarioDeclaration],
    shard_index: u8,
    observations: &[ScaleObservation],
) -> Result<ScaleShardReceipt, ScaleShardError> {
    if shard_index >= profile.shard_count() {
        return Err(ScaleShardError::ShardOutOfRange {
            shard_index,
            shard_count: profile.shard_count(),
        });
    }
    let mut expected_ids = Vec::new();
    for declaration in declarations {
        if profile
            .scenario_shard(&declaration.scenario.key)
            .map_err(ScaleShardError::InvalidScenarioSet)?
            == shard_index
        {
            expected_ids.push(declaration.scenario.key.as_str());
        }
    }
    let observed_ids = observations
        .iter()
        .map(|observation| observation.scenario_id.as_str())
        .collect::<Vec<_>>();

    Ok(ScaleShardReceipt {
        shard_index,
        shard_count: profile.shard_count(),
        expected_observation_count: expected_ids.len(),
        observed_observation_count: observed_ids.len(),
        expected_shard_hash: scenario_set_hash(expected_ids.iter().copied())
            .map_err(ScaleShardError::InvalidScenarioSet)?,
        observed_shard_hash: scenario_set_hash(observed_ids.iter().copied())
            .map_err(ScaleShardError::InvalidScenarioSet)?,
    })
}

fn scale_p2_representatives(observations: &[ScaleObservation]) -> Vec<P2ScaleRepresentative> {
    let mut by_stratum = BTreeMap::<String, (u64, &str)>::new();
    for observation in observations {
        let payload = match observation.fixture.payload_profile {
            ScalePayloadProfile::NotApplicable => "not_applicable",
            ScalePayloadProfile::BlobCycleV1 => "blob_cycle_v1",
        };
        let window = observation
            .result_window
            .map_or_else(|| "none".to_string(), |window| window.to_string());
        let strata = [
            format!("route/{}", observation.sample.route_family),
            format!("selectivity/{}", observation.selectivity.code()),
            format!("window/{window}"),
            format!("surface/{}", observation.sample.surface),
            format!("payload/{payload}"),
            format!("sentinel/{}", observation.sentinel_id),
        ];
        for stratum in strata {
            let candidate = (
                observation.sample.total_local_instructions,
                observation.p1_scenario_id.as_str(),
            );
            by_stratum
                .entry(stratum)
                .and_modify(|selected| {
                    if candidate.0 > selected.0
                        || (candidate.0 == selected.0 && candidate.1 < selected.1)
                    {
                        *selected = candidate;
                    }
                })
                .or_insert(candidate);
        }
    }

    by_stratum
        .into_iter()
        .map(|(stratum, (_, scenario_id))| P2ScaleRepresentative {
            scenario_id: scenario_id.to_string(),
            stratum,
        })
        .collect()
}

///
/// ScaleShardError
///
/// Typed failure at scale report construction, validation, merge, or artifact boundaries.
/// Owned by scale sharding and preserves underlying profile, evidence, JSON, and I/O causes.
///

#[derive(Debug)]
pub(crate) enum ScaleShardError {
    /// The merged observation count differs from the exact declaration count.
    AggregateObservationCount {
        /// Exact declaration count.
        expected: usize,
        /// Merged observation count.
        actual: usize,
    },

    /// A strict JSON artifact could not be decoded.
    Decode {
        /// Artifact path.
        path: PathBuf,
        /// JSON decoding cause.
        source: serde_json::Error,
    },

    /// More than one report claims the same shard index.
    DuplicateReport(u8),

    /// A strict JSON artifact could not be encoded.
    Encode {
        /// Artifact path.
        path: PathBuf,
        /// JSON encoding cause.
        source: serde_json::Error,
    },

    /// One retained observation failed current declaration/evidence validation.
    InvalidObservation {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Typed evidence cause.
        source: ScaleEvidenceError,
    },

    /// The report's captured performance environment is invalid.
    InvalidEnvironment(PerfEnvironmentError),

    /// The checked-in or observed scenario identity set is invalid.
    InvalidScenarioSet(PerformanceProfileError),

    /// Complete merged scale evidence cannot derive required slopes.
    InvalidScaleEvidence(ScaleEvidenceError),

    /// Scale declarations cannot be materialized from current P1 authority.
    InvalidScaleProfile(ScaleProfileError),

    /// A filesystem operation failed.
    Io {
        /// Affected path.
        path: PathBuf,
        /// Human-readable operation.
        operation: &'static str,
        /// I/O cause.
        source: io::Error,
    },

    /// One shard's observed IDs differ from its deterministic assignment.
    MembershipDrift {
        /// Deterministic shard index.
        shard_index: u8,
        /// Missing assigned scenario IDs.
        missing: Vec<String>,
        /// Unexpected scenario IDs.
        unexpected: Vec<String>,
    },

    /// The report's measured/unmeasured resource table differs from current authority.
    MeasurementCoverageDrift,

    /// One of the required eight shard reports is absent.
    MissingReport(u8),

    /// The merge received no report from which to retain environment identity.
    MissingEnvironment,

    /// A merged derived projection differs from its retained observations.
    MergedProjectionDrift(&'static str),

    /// One shard has a different number of observations than declarations.
    ObservationCount {
        /// Deterministic shard index.
        shard_index: u8,
        /// Assigned declaration count.
        expected: usize,
        /// Retained observation count.
        actual: usize,
    },

    /// The report names a different P1 scenario-set authority.
    P1ScenarioSetHash,

    /// One sample's serialized phase reconciliation differs from its raw counters.
    PhaseReconciliationDrift(String),

    /// The report's phase-ownership table differs from the current schema.
    PhaseOwnershipDrift,

    /// The report names a performance profile version other than the current one.
    ProfileVersion {
        /// Current checked-in version.
        expected: u32,
        /// Reported version.
        actual: u32,
    },

    /// Serialized receipt fields differ from receipt facts rederived from observations.
    ReceiptDrift(u8),

    /// A merged report does not retain exactly one receipt per required shard.
    ReceiptCount {
        /// Required shard count.
        expected: u8,
        /// Retained receipt count.
        actual: usize,
    },

    /// The merge did not receive exactly one report for every scheduled shard.
    ReportCount {
        /// Required shard count.
        expected: u8,
        /// Supplied report count.
        actual: usize,
    },

    /// One independently produced shard has a different environment identity.
    EnvironmentDrift(u8),

    /// The report's exact scale scenario-set identity drifted.
    ScaleScenarioSetHash {
        /// Current declaration hash.
        expected: String,
        /// Reported hash.
        actual: String,
    },

    /// One report names a shard index outside the fixed range.
    ShardOutOfRange {
        /// Reported zero-based shard index.
        shard_index: u8,
        /// Required shard count.
        shard_count: u8,
    },

    /// One artifact exceeds the checked-in byte budget.
    TooLarge {
        /// Artifact path.
        path: PathBuf,
        /// Observed encoded byte count.
        observed_bytes: usize,
        /// Maximum allowed encoded byte count.
        max_bytes: usize,
    },

    /// One retained observation has no current declaration.
    UnknownDeclaration(String),

    /// The report was not measured with the required canister profile.
    UnsupportedWasmProfile(String),
}

impl Display for ScaleShardError {
    #[expect(
        clippy::too_many_lines,
        reason = "the exhaustive typed artifact error formatter keeps every retained cause visible at one boundary"
    )]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AggregateObservationCount { expected, actual } => write!(
                formatter,
                "merged scale observation count drifted: expected {expected}, observed {actual}",
            ),
            Self::Decode { path, source } => {
                write!(
                    formatter,
                    "scale artifact {} could not be decoded: {source}",
                    path.display()
                )
            }
            Self::DuplicateReport(shard_index) => {
                write!(formatter, "duplicate scale shard report {shard_index}")
            }
            Self::Encode { path, source } => {
                write!(
                    formatter,
                    "scale artifact {} could not be encoded: {source}",
                    path.display()
                )
            }
            Self::InvalidObservation {
                scenario_id,
                source,
            } => write!(
                formatter,
                "scale observation {scenario_id:?} is invalid: {source}"
            ),
            Self::InvalidEnvironment(source) => {
                write!(formatter, "invalid scale performance environment: {source}")
            }
            Self::InvalidScenarioSet(source) => {
                write!(formatter, "invalid scale scenario set: {source}")
            }
            Self::InvalidScaleEvidence(source) => {
                write!(formatter, "invalid merged scale evidence: {source}")
            }
            Self::InvalidScaleProfile(source) => {
                write!(formatter, "invalid scale profile: {source}")
            }
            Self::Io {
                path,
                operation,
                source,
            } => write!(
                formatter,
                "scale artifact {} could not be {operation}: {source}",
                path.display()
            ),
            Self::MembershipDrift {
                shard_index,
                missing,
                unexpected,
            } => write!(
                formatter,
                "scale shard {shard_index} membership drifted: missing {missing:?}, unexpected {unexpected:?}",
            ),
            Self::MeasurementCoverageDrift => {
                formatter.write_str("scale measurement coverage drifted")
            }
            Self::MissingReport(shard_index) => {
                write!(formatter, "missing scale shard report {shard_index}")
            }
            Self::MissingEnvironment => {
                formatter.write_str("scale merge received no performance environment")
            }
            Self::MergedProjectionDrift(projection) => {
                write!(formatter, "merged scale {projection} drifted")
            }
            Self::ObservationCount {
                shard_index,
                expected,
                actual,
            } => write!(
                formatter,
                "scale shard {shard_index} observation count drifted: expected {expected}, observed {actual}",
            ),
            Self::P1ScenarioSetHash => {
                formatter.write_str("scale report P1 scenario-set hash drifted")
            }
            Self::PhaseReconciliationDrift(scenario_id) => write!(
                formatter,
                "scale sample {scenario_id:?} phase reconciliation drifted",
            ),
            Self::PhaseOwnershipDrift => formatter.write_str("scale phase ownership drifted"),
            Self::ProfileVersion { expected, actual } => write!(
                formatter,
                "scale profile version drifted: expected {expected}, observed {actual}",
            ),
            Self::ReceiptDrift(shard_index) => {
                write!(formatter, "scale shard {shard_index} receipt drifted")
            }
            Self::ReceiptCount { expected, actual } => write!(
                formatter,
                "merged scale receipt count drifted: expected {expected}, observed {actual}",
            ),
            Self::ReportCount { expected, actual } => write!(
                formatter,
                "scale merge requires {expected} reports, observed {actual}",
            ),
            Self::EnvironmentDrift(shard_index) => write!(
                formatter,
                "scale shard {shard_index} performance environment drifted",
            ),
            Self::ScaleScenarioSetHash { expected, actual } => write!(
                formatter,
                "scale scenario-set hash drifted: expected {expected}, observed {actual}",
            ),
            Self::ShardOutOfRange {
                shard_index,
                shard_count,
            } => write!(
                formatter,
                "scale shard {shard_index} is outside zero..{shard_count}",
            ),
            Self::TooLarge {
                path,
                observed_bytes,
                max_bytes,
            } => write!(
                formatter,
                "scale artifact {} is {observed_bytes} bytes; maximum is {max_bytes}",
                path.display(),
            ),
            Self::UnknownDeclaration(scenario_id) => {
                write!(
                    formatter,
                    "scale observation {scenario_id:?} has no declaration"
                )
            }
            Self::UnsupportedWasmProfile(profile) => {
                write!(formatter, "unsupported scale wasm profile {profile:?}")
            }
        }
    }
}

impl Error for ScaleShardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source, .. } | Self::Encode { source, .. } => Some(source),
            Self::InvalidObservation { source, .. } | Self::InvalidScaleEvidence(source) => {
                Some(source)
            }
            Self::InvalidEnvironment(source) => Some(source),
            Self::InvalidScenarioSet(source) => Some(source),
            Self::InvalidScaleProfile(source) => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        MatrixOutcome, MatrixSample, QueryShape, deterministic_matrix,
        fill_matrix_phase_reconciliation,
        sql_perf_profile::SQL_PERFORMANCE_PROFILE,
        sql_perf_scale::{ScaleEvidenceError, ScaleSelectivity, build_scale_observation},
    };

    use super::*;

    fn fixture_facts(declaration: &ScaleScenarioDeclaration) -> crate::ScaleFixtureFacts {
        crate::ScaleFixtureFacts {
            profile_version: 1,
            surface: declaration.spec.surface.label().to_string(),
            fixture_rows: declaration.fixture_rows,
            zero_match_rows: 0,
            one_match_rows: 1,
            quarter_match_rows: declaration.fixture_rows / 4,
            all_match_rows: declaration.fixture_rows,
            payload_profile: declaration.spec.payload_profile,
        }
    }

    fn observation(declaration: &ScaleScenarioDeclaration) -> ScaleObservation {
        let facts = fixture_facts(declaration);
        let predicate_rows = declaration.spec.selectivity.realized_rows(&facts);
        let row_count = match declaration.scenario.metadata.shape {
            QueryShape::Scalar => declaration
                .spec
                .result_window
                .map_or(predicate_rows, |window| predicate_rows.min(window)),
            QueryShape::Grouped => 2,
            QueryShape::GlobalAggregate | QueryShape::Metadata | QueryShape::Mutation => 1,
        };
        let mut sample = MatrixSample {
            key: declaration.scenario.key.clone(),
            surface: declaration.spec.surface.label().to_string(),
            family: declaration.scenario.family.clone(),
            sql: declaration.scenario.sql.clone(),
            route_family: declaration.spec.route_family.code().to_string(),
            total_local_instructions: 10_000 + u64::from(declaration.fixture_rows),
            outcome: MatrixOutcome {
                result_kind: "test".to_string(),
                entity: declaration.spec.surface.table().to_string(),
                row_count: usize::try_from(row_count).expect("test row count fits usize"),
            },
            ..MatrixSample::default()
        };
        fill_matrix_phase_reconciliation(&mut sample);

        build_scale_observation(declaration, facts, sample)
            .expect("test observation should match its declaration")
    }

    fn complete_reports() -> (Vec<MatrixScenario>, Vec<ScaleShardReport>) {
        let p1_scenarios = deterministic_matrix();
        let declarations = scale_scenario_declarations(SQL_PERFORMANCE_PROFILE, &p1_scenarios)
            .expect("current scale declarations should be valid");
        let mut by_shard = BTreeMap::<u8, Vec<ScaleObservation>>::new();
        for declaration in &declarations {
            let shard_index = SQL_PERFORMANCE_PROFILE
                .scenario_shard(&declaration.scenario.key)
                .expect("test declaration should have a shard");
            by_shard
                .entry(shard_index)
                .or_default()
                .push(observation(declaration));
        }
        let reports = (0..SQL_PERFORMANCE_PROFILE.shard_count())
            .map(|shard_index| {
                build_scale_shard_report(
                    SQL_PERFORMANCE_PROFILE,
                    "wasm-release",
                    crate::sql_perf_environment::tests::identity(),
                    &p1_scenarios,
                    shard_index,
                    by_shard.remove(&shard_index).unwrap_or_default(),
                )
                .expect("complete assigned shard should validate")
            })
            .collect();

        (p1_scenarios, reports)
    }

    /// Build one complete merged scale report for sibling unit tests.
    pub(crate) fn complete_report() -> (Vec<MatrixScenario>, MergedScaleShardReports) {
        let (p1_scenarios, reports) = complete_reports();
        let merged = merge_scale_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &p1_scenarios,
            reports,
        )
        .expect("complete test scale evidence should merge");

        (p1_scenarios, merged)
    }

    #[test]
    fn scale_scenario_set_hash_and_shard_counts_are_stable() {
        let declarations =
            scale_scenario_declarations(SQL_PERFORMANCE_PROFILE, &deterministic_matrix())
                .expect("current scale declarations should be valid");
        let hash = scale_declaration_hash(&declarations).expect("scale IDs should hash");
        assert_eq!(
            hash,
            SQL_PERFORMANCE_PROFILE.expected_scale_scenario_set_hash(),
        );
        assert_eq!(declarations.len(), 45);
        let mut counts = vec![0_usize; usize::from(SQL_PERFORMANCE_PROFILE.shard_count())];
        for declaration in declarations {
            let shard_index = SQL_PERFORMANCE_PROFILE
                .scenario_shard(&declaration.scenario.key)
                .expect("scale scenario should have a shard");
            counts[usize::from(shard_index)] += 1;
        }
        assert_eq!(
            counts,
            SQL_PERFORMANCE_PROFILE.expected_scale_shard_counts(),
        );
    }

    #[test]
    fn global_aggregate_scale_evidence_separates_returned_and_matched_rows() {
        let declarations =
            scale_scenario_declarations(SQL_PERFORMANCE_PROFILE, &deterministic_matrix())
                .expect("current scale declarations should be valid");
        let declaration = declarations
            .iter()
            .find(|declaration| {
                declaration.spec.sentinel_id == "user.not_paginated.aggregate_quarter"
                    && declaration.fixture_rows == 16
            })
            .expect("quarter-selectivity aggregate scale declaration should exist");
        let observation = observation(declaration);

        assert_eq!(observation.predicate_match_rows, 4);
        assert_eq!(observation.sample.outcome.row_count, 1);

        let mut matched_rows_as_output = observation.sample;
        matched_rows_as_output.outcome.row_count = 4;
        assert!(matches!(
            build_scale_observation(
                declaration,
                fixture_facts(declaration),
                matched_rows_as_output,
            ),
            Err(ScaleEvidenceError::ResultCardinalityDrift {
                expected: 1,
                actual: 4,
                ..
            })
        ));
    }

    #[test]
    fn scale_merge_requires_all_shards_and_derives_complete_slopes() {
        let (p1_scenarios, reports) = complete_reports();
        let merged = merge_scale_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &p1_scenarios,
            reports.clone(),
        )
        .expect("all exact scale shards should merge");

        assert_eq!(merged.receipts.len(), 8);
        assert_eq!(merged.observations.len(), 45);
        assert_eq!(merged.slopes.len(), 30);
        assert!(merged.normalized_costs.len() >= 45);
        assert!(merged.p2_representatives.iter().any(|representative| {
            representative.stratum == "route/equality_prefix_ordered_suffix"
        }));
        assert!(merged.p2_representatives.iter().any(|representative| {
            representative.stratum == format!("selectivity/{}", ScaleSelectivity::Zero.code())
        }));

        let incomplete = reports.into_iter().take(7).collect();
        assert!(matches!(
            merge_scale_shard_reports(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &p1_scenarios,
                incomplete,
            ),
            Err(ScaleShardError::ReportCount {
                expected: 8,
                actual: 7,
            })
        ));
    }

    #[test]
    fn scale_shard_artifacts_reject_unknown_fields_and_oversize_output() {
        let (p1_scenarios, reports) = complete_reports();
        let encoded = serde_json::to_string(&reports[0]).expect("test report should encode");
        let with_unknown = encoded.replacen('{', "{\"legacy\":true,", 1);
        assert!(serde_json::from_str::<ScaleShardReport>(&with_unknown).is_err());
        assert!(matches!(
            validate_scale_shard_artifact_size(Path::new("scale.json"), 11, 10),
            Err(ScaleShardError::TooLarge {
                observed_bytes: 11,
                max_bytes: 10,
                ..
            })
        ));

        let merged = merge_scale_shard_reports(
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &p1_scenarios,
            reports,
        )
        .expect("complete test shards should merge");
        let mut coverage_drifted = merged.clone();
        coverage_drifted.measurement_coverage.peak_heap_bytes =
            crate::sql_perf_measurement::PerformanceMeasurementStatus::Measured;
        assert!(matches!(
            validate_merged_scale_report(
                SQL_PERFORMANCE_PROFILE,
                "wasm-release",
                &p1_scenarios,
                &coverage_drifted,
            ),
            Err(ScaleShardError::MeasurementCoverageDrift)
        ));
        let path = std::env::temp_dir().join(format!(
            "icydb-sql-perf-scale-merged-{}.json",
            std::process::id(),
        ));
        write_merged_scale_report(
            &path,
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &p1_scenarios,
            &merged,
        )
        .expect("merged scale report should write");
        let decoded = read_merged_scale_report(
            &path,
            SQL_PERFORMANCE_PROFILE,
            "wasm-release",
            &p1_scenarios,
        )
        .expect("merged scale report should read strictly");
        assert_eq!(decoded, merged);
        fs::remove_file(path).expect("test artifact should be removable");
    }
}
