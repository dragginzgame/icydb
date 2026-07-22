//! Module: db::session::tests::tier_c_reference
//! Responsibility: full fixed Tier C native SQL execution and exact shard receipts.
//! Does not own: generation semantics, shard mapping, receipt formats, CI timeouts, or distribution projection.
//! Boundary: visits one current scenario catalog, reuses native correctness assertions, and emits strict evidence.

use super::{
    mutation_reference::{
        GeneratedMutationMismatch, compare_generated_native_mutation_sequence,
        generated_mutation_snapshot_from_accepted_authority,
    },
    sqlite_reference::{
        GeneratedSelectMismatch, compare_generated_native_reference_case,
        compare_generated_native_rejection_case, execute_required_sqlite_reference_scenario,
        generated_select_snapshot_from_accepted_authority, seed_required_sqlite_reference_fixture,
    },
    *,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Read,
    path::{Path, PathBuf},
};

use icydb_testing_sql_generator::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedExpressionDepth,
    GeneratedFixtureProperty, GeneratedMutationSequence, GeneratedSelectCase,
    MutationKind as CoverageMutationKind, MutationSnapshot, RegressionCorpusCase,
    RegressionCorpusEntry, SQL_SCHEDULED_SHARD_COUNT, SelectComparisonProvider,
    SelectExecutionPhase, SelectExpectedOutcome, SelectMismatchCategory, SelectMismatchSignature,
    SelectObservedOutcome, SelectReplayRecord, SelectSnapshot,
    StatementFamily as CoverageStatementFamily, TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
    TIER_C_INVALID_CASES_PER_VIOLATION, TIER_C_MUTATION_BUDGETS, TIER_C_MUTATION_CASES_PER_ROOT,
    TIER_C_ROOT_SEEDS, TIER_C_SELECT_BUDGETS, TIER_C_VALID_CASES_PER_FAMILY,
    TierCCoverageDistributionReport, TierCDistributionError, TierCFailureArtifact,
    TierCFailureReplay, TierCMergedReport, TierCScenarioDeclaration, TierCScenarioObservation,
    TierCScenarioOutcome, TierCShardReport, checked_in_regression_corpus,
    generate_invalid_select_case, generate_mutation_sequence, generate_valid_select_case,
    generated_mutation_tier_c_declaration, generated_select_tier_c_declaration,
    scheduled_sql_scenario_shard, shrink_mutation_failure, shrink_select_failure,
};
use icydb_testing_sqlite_reference::{
    SqliteReferenceScenario, required_sqlite_reference_scenarios,
};

/// Required zero-based shard-selection environment variable.
const TIER_C_SHARD_INDEX_ENV: &str = "ICYDB_SQL_TIER_C_SHARD_INDEX";

/// Required directory for strict shard and merged artifacts.
const TIER_C_ARTIFACT_DIR_ENV: &str = "ICYDB_SQL_TIER_C_ARTIFACT_DIR";

/// Required current-format minimized failure artifact selected for exact replay.
const TIER_C_FAILURE_ARTIFACT_ENV: &str = "ICYDB_SQL_TIER_C_FAILURE_ARTIFACT";

/// Exact current Tier C declaration count including reviewed corpus entries.
const TIER_C_NATIVE_SCENARIO_COUNT: usize = 2_505;

/// Golden identity of the complete current accepted-snapshot Tier C catalog.
const TIER_C_NATIVE_SCENARIO_SET_HASH: &str =
    "412ed958e3734230ebcfd5f567ccda539401583b4a463b63d64248c0971ace7c";

///
/// TierCNativeScenario
///
/// Borrowed execution input produced by the single current Tier C catalog walk.
/// It carries no alternate scenario identity, expected behavior, or provider logic.
///

enum TierCNativeScenario<'a> {
    /// Reviewed checked-in regression using its embedded current case.
    Corpus(&'a RegressionCorpusEntry),

    /// One deterministic bundled-SQLite/native overlap scenario.
    Deterministic(SqliteReferenceScenario),

    /// One independently modeled native mutation sequence.
    Mutation(&'a GeneratedMutationSequence),

    /// One generated SELECT with its typed provider and acceptance contract.
    Select(&'a GeneratedSelectCase),
}

///
/// TierCNativeFailureReplayError
///
/// Exact way a current minimized failure stopped reproducing its recorded typed evidence.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TierCNativeFailureReplayError {
    /// The minimized mutation sequence now passes its complete comparison.
    MutationNoLongerReproduces,

    /// The minimized mutation sequence failed with different provider outcomes.
    MutationOutcomeDrift,

    /// The minimized mutation sequence failed under a different typed signature.
    MutationSignatureDrift,

    /// The minimized SELECT now passes its complete comparison.
    SelectNoLongerReproduces,

    /// The minimized SELECT failed with different provider outcomes.
    SelectOutcomeDrift,

    /// The minimized SELECT failed under a different typed signature.
    SelectSignatureDrift,
}

impl TierCNativeScenario<'_> {
    fn coverage_declaration(
        &self,
        scenario_id: &str,
    ) -> Result<TierCScenarioDeclaration, TierCDistributionError> {
        match self {
            Self::Corpus(entry) => match entry.regression_case() {
                RegressionCorpusCase::Mutation(sequence) => {
                    generated_mutation_tier_c_declaration(scenario_id, sequence)
                }
                RegressionCorpusCase::Select(case) => {
                    generated_select_tier_c_declaration(scenario_id, case)
                }
            },
            Self::Deterministic(scenario) => scenario.tier_c_declaration(),
            Self::Mutation(sequence) => {
                generated_mutation_tier_c_declaration(scenario_id, sequence)
            }
            Self::Select(case) => generated_select_tier_c_declaration(scenario_id, case),
        }
    }

    fn expected_outcome(&self) -> TierCScenarioOutcome {
        match self {
            Self::Corpus(entry) => match entry.regression_case() {
                RegressionCorpusCase::Mutation(_) => TierCScenarioOutcome::Passed,
                RegressionCorpusCase::Select(case) => select_outcome(case),
            },
            Self::Deterministic(_) | Self::Mutation(_) => TierCScenarioOutcome::Passed,
            Self::Select(case) => select_outcome(case),
        }
    }

    const fn failure_replay_scenario_id(&self) -> Option<&str> {
        match self {
            Self::Corpus(entry) => Some(entry.regression_case().generated_id()),
            Self::Deterministic(_) => None,
            Self::Mutation(sequence) => Some(sequence.identity().id()),
            Self::Select(case) => Some(case.identity().id()),
        }
    }

    fn execute(self, scenario_id: &str) -> Result<(), TierCFailureArtifact> {
        match self {
            Self::Corpus(entry) => execute_regression_corpus_entry(scenario_id, entry),
            Self::Deterministic(scenario) => {
                reset_session_sql_store();
                let session = sql_session();
                seed_required_sqlite_reference_fixture(&session);
                execute_required_sqlite_reference_scenario(&session, scenario);
                Ok(())
            }
            Self::Mutation(sequence) => execute_mutation_sequence(scenario_id, sequence),
            Self::Select(case) => execute_select_case(scenario_id, case),
        }
    }
}

#[test]
fn tier_c_native_catalog_is_exact_unique_and_fully_sharded() {
    let inputs = TierCNativeInputs::current();
    let declared = inputs.declared_scenario_ids();
    let expected_count = required_sqlite_reference_scenarios()
        .len()
        .saturating_add(inputs.corpus.len())
        .saturating_add(
            TIER_C_ROOT_SEEDS.len().saturating_mul(
                ALL_SELECT_GENERATOR_FAMILIES
                    .len()
                    .saturating_mul(
                        usize::try_from(TIER_C_VALID_CASES_PER_FAMILY)
                            .expect("Tier C valid quota should fit usize"),
                    )
                    .saturating_add(
                        ALL_SELECT_VIOLATIONS.len().saturating_mul(
                            usize::try_from(TIER_C_INVALID_CASES_PER_VIOLATION)
                                .expect("Tier C invalid quota should fit usize"),
                        ),
                    )
                    .saturating_add(
                        usize::try_from(TIER_C_MUTATION_CASES_PER_ROOT)
                            .expect("Tier C mutation quota should fit usize"),
                    ),
            ),
        );
    assert_eq!(declared.len(), expected_count);
    assert_eq!(expected_count, TIER_C_NATIVE_SCENARIO_COUNT);
    assert_eq!(
        declared.iter().collect::<BTreeSet<_>>().len(),
        expected_count
    );

    let populated_shards = declared
        .iter()
        .map(|scenario_id| {
            scheduled_sql_scenario_shard(scenario_id)
                .expect("current Tier C scenario identity should shard")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        populated_shards.len(),
        usize::from(SQL_SCHEDULED_SHARD_COUNT)
    );

    let mut expected_outcomes = BTreeMap::new();
    inputs.visit(|scenario_id, scenario| {
        assert!(
            expected_outcomes
                .insert(scenario_id.to_string(), scenario.expected_outcome())
                .is_none(),
        );
    });
    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = declared
                .iter()
                .filter(|scenario_id| {
                    scheduled_sql_scenario_shard(scenario_id)
                        .expect("current Tier C scenario identity should shard")
                        == shard_index
                })
                .map(|scenario_id| {
                    TierCScenarioObservation::try_new(
                        scenario_id,
                        expected_outcomes[scenario_id].clone(),
                    )
                    .expect("current Tier C declaration should form an identity observation")
                })
                .collect();
            TierCShardReport::try_new(shard_index, &declared_refs, observations)
                .expect("current Tier C declaration should form one exact shard")
        })
        .collect();
    let merged = TierCMergedReport::try_merge(&declared_refs, reports)
        .expect("all current Tier C declaration shards should merge exactly");
    assert_eq!(
        merged.expected_scenario_set_hash(),
        TIER_C_NATIVE_SCENARIO_SET_HASH,
    );
    assert_native_outcome_contract(&inputs, &merged);
    assert_native_coverage_contract(&inputs, &merged);
}

#[test]
fn reviewed_regression_corpus_matches_current_native_behavior() {
    let inputs = TierCNativeInputs::current();
    assert!(!inputs.corpus.is_empty());

    for entry in &inputs.corpus {
        let scenario_id = format!("corpus.{}", entry.regression_id());
        execute_regression_corpus_entry(scenario_id.as_str(), entry).unwrap_or_else(|artifact| {
            panic!(
                "reviewed regression {:?} diverged from current behavior: {artifact:?}",
                entry.regression_id(),
            )
        });
    }
}

#[test]
fn tier_c_failed_receipts_resolve_exact_generated_and_corpus_sources() {
    let mut inputs = TierCNativeInputs::current();
    let (generated, replay) = injected_tier_c_select_failure_replay(&inputs.select_snapshot);
    let regression_id = "select.failure-link";
    let corpus_scenario_id = format!("corpus.{regression_id}");
    inputs.corpus.push(
        RegressionCorpusEntry::try_from_select_replay(regression_id, &replay)
            .expect("injected minimized replay should form a reviewed corpus entry"),
    );
    let directory =
        env::temp_dir().join(format!("icydb-tier-c-failure-link-{}", std::process::id()));
    let generated_failure_artifact =
        TierCFailureArtifact::try_from_select_replay(generated.identity().id(), replay.clone())
            .expect("generated failure-link artifact should validate");
    assert_eq!(
        replay_tier_c_failure_artifact(&generated_failure_artifact),
        Err(TierCNativeFailureReplayError::SelectNoLongerReproduces),
    );
    let corpus_failure_artifact =
        TierCFailureArtifact::try_from_select_replay(&corpus_scenario_id, replay)
            .expect("corpus failure-link artifact should validate");
    let failure_artifact_ids = BTreeMap::from([
        (
            generated.identity().id().to_string(),
            write_failure_artifact(&directory, &generated_failure_artifact),
        ),
        (
            corpus_scenario_id,
            write_failure_artifact(&directory, &corpus_failure_artifact),
        ),
    ]);
    let declared = inputs.declared_scenario_ids();
    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let mut expected = BTreeMap::new();
    inputs.visit(|scenario_id, scenario| {
        expected.insert(scenario_id.to_string(), scenario.expected_outcome());
    });
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let observations = declared
                .iter()
                .filter(|scenario_id| {
                    scheduled_sql_scenario_shard(scenario_id)
                        .expect("current Tier C scenario identity should shard")
                        == shard_index
                })
                .map(|scenario_id| {
                    let outcome = failure_artifact_ids.get(scenario_id).map_or_else(
                        || expected[scenario_id].clone(),
                        |artifact_id| TierCScenarioOutcome::Failed(artifact_id.clone()),
                    );
                    TierCScenarioObservation::try_new(scenario_id, outcome)
                        .expect("injected failure-link observation should validate")
                })
                .collect();
            TierCShardReport::try_new(shard_index, &declared_refs, observations)
                .expect("injected failure-link shard should validate")
        })
        .collect();
    let merged = TierCMergedReport::try_merge(&declared_refs, reports)
        .expect("injected failure-link receipts should merge red");

    validate_failure_artifact_references(&inputs, &directory, &merged);
    fs::remove_dir_all(directory).expect("injected failure-link directory should clean up");
}

fn injected_tier_c_select_failure_replay(
    snapshot: &SelectSnapshot,
) -> (GeneratedSelectCase, SelectReplayRecord) {
    let generated = generate_valid_select_case(
        snapshot,
        TIER_C_ROOT_SEEDS[0],
        ALL_SELECT_GENERATOR_FAMILIES[0],
        0,
        TIER_C_SELECT_BUDGETS,
    )
    .expect("fixed Tier C SELECT should generate for failure-link evidence");
    let signature = SelectMismatchSignature::try_new(
        generated.features().clone(),
        SelectExecutionPhase::Comparison,
        "icydb-native",
        SelectComparisonProvider::SqliteReference,
        None,
        SelectMismatchCategory::Value,
        Some("injected-failure-link".to_string()),
    )
    .expect("injected failure-link signature should validate");
    let replay = SelectReplayRecord::try_new(
        generated.clone(),
        generated.clone(),
        signature,
        SelectObservedOutcome::accepted("subject-result", 1),
        SelectObservedOutcome::accepted("reference-result", 1),
        true,
        0,
        0,
    )
    .expect("injected failure-link replay should validate");

    (generated, replay)
}

#[test]
#[ignore = "scheduled Tier C shard execution is user-owned release validation"]
fn tier_c_native_shard_emits_exact_receipt() {
    let shard_index = selected_shard_index();
    let inputs = TierCNativeInputs::current();
    let artifact_directory = artifact_directory();
    let mut declared = Vec::new();
    let mut observations = Vec::new();
    inputs.visit(|scenario_id, scenario| {
        declared.push(scenario_id.to_string());
        let assigned_shard = scheduled_sql_scenario_shard(scenario_id)
            .expect("current Tier C scenario identity should shard");
        if assigned_shard != shard_index {
            return;
        }

        let expected_outcome = scenario.expected_outcome();
        let expected_failure_replay_scenario_id =
            scenario.failure_replay_scenario_id().map(str::to_string);
        let outcome = match scenario.execute(scenario_id) {
            Ok(()) => expected_outcome,
            Err(artifact) => {
                assert_failure_artifact_matches_native_scenario(
                    scenario_id,
                    expected_failure_replay_scenario_id.as_deref(),
                    &artifact,
                );
                TierCScenarioOutcome::Failed(write_failure_artifact(&artifact_directory, &artifact))
            }
        };
        observations.push(
            TierCScenarioObservation::try_new(scenario_id, outcome)
                .expect("executed Tier C scenario observation should validate"),
        );
    });
    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let report = TierCShardReport::try_new(shard_index, &declared_refs, observations)
        .expect("executed Tier C shard should exactly cover its declared membership");
    let bytes = report
        .to_canonical_json(&declared_refs)
        .expect("executed Tier C shard should encode canonically");
    let path = shard_artifact_path(&artifact_directory, shard_index);
    write_artifact(&path, bytes.as_slice());

    eprintln!(
        "Tier C shard {shard_index} wrote {} exact observations to {}",
        report.observed_scenario_count(),
        path.display(),
    );
    assert_eq!(
        report.failed_scenario_count(),
        0,
        "Tier C shard {shard_index} retained minimized failure evidence and remains red",
    );
}

#[test]
#[ignore = "scheduled Tier C receipt merge is user-owned release validation"]
fn tier_c_native_receipts_merge_exactly_and_require_clean_evidence() {
    let inputs = TierCNativeInputs::current();
    let declared = inputs.declared_scenario_ids();
    let declared_refs = declared.iter().map(String::as_str).collect::<Vec<_>>();
    let artifact_directory = artifact_directory();
    let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
        .map(|shard_index| {
            let path = shard_artifact_path(&artifact_directory, shard_index);
            let bytes = read_bounded_artifact(&path);
            TierCShardReport::from_canonical_json(bytes.as_slice(), &declared_refs)
                .unwrap_or_else(|error| panic!("Tier C shard {} rejected: {error}", path.display()))
        })
        .collect::<Vec<_>>();
    let merged = TierCMergedReport::try_merge(&declared_refs, reports)
        .expect("all eight exact Tier C receipts should merge");
    assert_native_outcome_contract(&inputs, &merged);
    validate_failure_artifact_references(&inputs, &artifact_directory, &merged);
    let bytes = merged
        .to_canonical_json(&declared_refs)
        .expect("merged Tier C report should encode canonically");
    let path = artifact_directory.join("tier-c-merged.json");
    write_artifact(&path, bytes.as_slice());
    let declarations = inputs.coverage_declarations();
    let distribution =
        TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged)
            .expect("complete Tier C evidence must be clean and semantically labeled");
    let distribution_bytes = distribution
        .to_canonical_json(&declarations, &merged)
        .expect("Tier C coverage distribution should encode canonically");
    let distribution_path = artifact_directory.join("tier-c-distribution.json");
    write_artifact(&distribution_path, distribution_bytes.as_slice());

    eprintln!(
        "Tier C merge wrote {} clean observations to {} and typed distribution to {}",
        merged.observed_scenario_count(),
        path.display(),
        distribution_path.display(),
    );
}

#[test]
#[ignore = "focused minimized-failure replay is user-selected validation"]
fn tier_c_failure_artifact_replays_exact_minimized_failure() {
    let path = env::var_os(TIER_C_FAILURE_ARTIFACT_ENV).map_or_else(
        || panic!("{TIER_C_FAILURE_ARTIFACT_ENV} must name one failure artifact"),
        PathBuf::from,
    );
    let bytes = read_bounded_artifact(&path);
    let artifact =
        TierCFailureArtifact::from_canonical_json(bytes.as_slice()).unwrap_or_else(|error| {
            panic!(
                "Tier C failure artifact {} rejected: {error}",
                path.display()
            )
        });
    let inputs = TierCNativeInputs::current();
    let replay_scenario_ids = inputs.failure_replay_scenario_ids();
    let expected_replay_scenario_id = replay_scenario_ids
        .get(artifact.scenario_id())
        .unwrap_or_else(|| {
            panic!(
                "Tier C failure artifact {} names a scenario outside the current native catalog",
                path.display(),
            )
        });
    assert_failure_artifact_matches_native_scenario(
        artifact.scenario_id(),
        expected_replay_scenario_id.as_deref(),
        &artifact,
    );
    replay_tier_c_failure_artifact(&artifact).unwrap_or_else(|error| {
        panic!(
            "Tier C failure artifact {} did not reproduce exactly: {error:?}",
            path.display(),
        )
    });

    eprintln!(
        "Tier C failure artifact {} reproduced scenario {:?} with its exact minimized signature and outcomes",
        path.display(),
        artifact.scenario_id(),
    );
}

///
/// TierCNativeInputs
///
/// Current accepted-snapshot facts plus the one strict checked-in corpus used by
/// both native shard execution and receipt merge declaration reconstruction.
///

struct TierCNativeInputs {
    corpus: Vec<RegressionCorpusEntry>,
    mutation_snapshot: MutationSnapshot,
    select_snapshot: SelectSnapshot,
}

impl TierCNativeInputs {
    fn current() -> Self {
        reset_session_sql_store();
        let session = sql_session();
        let select_snapshot = generated_select_snapshot_from_accepted_authority(&session)
            .expect("accepted SELECT snapshot should map into Tier C generator facts");
        let mutation_snapshot = generated_mutation_snapshot_from_accepted_authority(&session)
            .expect("accepted mutation snapshot should map into Tier C generator facts");
        let corpus = checked_in_regression_corpus()
            .expect("sole checked-in regression corpus should validate strictly");

        Self {
            corpus,
            mutation_snapshot,
            select_snapshot,
        }
    }

    fn declared_scenario_ids(&self) -> Vec<String> {
        let mut declared = Vec::new();
        self.visit(|scenario_id, _| declared.push(scenario_id.to_string()));
        declared
    }

    fn coverage_declarations(&self) -> Vec<TierCScenarioDeclaration> {
        let mut declarations = Vec::new();
        self.visit(|scenario_id, scenario| {
            declarations.push(
                scenario
                    .coverage_declaration(scenario_id)
                    .unwrap_or_else(|error| {
                        panic!(
                            "current Tier C scenario {scenario_id:?} should declare coverage: {error}",
                        )
                    }),
            );
        });
        declarations
    }

    fn failure_replay_scenario_ids(&self) -> BTreeMap<String, Option<String>> {
        let mut scenario_ids = BTreeMap::new();
        self.visit(|scenario_id, scenario| {
            assert!(
                scenario_ids
                    .insert(
                        scenario_id.to_string(),
                        scenario.failure_replay_scenario_id().map(str::to_string),
                    )
                    .is_none(),
                "Tier C failure-replay catalog contains duplicate scenario {scenario_id:?}",
            );
        });
        scenario_ids
    }

    fn visit<F>(&self, mut visit: F)
    where
        F: for<'scenario> FnMut(&'scenario str, TierCNativeScenario<'scenario>),
    {
        for scenario in required_sqlite_reference_scenarios() {
            visit(scenario.id(), TierCNativeScenario::Deterministic(*scenario));
        }
        for root_seed in TIER_C_ROOT_SEEDS {
            for family in ALL_SELECT_GENERATOR_FAMILIES {
                for case_index in 0..TIER_C_VALID_CASES_PER_FAMILY {
                    let generated = generate_valid_select_case(
                        &self.select_snapshot,
                        *root_seed,
                        *family,
                        case_index,
                        TIER_C_SELECT_BUDGETS,
                    )
                    .expect("fixed Tier C valid SELECT should generate");
                    visit(
                        generated.identity().id(),
                        TierCNativeScenario::Select(&generated),
                    );
                }
            }
            for violation in ALL_SELECT_VIOLATIONS {
                for case_index in 0..TIER_C_INVALID_CASES_PER_VIOLATION {
                    let generated = generate_invalid_select_case(
                        &self.select_snapshot,
                        *root_seed,
                        *violation,
                        case_index,
                        TIER_C_SELECT_BUDGETS,
                    )
                    .expect("fixed Tier C invalid SELECT should generate");
                    visit(
                        generated.identity().id(),
                        TierCNativeScenario::Select(&generated),
                    );
                }
            }
            for case_index in 0..TIER_C_MUTATION_CASES_PER_ROOT {
                let sequence = generate_mutation_sequence(
                    &self.mutation_snapshot,
                    *root_seed,
                    case_index,
                    TIER_C_MUTATION_BUDGETS,
                )
                .expect("fixed Tier C mutation sequence should generate");
                visit(
                    sequence.identity().id(),
                    TierCNativeScenario::Mutation(&sequence),
                );
            }
        }
        for entry in &self.corpus {
            let scenario_id = format!("corpus.{}", entry.regression_id());
            visit(scenario_id.as_str(), TierCNativeScenario::Corpus(entry));
        }
    }
}

const fn select_outcome(case: &GeneratedSelectCase) -> TierCScenarioOutcome {
    match case.expected() {
        SelectExpectedOutcome::Accepted => TierCScenarioOutcome::Passed,
        SelectExpectedOutcome::Rejected(_) => TierCScenarioOutcome::ExpectedRejection,
    }
}

fn execute_select_case(
    scenario_id: &str,
    case: &GeneratedSelectCase,
) -> Result<(), TierCFailureArtifact> {
    reset_session_sql_store();
    let session = sql_session();
    match case.expected() {
        SelectExpectedOutcome::Accepted => compare_generated_native_reference_case(&session, case)
            .map_err(|failure| minimize_select_failure(scenario_id, case, *failure)),
        SelectExpectedOutcome::Rejected(_) => {
            compare_generated_native_rejection_case(&session, case)
                .map_err(|failure| minimize_select_failure(scenario_id, case, *failure))
        }
    }
}

fn execute_mutation_sequence(
    scenario_id: &str,
    sequence: &GeneratedMutationSequence,
) -> Result<(), TierCFailureArtifact> {
    reset_session_sql_store();
    let session = sql_session();
    compare_generated_native_mutation_sequence(&session, sequence)
        .map_err(|failure| minimize_mutation_failure(scenario_id, sequence, *failure))
}

fn execute_regression_corpus_entry(
    scenario_id: &str,
    entry: &RegressionCorpusEntry,
) -> Result<(), TierCFailureArtifact> {
    match entry.regression_case() {
        RegressionCorpusCase::Mutation(sequence) => {
            execute_mutation_sequence(scenario_id, sequence)
        }
        RegressionCorpusCase::Select(case) => execute_select_case(scenario_id, case),
    }
}

fn minimize_select_failure(
    scenario_id: &str,
    case: &GeneratedSelectCase,
    original_failure: GeneratedSelectMismatch,
) -> TierCFailureArtifact {
    let signature = original_failure.signature().clone();
    let report = shrink_select_failure(case, &signature, |candidate| {
        Ok(observe_select_mismatch(candidate).map(|failure| failure.signature().clone()))
    })
    .expect("typed Tier C SELECT mismatch should shrink within generator contracts");
    let minimized_case = report.minimized_case().clone();
    let minimized_failure = observe_select_mismatch(&minimized_case)
        .expect("smallest Tier C SELECT candidate should retain its mismatch");
    assert_eq!(
        minimized_failure.signature(),
        &signature,
        "smallest Tier C SELECT candidate changed mismatch identity",
    );
    let (subject_outcome, comparison_outcome) = minimized_failure.outcomes();
    let replay = report
        .into_replay_record(subject_outcome, comparison_outcome)
        .expect("minimized Tier C SELECT mismatch should form current replay evidence");

    TierCFailureArtifact::try_from_select_replay(scenario_id, replay)
        .expect("minimized Tier C SELECT replay should form one bounded failure artifact")
}

fn observe_select_mismatch(case: &GeneratedSelectCase) -> Option<Box<GeneratedSelectMismatch>> {
    reset_session_sql_store();
    let session = sql_session();
    match case.expected() {
        SelectExpectedOutcome::Accepted => {
            compare_generated_native_reference_case(&session, case).err()
        }
        SelectExpectedOutcome::Rejected(_) => {
            compare_generated_native_rejection_case(&session, case).err()
        }
    }
}

fn minimize_mutation_failure(
    scenario_id: &str,
    sequence: &GeneratedMutationSequence,
    original_failure: GeneratedMutationMismatch,
) -> TierCFailureArtifact {
    let signature = original_failure.signature().clone();
    let report = shrink_mutation_failure(sequence, &signature, |candidate| {
        Ok(observe_mutation_mismatch(candidate).map(|failure| failure.signature().clone()))
    })
    .expect("typed Tier C mutation mismatch should shrink within generator contracts");
    let minimized_sequence = report.minimized_sequence().clone();
    let minimized_failure = observe_mutation_mismatch(&minimized_sequence)
        .expect("smallest Tier C mutation candidate should retain its mismatch");
    assert_eq!(
        minimized_failure.signature(),
        &signature,
        "smallest Tier C mutation candidate changed mismatch identity",
    );
    let (subject_outcome, comparison_outcome) = minimized_failure.outcomes();
    let replay = report
        .into_replay_record(subject_outcome, comparison_outcome)
        .expect("minimized Tier C mutation mismatch should form current replay evidence");

    TierCFailureArtifact::try_from_mutation_replay(scenario_id, replay)
        .expect("minimized Tier C mutation replay should form one bounded failure artifact")
}

fn observe_mutation_mismatch(
    sequence: &GeneratedMutationSequence,
) -> Option<Box<GeneratedMutationMismatch>> {
    reset_session_sql_store();
    let session = sql_session();
    compare_generated_native_mutation_sequence(&session, sequence).err()
}

fn replay_tier_c_failure_artifact(
    artifact: &TierCFailureArtifact,
) -> Result<(), TierCNativeFailureReplayError> {
    match artifact.replay() {
        TierCFailureReplay::Mutation(replay) => {
            let failure = observe_mutation_mismatch(replay.minimized_sequence())
                .ok_or(TierCNativeFailureReplayError::MutationNoLongerReproduces)?;
            if failure.signature() != replay.signature() {
                return Err(TierCNativeFailureReplayError::MutationSignatureDrift);
            }
            let (subject_outcome, comparison_outcome) = failure.outcomes();
            if &subject_outcome != replay.subject_outcome()
                || &comparison_outcome != replay.comparison_outcome()
            {
                return Err(TierCNativeFailureReplayError::MutationOutcomeDrift);
            }
        }
        TierCFailureReplay::Select(replay) => {
            let failure = observe_select_mismatch(replay.minimized_case())
                .ok_or(TierCNativeFailureReplayError::SelectNoLongerReproduces)?;
            if failure.signature() != replay.signature() {
                return Err(TierCNativeFailureReplayError::SelectSignatureDrift);
            }
            let (subject_outcome, comparison_outcome) = failure.outcomes();
            if &subject_outcome != replay.subject_outcome()
                || &comparison_outcome != replay.comparison_outcome()
            {
                return Err(TierCNativeFailureReplayError::SelectOutcomeDrift);
            }
        }
    }

    Ok(())
}

fn assert_native_outcome_contract(inputs: &TierCNativeInputs, merged: &TierCMergedReport) {
    let mut expected = BTreeMap::new();
    inputs.visit(|scenario_id, scenario| {
        assert!(
            expected
                .insert(scenario_id.to_string(), scenario.expected_outcome())
                .is_none(),
            "Tier C native outcome contract contains duplicate scenario {scenario_id:?}",
        );
    });
    for observation in merged
        .shard_reports()
        .iter()
        .flat_map(TierCShardReport::observations)
    {
        let declared = expected
            .remove(observation.scenario_id())
            .unwrap_or_else(|| {
                panic!(
                    "Tier C receipt contains undeclared scenario {:?}",
                    observation.scenario_id(),
                )
            });
        let aligned = matches!(
            (&declared, observation.outcome()),
            (
                TierCScenarioOutcome::Passed,
                TierCScenarioOutcome::Passed | TierCScenarioOutcome::Failed(_),
            ) | (
                TierCScenarioOutcome::ExpectedRejection,
                TierCScenarioOutcome::ExpectedRejection | TierCScenarioOutcome::Failed(_),
            )
        );
        assert!(
            aligned,
            "Tier C receipt outcome for {:?} disagrees with its typed native contract",
            observation.scenario_id(),
        );
    }
    assert!(
        expected.is_empty(),
        "Tier C receipt omitted typed native outcome declarations: {:?}",
        expected.keys().collect::<Vec<_>>(),
    );
}

fn assert_native_coverage_contract(inputs: &TierCNativeInputs, merged: &TierCMergedReport) {
    let declarations = inputs.coverage_declarations();
    let distribution =
        TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, merged)
            .expect("current Tier C declarations should form one clean distribution");
    assert_eq!(
        distribution.scenario_count(),
        u32::try_from(TIER_C_NATIVE_SCENARIO_COUNT)
            .expect("Tier C native scenario count should fit u32"),
    );
    for depth in [
        GeneratedExpressionDepth::One,
        GeneratedExpressionDepth::Two,
        GeneratedExpressionDepth::Three,
        GeneratedExpressionDepth::Four,
    ] {
        assert!(
            distribution.generated_expression_depth_count(depth) > 0,
            "Tier C generated SELECT evidence must reach expression-depth stratum {depth:?}",
        );
    }
    assert_native_generated_select_contract(inputs, &distribution);
    let mutation_sequence_count = u32::try_from(TIER_C_ROOT_SEEDS.len())
        .expect("Tier C root count should fit u32")
        .saturating_mul(
            u32::try_from(TIER_C_MUTATION_CASES_PER_ROOT)
                .expect("Tier C mutation quota should fit u32"),
        );

    assert_eq!(
        distribution.generated_mutation_fixture_row_count(4),
        mutation_sequence_count,
        "every current generated mutation sequence must report its four-row fixture",
    );
    assert_eq!(
        distribution.generated_mutation_statement_count(8),
        mutation_sequence_count,
        "every current generated mutation sequence must report its reviewed eight-step shape",
    );
    assert_eq!(
        distribution.generated_schema_fixture_family_count("session-write-accepted-snapshot-v1"),
        mutation_sequence_count,
    );
    let mutation_labels = distribution
        .mutation_count(CoverageMutationKind::Insert)
        .saturating_add(distribution.mutation_count(CoverageMutationKind::Update))
        .saturating_add(distribution.mutation_count(CoverageMutationKind::Delete));
    assert!(
        mutation_labels > mutation_sequence_count,
        "mixed Tier C mutation sequences must contribute more than one operation label",
    );
    for (statement, mutation) in [
        (
            CoverageStatementFamily::Insert,
            CoverageMutationKind::Insert,
        ),
        (
            CoverageStatementFamily::Update,
            CoverageMutationKind::Update,
        ),
        (
            CoverageStatementFamily::Delete,
            CoverageMutationKind::Delete,
        ),
    ] {
        assert_eq!(
            distribution.statement_count(statement),
            distribution.mutation_count(mutation),
        );
    }
}

fn assert_native_generated_select_contract(
    inputs: &TierCNativeInputs,
    distribution: &TierCCoverageDistributionReport,
) {
    let root_count =
        u32::try_from(TIER_C_ROOT_SEEDS.len()).expect("Tier C root count should fit u32");
    let family_count = u32::try_from(ALL_SELECT_GENERATOR_FAMILIES.len())
        .expect("Tier C SELECT family count should fit u32");
    let violation_count = u32::try_from(ALL_SELECT_VIOLATIONS.len())
        .expect("Tier C SELECT violation count should fit u32");
    let accepted_generated_select_count = root_count.saturating_mul(family_count).saturating_mul(
        u32::try_from(TIER_C_VALID_CASES_PER_FAMILY)
            .expect("Tier C valid case count should fit u32"),
    );
    let rejected_generated_select_count =
        root_count.saturating_mul(violation_count).saturating_mul(
            u32::try_from(TIER_C_INVALID_CASES_PER_VIOLATION)
                .expect("Tier C invalid case count should fit u32"),
        );
    let generated_select_count =
        accepted_generated_select_count.saturating_add(rejected_generated_select_count);
    let corpus_select_count = u32::try_from(
        inputs
            .corpus
            .iter()
            .filter(|entry| matches!(entry.regression_case(), RegressionCorpusCase::Select(_)))
            .count(),
    )
    .expect("Tier C SELECT corpus count should fit u32");
    let profiled_select_count = generated_select_count.saturating_add(corpus_select_count);
    let executed_profiled_select_count =
        accepted_generated_select_count.saturating_add(corpus_select_count);
    for row_count in [0, 1, 32, 64] {
        assert!(
            distribution.generated_select_fixture_row_count(row_count) > 0,
            "Tier C generated SELECT evidence must reach fixture size {row_count}",
        );
    }
    for property in [
        GeneratedFixtureProperty::StoredNull,
        GeneratedFixtureProperty::DuplicateValue,
        GeneratedFixtureProperty::NumericBoundary,
        GeneratedFixtureProperty::OrderingTie,
    ] {
        let count = distribution.generated_select_fixture_property_count(property);
        assert!(
            count > 0 && count <= executed_profiled_select_count,
            "accepted Tier C generated SELECT evidence must execute exact fixture property {property:?}",
        );
    }
    assert_eq!(
        distribution.generated_select_schema_field_count(6),
        generated_select_count,
        "current generated SELECTs must report their six accepted fields exactly",
    );
    assert_eq!(
        distribution.generated_select_schema_field_count(5),
        corpus_select_count,
        "the reviewed SELECT regression corpus must retain its exact five-field replay snapshot",
    );
    assert_eq!(
        distribution.generated_select_schema_generated_field_count(1),
        profiled_select_count,
        "every generated or replayed SELECT schema must report its generated ULID field exactly",
    );
    assert_eq!(
        distribution.generated_select_schema_index_count(0),
        profiled_select_count,
        "every generated or replayed SELECT schema must report zero secondary indexes exactly",
    );
    assert_eq!(
        distribution.generated_select_schema_nullable_field_count(1),
        generated_select_count,
        "current generated SELECTs must report their one nullable field exactly",
    );
    assert_eq!(
        distribution.generated_select_schema_nullable_field_count(0),
        corpus_select_count,
        "the reviewed SELECT regression corpus must retain its exact non-null replay snapshot",
    );
    assert_eq!(
        distribution.generated_schema_fixture_family_count("session-accepted-snapshot-v1"),
        profiled_select_count,
    );
}

fn selected_shard_index() -> u8 {
    let raw = env::var(TIER_C_SHARD_INDEX_ENV)
        .unwrap_or_else(|_| panic!("{TIER_C_SHARD_INDEX_ENV} must select one exact shard"));
    let shard_index = raw
        .parse::<u8>()
        .unwrap_or_else(|error| panic!("invalid {TIER_C_SHARD_INDEX_ENV} value {raw:?}: {error}"));
    assert!(
        shard_index < SQL_SCHEDULED_SHARD_COUNT,
        "{TIER_C_SHARD_INDEX_ENV} must be in 0..{SQL_SCHEDULED_SHARD_COUNT}",
    );
    shard_index
}

fn artifact_directory() -> PathBuf {
    env::var_os(TIER_C_ARTIFACT_DIR_ENV).map_or_else(
        || panic!("{TIER_C_ARTIFACT_DIR_ENV} must name the artifact directory"),
        PathBuf::from,
    )
}

fn shard_artifact_path(directory: &Path, shard_index: u8) -> PathBuf {
    directory.join(format!("tier-c-shard-{shard_index}.json"))
}

fn failure_artifact_path(directory: &Path, artifact_id: &str) -> PathBuf {
    directory
        .join("failures")
        .join(format!("{artifact_id}.json"))
}

fn write_failure_artifact(directory: &Path, artifact: &TierCFailureArtifact) -> String {
    let artifact_id = artifact
        .artifact_id()
        .expect("minimized Tier C failure artifact should derive its content identity");
    let bytes = artifact
        .to_canonical_json()
        .expect("minimized Tier C failure artifact should encode canonically");
    let path = failure_artifact_path(directory, artifact_id.as_str());
    write_artifact(&path, bytes.as_slice());

    artifact_id
}

fn validate_failure_artifact_references(
    inputs: &TierCNativeInputs,
    directory: &Path,
    merged: &TierCMergedReport,
) {
    let replay_scenario_ids = inputs.failure_replay_scenario_ids();
    for observation in merged
        .shard_reports()
        .iter()
        .flat_map(TierCShardReport::observations)
    {
        let TierCScenarioOutcome::Failed(artifact_id) = observation.outcome() else {
            continue;
        };
        let path = failure_artifact_path(directory, artifact_id);
        let bytes = read_bounded_artifact(&path);
        let artifact =
            TierCFailureArtifact::from_canonical_json(bytes.as_slice()).unwrap_or_else(|error| {
                panic!(
                    "Tier C failure artifact {} rejected: {error}",
                    path.display(),
                )
            });
        let expected_replay_scenario_id = replay_scenario_ids
            .get(observation.scenario_id())
            .unwrap_or_else(|| {
                panic!(
                    "Tier C failure artifact {} names a scenario outside the native catalog",
                    path.display(),
                )
            });
        assert_failure_artifact_matches_native_scenario(
            observation.scenario_id(),
            expected_replay_scenario_id.as_deref(),
            &artifact,
        );
        assert_eq!(
            artifact
                .artifact_id()
                .expect("decoded Tier C failure artifact should derive its content identity"),
            *artifact_id,
            "Tier C failure artifact {} does not match its receipt identity",
            path.display(),
        );
    }
}

fn assert_failure_artifact_matches_native_scenario(
    scheduled_scenario_id: &str,
    expected_replay_scenario_id: Option<&str>,
    artifact: &TierCFailureArtifact,
) {
    assert_eq!(
        artifact.scenario_id(),
        scheduled_scenario_id,
        "Tier C failure artifact belongs to a different scheduled scenario",
    );
    let expected_replay_scenario_id = expected_replay_scenario_id.unwrap_or_else(|| {
        panic!(
            "Tier C deterministic scenario {scheduled_scenario_id:?} has no generated replay authority",
        )
    });
    assert_eq!(
        artifact.replay_scenario_id(),
        expected_replay_scenario_id,
        "Tier C failure artifact embeds replay evidence for a different native scenario",
    );
}

fn write_artifact(path: &Path, bytes: &[u8]) {
    let parent = path
        .parent()
        .expect("Tier C artifact path should have a parent directory");
    fs::create_dir_all(parent).unwrap_or_else(|error| {
        panic!(
            "Tier C artifact directory {} should create: {error}",
            parent.display(),
        )
    });
    fs::write(path, bytes)
        .unwrap_or_else(|error| panic!("Tier C artifact {} should write: {error}", path.display()));
}

fn read_bounded_artifact(path: &Path) -> Vec<u8> {
    let file = fs::File::open(path).unwrap_or_else(|error| {
        panic!(
            "required Tier C artifact {} should open: {error}",
            path.display(),
        )
    });
    let read_limit = u64::try_from(TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES)
        .expect("Tier C artifact bound should fit u64")
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .unwrap_or_else(|error| {
            panic!(
                "required Tier C artifact {} should read: {error}",
                path.display(),
            )
        });
    assert!(
        bytes.len() <= TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
        "Tier C artifact {} exceeds its {}-byte bound",
        path.display(),
        TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
    );
    bytes
}
