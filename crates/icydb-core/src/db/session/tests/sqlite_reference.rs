//! Module: db::session::tests::sqlite_reference
//! Responsibility: required native IcyDB/bundled-SQLite SELECT differential evidence.
//! Does not own: SQLite environment policy, manifest eligibility, or production SQL behavior.
//! Boundary: independently executes one shared typed profile and compares exact normalized results.

use super::*;
use crate::db::schema::{AcceptedFieldKind, AcceptedSchemaSnapshot};
use icydb_testing_sql_generator::{
    ALL_SELECT_GENERATOR_FAMILIES, ALL_SELECT_VIOLATIONS, GeneratedSelectCase, GeneratedValue,
    SelectComparisonProvider, SelectExecutionPhase, SelectExpectedOutcome, SelectExpectedRejection,
    SelectField, SelectFieldKind, SelectIndex, SelectMismatchCategory, SelectMismatchSignature,
    SelectObservedOutcome, SelectQueryShape, SelectResultOrder, SelectSnapshot, SelectValueKind,
    SelectViolation, TIER_A_INVALID_CASES_PER_VIOLATION, TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS,
    TIER_A_VALID_CASES_PER_FAMILY, generate_invalid_select_case, generate_valid_select_case,
};
use icydb_testing_sqlite_reference::{
    SQLITE_REFERENCE_FIXTURE_ROWS, SqliteAdapterError, SqliteReferenceColumnKind,
    SqliteReferenceResult, SqliteReferenceRowOrder, SqliteReferenceScenario, SqliteReferenceValue,
    execute_generated_select_case, execute_sqlite_reference_scenario,
    required_sqlite_reference_scenarios,
};

#[test]
fn required_sqlite_reference_profile_matches_native_icydb() {
    reset_session_sql_store();
    let session = sql_session();
    seed_required_sqlite_reference_fixture(&session);

    for scenario in required_sqlite_reference_scenarios() {
        execute_required_sqlite_reference_scenario(&session, *scenario);
    }
}

/// Seed the one native fixture shared by the deterministic SQLite profile.
pub(super) fn seed_required_sqlite_reference_fixture(session: &DbSession<SessionSqlCanister>) {
    insert_session_fixture_rows(
        session,
        SQLITE_REFERENCE_FIXTURE_ROWS.iter().copied(),
        |row| SessionSqliteReferenceEntity {
            id: Ulid::generate(),
            name: row.name().to_string(),
            age: row.age(),
            rank: row.rank(),
            active: row.age() % 2 == 0,
        },
        "SQLite reference",
    );
}

/// Require one deterministic scenario to agree exactly across SQLite and native execution.
pub(super) fn execute_required_sqlite_reference_scenario(
    session: &DbSession<SessionSqlCanister>,
    scenario: SqliteReferenceScenario,
) {
    let expected = execute_sqlite_reference_scenario(scenario).unwrap_or_else(|error| {
        panic!(
            "SQLite reference scenario {:?} should execute: {error}",
            scenario.id()
        )
    });
    let sql = scenario
        .render_sql("SessionSqliteReferenceEntity")
        .expect("maintained SQLite reference entity identifier should render");
    let result = execute_sql_statement_for_tests::<SessionSqliteReferenceEntity>(session, &sql)
        .unwrap_or_else(|error| {
            panic!(
                "IcyDB SQLite reference scenario {:?} should execute: {error}",
                scenario.id()
            )
        });
    let actual = normalize_icydb_result(scenario, result).unwrap_or_else(|error| {
        panic!(
            "IcyDB SQLite reference scenario {:?} should normalize: {error}",
            scenario.id()
        )
    });

    assert_eq!(
        actual,
        expected,
        "native IcyDB should agree with bundled SQLite for scenario {:?}",
        scenario.id(),
    );
}

#[test]
fn tier_a_generated_select_profile_matches_native_icydb() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_select_snapshot_from_accepted_authority(&session)
        .expect("accepted session snapshot should map into generator facts");
    let mut executed = 0_u32;
    let mut route_distribution = GeneratedRouteDistribution::default();

    for root_seed in TIER_A_ROOT_SEEDS {
        for family in ALL_SELECT_GENERATOR_FAMILIES {
            for case_index in 0..TIER_A_VALID_CASES_PER_FAMILY {
                reset_session_sql_store();
                let session = sql_session();
                let generated = generate_valid_select_case(
                    &snapshot,
                    *root_seed,
                    *family,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A native differential case should generate");
                let route = execute_generated_native_reference_case(&session, &generated)
                    .unwrap_or_else(|failure| {
                        panic!(
                            "generated SELECT mismatch for {} with SQL {:?}: {failure:?}",
                            generated.identity().id(),
                            generated.rendered_sql(),
                        )
                    });
                route_distribution.record(route);
                executed = executed.saturating_add(1);
            }
        }
    }

    assert_eq!(executed, 128);
    route_distribution.assert_complete(executed);
}

#[test]
fn generated_select_adapter_mismatch_is_typed_and_fingerprint_backed() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_select_snapshot_from_accepted_authority(&session)
        .expect("accepted session snapshot should map into generator facts");
    let generated = generate_valid_select_case(
        &snapshot,
        TIER_A_ROOT_SEEDS[0],
        ALL_SELECT_GENERATOR_FAMILIES[0],
        0,
        TIER_A_SELECT_BUDGETS,
    )
    .expect("typed adapter mismatch case should generate");
    let subject = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![vec![SqliteReferenceValue::Integer(1)]],
        SqliteReferenceRowOrder::Unordered,
    )
    .expect("typed subject mismatch result should validate");
    let comparison = SqliteReferenceResult::try_new(
        vec!["value".to_string()],
        vec![vec![SqliteReferenceValue::Integer(2)]],
        SqliteReferenceRowOrder::Unordered,
    )
    .expect("typed comparison mismatch result should validate");
    let mismatch = GeneratedSelectMismatch::from_results(
        &generated,
        "icydb-native-warm",
        SelectComparisonProvider::SqliteReference,
        &subject,
        &comparison,
        "injected-adapter-mismatch",
    );
    let (subject_outcome, comparison_outcome) = mismatch.outcomes();

    assert_eq!(
        mismatch.signature().category(),
        SelectMismatchCategory::Value
    );
    assert_eq!(
        mismatch.signature().comparison_provider(),
        SelectComparisonProvider::SqliteReference
    );
    assert_ne!(subject_outcome, comparison_outcome);

    let cold_warm = GeneratedSelectMismatch::from_cold_warm_invariant(
        &generated,
        SelectComparisonProvider::IcydbWarm,
        &subject,
        &comparison,
        SelectMismatchCategory::Route,
        "injected-cold-warm-invariant",
    );
    assert_eq!(
        cold_warm.signature().subject_provider_id(),
        "icydb-native-cold"
    );
    assert_eq!(
        cold_warm.signature().comparison_provider(),
        SelectComparisonProvider::IcydbWarm
    );
}

/// Compare one model-accepted generated SELECT without converting a semantic
/// mismatch into a panic, so scheduled evidence can shrink it first.
pub(super) fn compare_generated_native_reference_case(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) -> Result<(), Box<GeneratedSelectMismatch>> {
    execute_generated_native_reference_case(session, generated).map(|_| ())
}

fn execute_generated_native_reference_case(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) -> Result<GeneratedRouteFact, Box<GeneratedSelectMismatch>> {
    assert_eq!(generated.expected(), SelectExpectedOutcome::Accepted);
    seed_generated_select_fixture(session, generated).map_err(|error| {
        Box::new(GeneratedSelectMismatch::from_fixture_error(
            generated, &error,
        ))
    })?;
    let expected = execute_generated_select_case(generated).map_err(|error| {
        Box::new(GeneratedSelectMismatch::from_reference_error(
            generated, &error,
        ))
    })?;
    let (cold_actual, warm_actual, warm_context) =
        execute_generated_cold_warm_modes(session, generated, &expected)?;

    if cold_actual != warm_actual {
        return Err(Box::new(GeneratedSelectMismatch::from_results(
            generated,
            "icydb-native-cold",
            SelectComparisonProvider::IcydbWarm,
            &cold_actual,
            &warm_actual,
            "cold-warm-result",
        )));
    }
    if warm_actual != expected {
        return Err(Box::new(GeneratedSelectMismatch::from_results(
            generated,
            "icydb-native-warm",
            SelectComparisonProvider::SqliteReference,
            &warm_actual,
            &expected,
            "native-sqlite-result",
        )));
    }
    if session.sql_compiled_command_cache_len() != 1 {
        return Err(Box::new(GeneratedSelectMismatch::from_cold_warm_invariant(
            generated,
            SelectComparisonProvider::IcydbWarm,
            &cold_actual,
            &warm_actual,
            SelectMismatchCategory::Boundary,
            "warm-compiled-command-cache-cardinality",
        )));
    }
    if generated_compiled_command_shape(warm_context.command()) != Some(generated.query().shape()) {
        return Err(Box::new(GeneratedSelectMismatch::from_cold_warm_invariant(
            generated,
            SelectComparisonProvider::IcydbWarm,
            &cold_actual,
            &warm_actual,
            SelectMismatchCategory::Route,
            "compiled-query-shape",
        )));
    }
    generated_route_fact(session, generated, warm_context.command()).map_err(|error| {
        Box::new(GeneratedSelectMismatch::from_cold_warm_invariant(
            generated,
            SelectComparisonProvider::IcydbWarm,
            &cold_actual,
            &warm_actual,
            SelectMismatchCategory::Route,
            error.invariant_class_id(),
        ))
    })
}

///
/// GeneratedSelectMismatch
///
/// Typed generated SELECT failure spanning provider outcomes, typed rejections,
/// setup, and execution invariants. Scheduled execution shrinks this exact
/// signature before converting the evidence into a replay artifact.
///

#[derive(Clone, Debug)]
pub(super) struct GeneratedSelectMismatch {
    comparison_outcome: SelectObservedOutcome,
    signature: SelectMismatchSignature,
    subject_outcome: SelectObservedOutcome,
}

impl GeneratedSelectMismatch {
    /// Borrow the exact structured mismatch identity preserved by shrinking.
    pub(super) const fn signature(&self) -> &SelectMismatchSignature {
        &self.signature
    }

    /// Clone the compact subject and comparison outcomes for replay construction.
    pub(super) fn outcomes(&self) -> (SelectObservedOutcome, SelectObservedOutcome) {
        (
            self.subject_outcome.clone(),
            self.comparison_outcome.clone(),
        )
    }

    fn from_fixture_error(generated: &GeneratedSelectCase, error: &InternalError) -> Self {
        let error_class_id = internal_error_id(error);
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::SqliteReference,
            SelectExecutionPhase::Reference,
            SelectMismatchCategory::InternalInvariant,
            Some(error_class_id.clone()),
            "native-fixture-setup",
            SelectObservedOutcome::infrastructure_failure(
                error_class_id,
                SelectExecutionPhase::Reference,
            ),
            SelectObservedOutcome::infrastructure_failure(
                "sqlite.not_executed",
                SelectExecutionPhase::Reference,
            ),
        )
    }

    fn from_reference_error(generated: &GeneratedSelectCase, error: &SqliteAdapterError) -> Self {
        let error_class_id = error.kind().id();
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::SqliteReference,
            SelectExecutionPhase::Reference,
            SelectMismatchCategory::InternalInvariant,
            Some(error_class_id.to_string()),
            "sqlite-reference-execution",
            SelectObservedOutcome::infrastructure_failure(
                "icydb.not_executed",
                SelectExecutionPhase::Reference,
            ),
            SelectObservedOutcome::infrastructure_failure(
                error_class_id,
                SelectExecutionPhase::Reference,
            ),
        )
    }

    fn from_query_error(
        generated: &GeneratedSelectCase,
        comparison: &SqliteReferenceResult,
        phase: SelectExecutionPhase,
        error: &QueryError,
        invariant_class_id: &str,
    ) -> Self {
        let error_class_id = query_error_id(error);
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::SqliteReference,
            phase,
            SelectMismatchCategory::Acceptance,
            Some(error_class_id.clone()),
            invariant_class_id,
            SelectObservedOutcome::rejected(error_class_id, phase),
            generated_result_outcome(comparison),
        )
    }

    fn from_infrastructure_failure(
        generated: &GeneratedSelectCase,
        comparison: &SqliteReferenceResult,
        phase: SelectExecutionPhase,
        failure_class_id: &str,
        invariant_class_id: &str,
    ) -> Self {
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::SqliteReference,
            phase,
            SelectMismatchCategory::RowShape,
            Some(failure_class_id.to_string()),
            invariant_class_id,
            SelectObservedOutcome::infrastructure_failure(failure_class_id, phase),
            generated_result_outcome(comparison),
        )
    }

    fn from_cold_warm_invariant(
        generated: &GeneratedSelectCase,
        comparison_provider: SelectComparisonProvider,
        subject: &SqliteReferenceResult,
        comparison: &SqliteReferenceResult,
        category: SelectMismatchCategory,
        invariant_class_id: &str,
    ) -> Self {
        Self::from_typed_outcomes(
            generated,
            "icydb-native-cold",
            comparison_provider,
            SelectExecutionPhase::Comparison,
            category,
            None,
            invariant_class_id,
            generated_result_outcome(subject),
            generated_result_outcome(comparison),
        )
    }

    fn from_rejection_mismatch(
        generated: &GeneratedSelectCase,
        error: &QueryError,
        expected: SelectExpectedRejection,
    ) -> Self {
        let phase = expected_rejection_phase(expected);
        let actual_error_class = query_error_id(error);
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::RejectionInvariant,
            phase,
            SelectMismatchCategory::TypedError,
            Some(actual_error_class.clone()),
            "typed-rejection-class",
            SelectObservedOutcome::rejected(actual_error_class, phase),
            SelectObservedOutcome::rejected(expected_rejection_id(expected), phase),
        )
    }

    fn from_unexpected_acceptance(
        generated: &GeneratedSelectCase,
        actual: &SqliteReferenceResult,
        expected: SelectExpectedRejection,
    ) -> Self {
        let phase = expected_rejection_phase(expected);
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::RejectionInvariant,
            phase,
            SelectMismatchCategory::Acceptance,
            None,
            "unexpected-invalid-acceptance",
            generated_result_outcome(actual),
            SelectObservedOutcome::rejected(expected_rejection_id(expected), phase),
        )
    }

    fn from_unshapeable_acceptance(
        generated: &GeneratedSelectCase,
        expected: SelectExpectedRejection,
    ) -> Self {
        let phase = expected_rejection_phase(expected);
        Self::from_typed_outcomes(
            generated,
            "icydb-native",
            SelectComparisonProvider::RejectionInvariant,
            phase,
            SelectMismatchCategory::Acceptance,
            Some("icydb.unexpected_result_shape".to_string()),
            "unexpected-invalid-result-shape",
            SelectObservedOutcome::infrastructure_failure(
                "icydb.unexpected_result_shape",
                SelectExecutionPhase::Execution,
            ),
            SelectObservedOutcome::rejected(expected_rejection_id(expected), phase),
        )
    }

    fn from_results(
        generated: &GeneratedSelectCase,
        subject_provider_id: &str,
        comparison_provider: SelectComparisonProvider,
        subject: &SqliteReferenceResult,
        comparison: &SqliteReferenceResult,
        invariant_class_id: &str,
    ) -> Self {
        Self::from_typed_outcomes(
            generated,
            subject_provider_id,
            comparison_provider,
            SelectExecutionPhase::Comparison,
            classify_generated_result_mismatch(subject, comparison),
            None,
            invariant_class_id,
            generated_result_outcome(subject),
            generated_result_outcome(comparison),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "failure construction keeps signature and both typed provider outcomes explicit"
    )]
    fn from_typed_outcomes(
        generated: &GeneratedSelectCase,
        subject_provider_id: &str,
        comparison_provider: SelectComparisonProvider,
        phase: SelectExecutionPhase,
        category: SelectMismatchCategory,
        error_class_id: Option<String>,
        invariant_class_id: &str,
        subject_outcome: SelectObservedOutcome,
        comparison_outcome: SelectObservedOutcome,
    ) -> Self {
        let signature = SelectMismatchSignature::try_new(
            generated.features().clone(),
            phase,
            subject_provider_id,
            comparison_provider,
            error_class_id,
            category,
            Some(invariant_class_id.to_string()),
        )
        .expect("static generated SELECT mismatch identity should validate");

        Self {
            comparison_outcome,
            signature,
            subject_outcome,
        }
    }
}

fn query_error_id(error: &QueryError) -> String {
    format!(
        "diagnostic.{:04x}",
        error.diagnostic_code().error_code().raw()
    )
}

fn internal_error_id(error: &InternalError) -> String {
    format!(
        "diagnostic.{:04x}",
        error.diagnostic_code().error_code().raw()
    )
}

const fn expected_rejection_id(expected: SelectExpectedRejection) -> &'static str {
    match expected {
        SelectExpectedRejection::InvalidClauseOrder => "invalid_clause_order",
        SelectExpectedRejection::LimitOverflow => "limit_overflow",
        SelectExpectedRejection::UnknownField => "unknown_field",
        SelectExpectedRejection::UnsupportedFunctionSignature => "unsupported_function_signature",
        SelectExpectedRejection::WrongOperatorType => "wrong_operator_type",
    }
}

const fn expected_rejection_phase(expected: SelectExpectedRejection) -> SelectExecutionPhase {
    match expected {
        SelectExpectedRejection::InvalidClauseOrder | SelectExpectedRejection::LimitOverflow => {
            SelectExecutionPhase::Parsing
        }
        SelectExpectedRejection::UnknownField
        | SelectExpectedRejection::UnsupportedFunctionSignature
        | SelectExpectedRejection::WrongOperatorType => SelectExecutionPhase::Planning,
    }
}

fn generated_result_outcome(result: &SqliteReferenceResult) -> SelectObservedOutcome {
    let row_count = u32::try_from(result.rows().len())
        .expect("bounded generated SELECT result row count should fit u32");
    let fingerprint = result
        .fingerprint()
        .expect("bounded normalized generated SELECT result should fingerprint");
    SelectObservedOutcome::accepted(fingerprint, row_count)
}

fn classify_generated_result_mismatch(
    subject: &SqliteReferenceResult,
    comparison: &SqliteReferenceResult,
) -> SelectMismatchCategory {
    if subject.columns() != comparison.columns() {
        return SelectMismatchCategory::RowShape;
    }
    if subject.row_order() != comparison.row_order() {
        return SelectMismatchCategory::Boundary;
    }
    if subject.rows().len() != comparison.rows().len() {
        return SelectMismatchCategory::DuplicateMultiplicity;
    }
    if subject.row_order() == SqliteReferenceRowOrder::Ordered {
        let mut subject_rows = subject.rows().to_vec();
        let mut comparison_rows = comparison.rows().to_vec();
        subject_rows.sort();
        comparison_rows.sort();
        if subject_rows == comparison_rows {
            return SelectMismatchCategory::Ordering;
        }
    }

    SelectMismatchCategory::Value
}

fn execute_generated_cold_warm_modes(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
    expected: &SqliteReferenceResult,
) -> Result<
    (
        SqliteReferenceResult,
        SqliteReferenceResult,
        crate::db::session::sql::SqlCompiledCommandExecutionContext,
    ),
    Box<GeneratedSelectMismatch>,
> {
    let (cold_context, cold_compile_cache, _) = session
        .compile_sql_query_with_execution_context::<SessionSqliteReferenceEntity>(
            generated.rendered_sql(),
        )
        .map_err(|error| {
            Box::new(GeneratedSelectMismatch::from_query_error(
                generated,
                expected,
                SelectExecutionPhase::Planning,
                &error,
                "cold-compile",
            ))
        })?;
    let cold_compiled_cache_len = session.sql_compiled_command_cache_len();
    let (cold_result, cold_cache) = session
        .execute_compiled_sql_context_with_cache_attribution::<SessionSqliteReferenceEntity>(
            &cold_context,
        )
        .map_err(|error| {
            Box::new(GeneratedSelectMismatch::from_query_error(
                generated,
                expected,
                SelectExecutionPhase::Execution,
                &error,
                "cold-execution",
            ))
        })?;
    let (warm_context, warm_compile_cache, _) = session
        .compile_sql_query_with_execution_context::<SessionSqliteReferenceEntity>(
            generated.rendered_sql(),
        )
        .map_err(|error| {
            Box::new(GeneratedSelectMismatch::from_query_error(
                generated,
                expected,
                SelectExecutionPhase::Planning,
                &error,
                "warm-compile",
            ))
        })?;
    let (warm_result, warm_cache) = session
        .execute_compiled_sql_context_with_cache_attribution::<SessionSqliteReferenceEntity>(
            &warm_context,
        )
        .map_err(|error| {
            Box::new(GeneratedSelectMismatch::from_query_error(
                generated,
                expected,
                SelectExecutionPhase::Execution,
                &error,
                "warm-execution",
            ))
        })?;
    let cold_actual = normalize_generated_icydb_result(generated, cold_result).map_err(|_| {
        Box::new(GeneratedSelectMismatch::from_infrastructure_failure(
            generated,
            expected,
            SelectExecutionPhase::Execution,
            "icydb.result_normalization",
            "cold-result-normalization",
        ))
    })?;
    let warm_actual = normalize_generated_icydb_result(generated, warm_result).map_err(|_| {
        Box::new(GeneratedSelectMismatch::from_infrastructure_failure(
            generated,
            expected,
            SelectExecutionPhase::Execution,
            "icydb.result_normalization",
            "warm-result-normalization",
        ))
    })?;
    let cache_invariant = if cold_compiled_cache_len == 1 {
        validate_generated_cold_warm_cache_attribution(
            cold_compile_cache,
            cold_cache,
            warm_compile_cache,
            warm_cache,
        )
    } else {
        Err("cold-compiled-command-cache-cardinality")
    };
    if let Err(invariant_class_id) = cache_invariant {
        return Err(Box::new(GeneratedSelectMismatch::from_cold_warm_invariant(
            generated,
            SelectComparisonProvider::IcydbWarm,
            &cold_actual,
            &warm_actual,
            SelectMismatchCategory::Boundary,
            invariant_class_id,
        )));
    }

    Ok((cold_actual, warm_actual, warm_context))
}

///
/// GeneratedRouteFact
///
/// Typed execution-family observation taken from the compiled command lane or
/// the existing query trace. It deliberately carries no textual EXPLAIN data.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum GeneratedRouteFact {
    /// Dedicated whole-input aggregate command lane.
    GlobalAggregate,

    /// Grouped executor family selected after planning.
    Grouped,

    /// Ordered scalar executor family selected after planning.
    Ordered,

    /// Primary-key scalar executor family selected after planning.
    PrimaryKey,
}

///
/// GeneratedRouteDistribution
///
/// Test-owned route counts used only to prove every generated case yielded a
/// typed route fact. Counts are evidence and never an optimization target.
///

#[derive(Debug, Default)]
struct GeneratedRouteDistribution {
    counts: BTreeMap<GeneratedRouteFact, u32>,
}

impl GeneratedRouteDistribution {
    fn record(&mut self, route: GeneratedRouteFact) {
        let count = self.counts.entry(route).or_default();
        *count = count.saturating_add(1);
    }

    fn count(&self, route: GeneratedRouteFact) -> u32 {
        self.counts.get(&route).copied().unwrap_or_default()
    }

    fn assert_complete(&self, expected_total: u32) {
        let total = self
            .counts
            .values()
            .copied()
            .fold(0_u32, u32::saturating_add);
        let scalar = self
            .count(GeneratedRouteFact::Ordered)
            .saturating_add(self.count(GeneratedRouteFact::PrimaryKey));

        assert_eq!(
            total, expected_total,
            "every generated case must contribute exactly one typed route fact: {self:?}",
        );
        assert!(
            self.count(GeneratedRouteFact::GlobalAggregate) > 0,
            "generated route evidence must include the global aggregate lane: {self:?}",
        );
        assert!(
            self.count(GeneratedRouteFact::Grouped) > 0,
            "generated route evidence must include the grouped family: {self:?}",
        );
        assert!(
            scalar > 0,
            "generated route evidence must include a scalar execution family: {self:?}",
        );
    }
}

fn validate_generated_cold_warm_cache_attribution(
    cold_compile: crate::db::session::sql::SqlCacheAttribution,
    cold_execute: crate::db::session::sql::SqlCacheAttribution,
    warm_compile: crate::db::session::sql::SqlCacheAttribution,
    warm_execute: crate::db::session::sql::SqlCacheAttribution,
) -> Result<(), &'static str> {
    if (
        cold_compile.sql_compiled_command_cache_hits,
        cold_compile.sql_compiled_command_cache_misses,
    ) != (0, 1)
    {
        return Err("cold-compiled-command-cache-attribution");
    }
    if (
        warm_compile.sql_compiled_command_cache_hits,
        warm_compile.sql_compiled_command_cache_misses,
    ) != (1, 0)
    {
        return Err("warm-compiled-command-cache-attribution");
    }
    if (
        cold_execute.sql_compiled_command_cache_hits,
        cold_execute.sql_compiled_command_cache_misses,
    ) != (0, 0)
    {
        return Err("cold-execution-compile-cache-attribution");
    }
    if cold_execute
        .shared_query_plan_cache_hits
        .saturating_add(cold_execute.shared_query_plan_cache_misses)
        != 1
    {
        return Err("cold-plan-cache-attribution");
    }
    if (
        warm_execute.sql_compiled_command_cache_hits,
        warm_execute.sql_compiled_command_cache_misses,
    ) != (0, 0)
    {
        return Err("warm-execution-compile-cache-attribution");
    }
    if (
        warm_execute.shared_query_plan_cache_hits,
        warm_execute.shared_query_plan_cache_misses,
    ) != (1, 0)
    {
        return Err("warm-plan-cache-attribution");
    }

    Ok(())
}

fn generated_compiled_command_shape(
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) -> Option<SelectQueryShape> {
    match compiled {
        crate::db::session::sql::CompiledSqlCommand::GlobalAggregate { .. } => {
            Some(SelectQueryShape::GlobalAggregate)
        }
        crate::db::session::sql::CompiledSqlCommand::Select { query, .. } => {
            if query.has_grouping() {
                Some(SelectQueryShape::GroupedAggregate)
            } else {
                Some(SelectQueryShape::Scalar)
            }
        }
        _ => None,
    }
}

/// Exact route-evidence invariant that failed after successful generated execution.
#[derive(Clone, Copy, Debug)]
enum GeneratedRouteFactError {
    CompiledCommandFamily,
    CompiledQueryShape,
    ExecutionFamilyCompatibility,
    ExecutionFamilyMissing,
    TraceQuery,
}

impl GeneratedRouteFactError {
    const fn invariant_class_id(self) -> &'static str {
        match self {
            Self::CompiledCommandFamily => "route-compiled-command-family",
            Self::CompiledQueryShape => "route-compiled-query-shape",
            Self::ExecutionFamilyCompatibility => "route-execution-family-compatibility",
            Self::ExecutionFamilyMissing => "route-execution-family-missing",
            Self::TraceQuery => "route-trace-query",
        }
    }
}

fn generated_route_fact(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
    compiled: &crate::db::session::sql::CompiledSqlCommand,
) -> Result<GeneratedRouteFact, GeneratedRouteFactError> {
    let expected_shape = generated.query().shape();
    let compiled_shape = generated_compiled_command_shape(compiled)
        .ok_or(GeneratedRouteFactError::CompiledCommandFamily)?;
    if compiled_shape != expected_shape {
        return Err(GeneratedRouteFactError::CompiledQueryShape);
    }
    let crate::db::session::sql::CompiledSqlCommand::Select { query, .. } = compiled else {
        return Ok(GeneratedRouteFact::GlobalAggregate);
    };
    let query = Query::<SessionSqliteReferenceEntity>::from_inner(query.as_ref().clone());
    let trace = session
        .trace_query(&query)
        .map_err(|_| GeneratedRouteFactError::TraceQuery)?;
    let route = match trace.execution_family() {
        Some(crate::db::TraceExecutionFamily::Grouped) => GeneratedRouteFact::Grouped,
        Some(crate::db::TraceExecutionFamily::Ordered) => GeneratedRouteFact::Ordered,
        Some(crate::db::TraceExecutionFamily::PrimaryKey) => GeneratedRouteFact::PrimaryKey,
        None => return Err(GeneratedRouteFactError::ExecutionFamilyMissing),
    };
    match (expected_shape, route) {
        (SelectQueryShape::GroupedAggregate, GeneratedRouteFact::Grouped)
        | (
            SelectQueryShape::Scalar,
            GeneratedRouteFact::Ordered | GeneratedRouteFact::PrimaryKey,
        ) => Ok(route),
        _ => Err(GeneratedRouteFactError::ExecutionFamilyCompatibility),
    }
}

#[test]
fn tier_a_generated_invalid_profile_rejects_with_owned_typed_classes() {
    reset_session_sql_store();
    let session = sql_session();
    let snapshot = generated_select_snapshot_from_accepted_authority(&session)
        .expect("accepted session snapshot should map into generator facts");
    let mut rejected = 0_u32;

    for root_seed in TIER_A_ROOT_SEEDS {
        for violation in ALL_SELECT_VIOLATIONS {
            for case_index in 0..TIER_A_INVALID_CASES_PER_VIOLATION {
                let generated = generate_invalid_select_case(
                    &snapshot,
                    *root_seed,
                    *violation,
                    case_index,
                    TIER_A_SELECT_BUDGETS,
                )
                .expect("Tier A invalid case should generate from one valid base");
                compare_generated_native_rejection_case(&session, &generated)
                    .unwrap_or_else(|failure| panic!("generated rejection mismatch: {failure:?}"));
                rejected = rejected.saturating_add(1);
            }
        }
    }

    assert_eq!(rejected, 40);
}

/// Compare one classified generated SELECT violation with its authoritative
/// typed rejection without panicking on contract drift.
pub(super) fn compare_generated_native_rejection_case(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) -> Result<(), Box<GeneratedSelectMismatch>> {
    let violation = generated
        .violation()
        .expect("rejection execution requires one classified violation");
    assert_eq!(
        generated.expected(),
        SelectExpectedOutcome::Rejected(violation.expected_rejection()),
    );
    match execute_sql_statement_for_tests::<SessionSqliteReferenceEntity>(
        session,
        generated.rendered_sql(),
    ) {
        Err(error) if generated_rejection_matches(&error, violation) => Ok(()),
        Err(error) => Err(Box::new(GeneratedSelectMismatch::from_rejection_mismatch(
            generated,
            &error,
            violation.expected_rejection(),
        ))),
        Ok(result) => {
            let actual = normalize_generated_icydb_result(generated, result).map_err(|_| {
                Box::new(GeneratedSelectMismatch::from_unshapeable_acceptance(
                    generated,
                    violation.expected_rejection(),
                ))
            })?;
            Err(Box::new(
                GeneratedSelectMismatch::from_unexpected_acceptance(
                    generated,
                    &actual,
                    violation.expected_rejection(),
                ),
            ))
        }
    }
}

/// Project the accepted runtime catalog into the bounded SELECT generator contract.
pub(super) fn generated_select_snapshot_from_accepted_authority(
    session: &DbSession<SessionSqlCanister>,
) -> Result<SelectSnapshot, String> {
    let context = session
        .accepted_schema_catalog_context_for_query::<SessionSqliteReferenceEntity>()
        .map_err(|error| error.to_string())?;
    select_snapshot_from_accepted(context.snapshot())
}

fn select_snapshot_from_accepted(
    accepted: &AcceptedSchemaSnapshot,
) -> Result<SelectSnapshot, String> {
    let persisted = accepted.persisted_snapshot();
    let fields = persisted
        .fields()
        .iter()
        .map(|field| {
            let kind = match field.kind() {
                AcceptedFieldKind::Blob { .. } => SelectFieldKind::Blob,
                AcceptedFieldKind::Bool => SelectFieldKind::Boolean,
                AcceptedFieldKind::Int64 => SelectFieldKind::Integer,
                AcceptedFieldKind::Text { .. } => SelectFieldKind::Text,
                AcceptedFieldKind::Ulid => SelectFieldKind::Ulid,
                unsupported => {
                    return Err(format!(
                        "accepted generated-test field {:?} has unsupported kind {unsupported:?}",
                        field.name(),
                    ));
                }
            };
            let primary_key = persisted.primary_key_field_ids().contains(&field.id());
            let write_policy = field.write_policy();
            let database_managed = write_policy.insert_generation().is_some()
                || write_policy.write_management().is_some();
            Ok(SelectField::new(
                field.id().get(),
                field.name(),
                kind,
                field.nullable(),
                primary_key,
                database_managed,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let indexes = persisted
        .indexes()
        .iter()
        .map(|index| {
            let field_ids = index
                .key()
                .field_paths()
                .iter()
                .map(|path| path.field_id().get())
                .collect::<Vec<_>>();
            if field_ids.is_empty() {
                return Err(format!(
                    "accepted generated-test index {:?} is not a direct field-path index",
                    index.name(),
                ));
            }
            Ok(SelectIndex::new(index.ordinal(), index.name(), field_ids))
        })
        .collect::<Result<Vec<_>, String>>()?;

    SelectSnapshot::try_new(
        "session-accepted-snapshot-v1",
        accepted.entity_path(),
        accepted.entity_name(),
        persisted.version().get(),
        fields,
        indexes,
    )
    .map_err(|error| error.to_string())
}

fn seed_generated_select_fixture(
    session: &DbSession<SessionSqlCanister>,
    generated: &GeneratedSelectCase,
) -> Result<(), InternalError> {
    for row in generated.fixture().rows() {
        session.insert(SessionSqliteReferenceEntity {
            id: Ulid::generate(),
            name: generated_text_value(row, generated, "name"),
            age: generated_integer_value(row, generated, "age"),
            rank: generated_integer_value(row, generated, "rank"),
            active: generated_boolean_value(row, generated, "active"),
        })?;
    }

    Ok(())
}

fn generated_text_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> String {
    let Some(GeneratedValue::Text(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry text field {field:?}",
            generated.identity().id(),
        );
    };
    value.clone()
}

fn generated_integer_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> i64 {
    let Some(GeneratedValue::Integer(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry integer field {field:?}",
            generated.identity().id(),
        );
    };
    *value
}

fn generated_boolean_value(
    row: &icydb_testing_sql_generator::GeneratedFixtureRow,
    generated: &GeneratedSelectCase,
    field: &str,
) -> bool {
    let Some(GeneratedValue::Boolean(value)) = row.value_by_field_name(generated.snapshot(), field)
    else {
        panic!(
            "generated case {:?} should carry boolean field {field:?}",
            generated.identity().id(),
        );
    };
    *value
}

fn generated_rejection_matches(error: &QueryError, violation: SelectViolation) -> bool {
    let expected = violation.expected_rejection();
    match expected {
        SelectExpectedRejection::InvalidClauseOrder | SelectExpectedRejection::LimitOverflow => {
            error.diagnostic_code() == DiagnosticCode::RuntimeUnsupported
        }
        SelectExpectedRejection::UnknownField
        | SelectExpectedRejection::UnsupportedFunctionSignature
        | SelectExpectedRejection::WrongOperatorType => {
            let QueryError::Plan(plan) = error else {
                return false;
            };
            let PlanError::User(user) = plan.as_ref() else {
                return false;
            };
            let PlanUserError::Expr(expression) = user.as_ref() else {
                return false;
            };
            matches!(
                (expected, expression.as_ref()),
                (
                    SelectExpectedRejection::UnknownField,
                    ExprPlanError::UnknownField { .. }
                ) | (
                    SelectExpectedRejection::UnsupportedFunctionSignature,
                    ExprPlanError::InvalidFunctionArgument { .. }
                ) | (
                    SelectExpectedRejection::WrongOperatorType,
                    ExprPlanError::InvalidBinaryOperands { .. }
                )
            )
        }
    }
}

fn normalize_icydb_result(
    scenario: SqliteReferenceScenario,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    normalize_icydb_result_with_contract(
        scenario.id(),
        scenario.columns(),
        scenario.row_order(),
        result,
    )
}

fn normalize_generated_icydb_result(
    generated: &GeneratedSelectCase,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    let kinds = generated
        .query()
        .projection_kinds(generated.snapshot())
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|kind| match kind {
            SelectValueKind::Boolean => SqliteReferenceColumnKind::Boolean,
            SelectValueKind::Decimal => SqliteReferenceColumnKind::Decimal,
            SelectValueKind::Integer => SqliteReferenceColumnKind::Integer,
            SelectValueKind::Text => SqliteReferenceColumnKind::Text,
        })
        .collect::<Vec<_>>();
    let row_order = match generated.query().result_order() {
        SelectResultOrder::Ordered => SqliteReferenceRowOrder::Ordered,
        SelectResultOrder::Unordered => SqliteReferenceRowOrder::Unordered,
    };
    normalize_icydb_result_with_contract(generated.identity().id(), &kinds, row_order, result)
}

fn normalize_icydb_result_with_contract(
    scenario_id: &str,
    kinds: &[SqliteReferenceColumnKind],
    row_order: SqliteReferenceRowOrder,
    result: SqlStatementResult,
) -> Result<SqliteReferenceResult, String> {
    let (columns, rows) = match result {
        SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
            ..
        } => {
            verify_row_count(scenario_id, row_count, rows.len())?;
            (columns, rows)
        }
        SqlStatementResult::Grouped {
            columns,
            rows,
            row_count,
            next_cursor,
            ..
        } => {
            verify_row_count(scenario_id, row_count, rows.len())?;
            if next_cursor.is_some() {
                return Err(
                    "compact SQLite reference profile unexpectedly produced a cursor".into(),
                );
            }
            let rows = rows
                .into_iter()
                .map(|row| {
                    row.group_key()
                        .iter()
                        .chain(row.aggregate_values())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect();
            (columns, rows)
        }
        other => {
            return Err(format!(
                "scenario {scenario_id:?} returned unsupported statement payload {other:?}",
            ));
        }
    };
    if columns.len() != kinds.len() {
        return Err(format!(
            "scenario {scenario_id:?} returned {} columns for {} declared mappings",
            columns.len(),
            kinds.len(),
        ));
    }
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(row_index, row)| normalize_icydb_row(scenario_id, kinds, row_index, row))
        .collect::<Result<Vec<_>, _>>()?;

    SqliteReferenceResult::try_new(columns, rows, row_order).map_err(|error| error.to_string())
}

fn normalize_icydb_row(
    scenario_id: &str,
    kinds: &[SqliteReferenceColumnKind],
    row_index: usize,
    row: Vec<OutputValue>,
) -> Result<Vec<SqliteReferenceValue>, String> {
    if row.len() != kinds.len() {
        return Err(format!(
            "scenario {scenario_id:?} row {row_index} returned {} values for {} declared mappings",
            row.len(),
            kinds.len(),
        ));
    }

    row.into_iter()
        .zip(kinds.iter().copied())
        .enumerate()
        .map(|(column, (value, kind))| normalize_icydb_value(scenario_id, column, kind, value))
        .collect()
}

fn normalize_icydb_value(
    scenario_id: &str,
    column: usize,
    kind: SqliteReferenceColumnKind,
    value: OutputValue,
) -> Result<SqliteReferenceValue, String> {
    let normalized = match (kind, value) {
        (_, OutputValue::Null) => SqliteReferenceValue::Null,
        (SqliteReferenceColumnKind::Blob, OutputValue::Blob(value)) => {
            SqliteReferenceValue::Blob(value)
        }
        (SqliteReferenceColumnKind::Boolean, OutputValue::Bool(value)) => {
            SqliteReferenceValue::Boolean(value)
        }
        (SqliteReferenceColumnKind::Decimal, OutputValue::Decimal(value)) => {
            SqliteReferenceValue::Decimal {
                mantissa: value.mantissa(),
                scale: value.scale(),
            }
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int64(value)) => {
            SqliteReferenceValue::Integer(value)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Int128(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Int128 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat64(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Nat64 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Integer, OutputValue::Nat128(value)) => {
            SqliteReferenceValue::Integer(i64::try_from(value).map_err(|_| {
                format!(
                    "scenario {scenario_id:?} column {column} returned non-SQLite Nat128 {value}",
                )
            })?)
        }
        (SqliteReferenceColumnKind::Text, OutputValue::Text(value)) => {
            SqliteReferenceValue::Text(value)
        }
        (kind, value) => {
            return Err(format!(
                "scenario {scenario_id:?} column {column} returned {value:?} for {kind:?}",
            ));
        }
    };

    Ok(normalized)
}

fn verify_row_count(scenario_id: &str, row_count: u32, rows_len: usize) -> Result<(), String> {
    let rows_len = u32::try_from(rows_len).map_err(|_| {
        format!("scenario {scenario_id:?} returned a row vector too large for its public count")
    })?;
    if row_count != rows_len {
        return Err(format!(
            "scenario {scenario_id:?} reported {row_count} rows but returned {rows_len}",
        ));
    }

    Ok(())
}
