use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashSet},
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorOrigin,
    db::{SqlQueryExecutionAttribution, sql::SqlQueryResult},
    diagnostic::{DiagnosticCode, ErrorClass},
};
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::{Deserialize, Serialize};

const DEFAULT_MATRIX_LIMIT: usize = 300;
const DEFAULT_RANDOM_CASE_COUNT: usize = 300;
const DEFAULT_TOP_N: usize = 20;
const DEFAULT_RANDOM_SEED: u64 = 0x1cdb_0182_0000_0001;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
struct SqlQueryPerfResult {
    result: SqlQueryResult,
    attribution: SqlQueryExecutionAttribution,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSurface {
    Account,
    Blob,
    HeapUser,
    JournaledUser,
    StableUser,
    User,
}

impl MatrixSurface {
    const fn label(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Blob => "blob",
            Self::HeapUser => "heap_user",
            Self::JournaledUser => "journaled_user",
            Self::StableUser => "stable_user",
            Self::User => "user",
        }
    }

    const fn table(self) -> &'static str {
        match self {
            Self::Account => "PerfAuditAccount",
            Self::Blob => "PerfAuditBlob",
            Self::HeapUser => "PerfAuditHeapUser",
            Self::JournaledUser => "PerfAuditJournaledUser",
            Self::StableUser => "PerfAuditStableUser",
            Self::User => "PerfAuditUser",
        }
    }

    const fn query_method(self) -> &'static str {
        match self {
            Self::Account => "query_account_with_perf",
            Self::Blob => "query_blob_with_perf",
            Self::HeapUser => "query_heap_user_with_perf",
            Self::JournaledUser => "query_journaled_user_with_perf",
            Self::StableUser => "query_stable_user_with_perf",
            Self::User => "query_user_with_perf",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixSource {
    Deterministic,
    Random,
}

impl MatrixSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixMode {
    Deterministic,
    Random,
}

impl MatrixMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Random => "random",
        }
    }

    const fn title(self) -> &'static str {
        match self {
            Self::Deterministic => "SQL Perf Deterministic Matrix",
            Self::Random => "SQL Perf Random Matrix",
        }
    }

    const fn default_report_stem(self) -> &'static str {
        match self {
            Self::Deterministic => "sql_perf_deterministic_matrix",
            Self::Random => "sql_perf_random_matrix",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SqlFragment {
    key: &'static str,
    sql: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MatrixScenario {
    key: String,
    source: MatrixSource,
    surface: MatrixSurface,
    family: String,
    sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixOutcome {
    result_kind: &'static str,
    entity: String,
    row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixSample {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    compile_local_instructions: u64,
    execute_local_instructions: u64,
    planner_local_instructions: u64,
    store_local_instructions: u64,
    executor_local_instructions: u64,
    grouped_stream_local_instructions: u64,
    grouped_fold_local_instructions: u64,
    grouped_finalize_local_instructions: u64,
    pure_covering_decode_local_instructions: u64,
    pure_covering_row_assembly_local_instructions: u64,
    store_get_calls: u64,
    sql_compiled_command_hits: u64,
    sql_compiled_command_misses: u64,
    shared_query_plan_hits: u64,
    shared_query_plan_misses: u64,
    total_local_instructions: u64,
    outcome: MatrixOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixFailure {
    key: String,
    source: String,
    surface: String,
    family: String,
    sql: String,
    code: u16,
    diagnostic_code: u16,
    diagnostic_label: &'static str,
    class: String,
    origin: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct MatrixReport {
    matrix_mode: String,
    generated_scenario_count: usize,
    executed_scenario_count: usize,
    failed_scenario_count: usize,
    matrix_limit: usize,
    random_seed: Option<u64>,
    random_case_count: usize,
    samples: Vec<MatrixSample>,
    failures: Vec<MatrixFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lcg {
    state: u64,
}

impl Lcg {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    const fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("matrix option count should fit u64");
        usize::try_from(self.next() % len).expect("matrix option index should fit usize")
    }

    fn choose<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        &values[self.index(values.len())]
    }
}

fn deterministic_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    scenarios.extend(select_matrix(
        MatrixSurface::User,
        &user_projections(),
        &user_predicates(),
        &user_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(select_matrix(
        MatrixSurface::Account,
        &account_projections(),
        &account_predicates(),
        &account_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(select_matrix(
        MatrixSurface::Blob,
        &blob_projections(),
        &blob_predicates(),
        &blob_orders(),
        &[1, 3, 10],
    ));
    scenarios.extend(storage_backend_mirror_matrix());
    scenarios.extend(aggregate_and_metadata_matrix());

    scenarios
}

fn select_matrix(
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicates: &[SqlFragment],
    orders: &[SqlFragment],
    limits: &[u32],
) -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();

    for projection in projections {
        for predicate in predicates {
            for order in orders {
                for limit in limits {
                    let key = format!(
                        "{}.select.{}.{}.{}.limit{}",
                        surface.label(),
                        projection.key,
                        predicate.key,
                        order.key,
                        limit
                    );
                    let family =
                        format!("select.{}.{}.{}", projection.key, predicate.key, order.key);
                    let sql = select_sql(
                        surface.table(),
                        projection.sql,
                        predicate.sql,
                        order.sql,
                        *limit,
                    );

                    scenarios.push(MatrixScenario {
                        key,
                        source: MatrixSource::Deterministic,
                        surface,
                        family,
                        sql,
                    });
                }
            }
        }
    }

    scenarios
}

fn select_sql(table: &str, projection: &str, predicate: &str, order: &str, limit: u32) -> String {
    let where_clause = if predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {predicate}")
    };
    let order_clause = if order.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {order}")
    };

    format!("SELECT {projection} FROM {table}{where_clause}{order_clause} LIMIT {limit}")
}

fn user_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age, age_nat, rank, active",
        },
        SqlFragment {
            key: "numeric_expr",
            sql: "id, age + rank AS total",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(name) AS lower_name",
        },
    ]
}

fn user_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_prefix",
            sql: "name LIKE 'A%'",
        },
        SqlFragment {
            key: "lower_name_prefix",
            sql: "LOWER(name) LIKE 'a%'",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "age_in",
            sql: "age IN (24, 31, 43)",
        },
        SqlFragment {
            key: "field_compare",
            sql: "age > rank",
        },
    ]
}

fn user_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "age_desc",
            sql: "age DESC, id DESC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
        SqlFragment {
            key: "lower_name_asc",
            sql: "LOWER(name) ASC, id ASC",
        },
        SqlFragment {
            key: "numeric_expr_asc",
            sql: "age + rank ASC, id ASC",
        },
    ]
}

fn account_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, handle",
        },
        SqlFragment {
            key: "wide",
            sql: "id, handle, tier, active, score",
        },
        SqlFragment {
            key: "text_expr",
            sql: "id, LOWER(handle) AS lower_handle",
        },
    ]
}

fn account_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "active_true",
            sql: "active = true",
        },
        SqlFragment {
            key: "tier_gold_active",
            sql: "tier = 'gold' AND active = true",
        },
        SqlFragment {
            key: "handle_prefix_active",
            sql: "handle LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "lower_handle_prefix_active",
            sql: "LOWER(handle) LIKE 'a%' AND active = true",
        },
        SqlFragment {
            key: "score_range",
            sql: "score >= 20",
        },
    ]
}

fn account_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "handle_asc",
            sql: "handle ASC, id ASC",
        },
        SqlFragment {
            key: "handle_desc",
            sql: "handle DESC, id DESC",
        },
        SqlFragment {
            key: "lower_handle_asc",
            sql: "LOWER(handle) ASC, id ASC",
        },
        SqlFragment {
            key: "tier_handle_asc",
            sql: "tier ASC, handle ASC, id ASC",
        },
    ]
}

fn blob_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "metadata",
            sql: "id, label, bucket",
        },
        SqlFragment {
            key: "lengths",
            sql: "id, label, OCTET_LENGTH(thumbnail), OCTET_LENGTH(chunk)",
        },
        SqlFragment {
            key: "thumbnail",
            sql: "id, label, thumbnail",
        },
        SqlFragment {
            key: "payload",
            sql: "id, label, thumbnail, chunk",
        },
    ]
}

fn blob_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "bucket_eq",
            sql: "bucket = 10",
        },
        SqlFragment {
            key: "bucket_range",
            sql: "bucket >= 10 AND bucket < 40",
        },
        SqlFragment {
            key: "label_prefix",
            sql: "label LIKE 'blob-%'",
        },
    ]
}

fn blob_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "bucket_asc",
            sql: "bucket ASC, id ASC",
        },
        SqlFragment {
            key: "label_asc",
            sql: "label ASC, id ASC",
        },
    ]
}

fn storage_backend_mirror_matrix() -> Vec<MatrixScenario> {
    let mut scenarios = Vec::new();
    for surface in [
        MatrixSurface::StableUser,
        MatrixSurface::HeapUser,
        MatrixSurface::JournaledUser,
    ] {
        scenarios.extend(select_matrix(
            surface,
            &storage_mirror_projections(),
            &storage_mirror_predicates(),
            &storage_mirror_orders(),
            &[1, 3, 10],
        ));
    }
    scenarios
}

fn storage_mirror_projections() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk",
            sql: "id",
        },
        SqlFragment {
            key: "narrow",
            sql: "id, name",
        },
        SqlFragment {
            key: "wide",
            sql: "id, name, age",
        },
    ]
}

fn storage_mirror_predicates() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "all",
            sql: "",
        },
        SqlFragment {
            key: "pk_range",
            sql: "id >= 2",
        },
        SqlFragment {
            key: "age_range",
            sql: "age >= 24 AND age < 40",
        },
        SqlFragment {
            key: "name_range",
            sql: "name >= 'a'",
        },
    ]
}

fn storage_mirror_orders() -> Vec<SqlFragment> {
    vec![
        SqlFragment {
            key: "pk_asc",
            sql: "id ASC",
        },
        SqlFragment {
            key: "pk_desc",
            sql: "id DESC",
        },
        SqlFragment {
            key: "age_asc",
            sql: "age ASC, id ASC",
        },
        SqlFragment {
            key: "name_asc",
            sql: "name ASC, id ASC",
        },
    ]
}

fn aggregate_and_metadata_matrix() -> Vec<MatrixScenario> {
    vec![
        scenario(
            "user.aggregate.count_all",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser",
        ),
        scenario(
            "user.aggregate.count_active",
            MatrixSurface::User,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditUser WHERE active = true",
        ),
        scenario(
            "user.aggregate.group_age_count",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT age, COUNT(*) FROM PerfAuditUser GROUP BY age ORDER BY age ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_active_avg_age",
            MatrixSurface::User,
            "aggregate.grouped",
            "SELECT active, AVG(age) FROM PerfAuditUser GROUP BY active ORDER BY active ASC LIMIT 10",
        ),
        scenario(
            "user.aggregate.group_age_having_alias",
            MatrixSurface::User,
            "aggregate.grouped_having",
            "SELECT age, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END) AS high_count FROM PerfAuditUser GROUP BY age HAVING high_count > 0 ORDER BY high_count DESC, age ASC LIMIT 5",
        ),
        scenario(
            "account.aggregate.group_tier_count",
            MatrixSurface::Account,
            "aggregate.grouped",
            "SELECT tier, COUNT(*) FROM PerfAuditAccount WHERE active = true GROUP BY tier ORDER BY tier ASC LIMIT 10",
        ),
        scenario(
            "blob.aggregate.count_bucket",
            MatrixSurface::Blob,
            "aggregate.count",
            "SELECT COUNT(*) FROM PerfAuditBlob WHERE bucket = 10",
        ),
        scenario(
            "user.metadata.explain_pk_limit",
            MatrixSurface::User,
            "metadata.explain",
            "EXPLAIN SELECT id, name FROM PerfAuditUser ORDER BY id ASC LIMIT 1",
        ),
        scenario(
            "user.metadata.describe",
            MatrixSurface::User,
            "metadata.describe",
            "DESCRIBE PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_columns",
            MatrixSurface::User,
            "metadata.show_columns",
            "SHOW COLUMNS PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_indexes",
            MatrixSurface::User,
            "metadata.show_indexes",
            "SHOW INDEXES FROM PerfAuditUser",
        ),
        scenario(
            "user.metadata.show_entities",
            MatrixSurface::User,
            "metadata.show_entities",
            "SHOW ENTITIES",
        ),
    ]
}

fn scenario(
    key: impl Into<String>,
    surface: MatrixSurface,
    family: impl Into<String>,
    sql: impl Into<String>,
) -> MatrixScenario {
    MatrixScenario {
        key: key.into(),
        source: MatrixSource::Deterministic,
        surface,
        family: family.into(),
        sql: sql.into(),
    }
}

fn random_matrix(seed: u64, case_count: usize) -> Vec<MatrixScenario> {
    let mut rng = Lcg::new(seed);
    (0..case_count)
        .map(|index| random_scenario(&mut rng, seed, index))
        .collect()
}

fn random_scenario(rng: &mut Lcg, seed: u64, index: usize) -> MatrixScenario {
    let surface = *rng.choose(&[
        MatrixSurface::User,
        MatrixSurface::Account,
        MatrixSurface::Blob,
    ]);
    let key = format!("random.{seed:016x}.{index:04}.{}", surface.label());

    match surface {
        MatrixSurface::Account => {
            let predicate = random_account_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &account_projections(),
                predicate,
                &account_orders(),
            )
        }
        MatrixSurface::Blob => {
            let predicate = random_blob_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &blob_projections(),
                predicate,
                &blob_orders(),
            )
        }
        MatrixSurface::HeapUser | MatrixSurface::JournaledUser | MatrixSurface::StableUser => {
            let predicate = random_storage_mirror_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &storage_mirror_projections(),
                predicate,
                &storage_mirror_orders(),
            )
        }
        MatrixSurface::User => {
            let predicate = random_user_predicate(rng);
            random_select_scenario(
                rng,
                key,
                surface,
                &user_projections(),
                predicate,
                &user_orders(),
            )
        }
    }
}

fn random_select_scenario(
    rng: &mut Lcg,
    key: String,
    surface: MatrixSurface,
    projections: &[SqlFragment],
    predicate: String,
    orders: &[SqlFragment],
) -> MatrixScenario {
    let projection = rng.choose(projections);
    let order = rng.choose(orders);
    let limit = *rng.choose(&[1, 2, 3, 5, 10]);
    let sql = select_sql(
        surface.table(),
        projection.sql,
        predicate.as_str(),
        order.sql,
        limit,
    );

    MatrixScenario {
        key,
        source: MatrixSource::Random,
        surface,
        family: format!("random.{}.{}", projection.key, order.key),
        sql,
    }
}

fn random_storage_mirror_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        _ => "name >= 'a'".to_string(),
    }
}

fn random_user_predicate(rng: &mut Lcg) -> String {
    match rng.index(8) {
        0 => String::new(),
        1 => format!("id >= {}", rng.choose(&[1, 2, 3, 4])),
        2 => {
            let low = *rng.choose(&[18, 24, 30, 35]);
            let high = low + *rng.choose(&[5, 10, 20]);
            format!("age >= {low} AND age < {high}")
        }
        3 => format!("name LIKE '{}%'", rng.choose(&["A", "B", "C", "D"])),
        4 => format!("LOWER(name) LIKE '{}%'", rng.choose(&["a", "b", "c", "d"])),
        5 => format!("active = {}", rng.choose(&["true", "false"])),
        6 => format!(
            "age IN ({}, {}, {})",
            rng.choose(&[18, 24, 30]),
            rng.choose(&[31, 35, 40]),
            rng.choose(&[43, 45, 50])
        ),
        _ => "age > rank".to_string(),
    }
}

fn random_account_predicate(rng: &mut Lcg) -> String {
    match rng.index(6) {
        0 => String::new(),
        1 => "active = true".to_string(),
        2 => format!(
            "tier = '{}' AND active = true",
            rng.choose(&["free", "gold", "pro"])
        ),
        3 => format!(
            "handle LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        4 => format!(
            "LOWER(handle) LIKE '{}%' AND active = true",
            rng.choose(&["a", "b", "c"])
        ),
        _ => format!("score >= {}", rng.choose(&[10, 20, 30, 40])),
    }
}

fn random_blob_predicate(rng: &mut Lcg) -> String {
    match rng.index(4) {
        0 => String::new(),
        1 => format!("bucket = {}", rng.choose(&[10, 20, 30, 40])),
        2 => {
            let low = *rng.choose(&[10, 20, 30]);
            let high = low + *rng.choose(&[10, 20]);
            format!("bucket >= {low} AND bucket < {high}")
        }
        _ => "label LIKE 'blob-%'".to_string(),
    }
}

fn generated_matrix(mode: MatrixMode) -> Vec<MatrixScenario> {
    match mode {
        MatrixMode::Deterministic => deterministic_matrix(),
        MatrixMode::Random => random_matrix(random_seed(), random_case_count()),
    }
}

fn matrix_limit(total: usize) -> usize {
    match env::var("ICYDB_SQL_PERF_MATRIX_LIMIT") {
        Ok(value) if value == "all" => total,
        Ok(value) => value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_LIMIT should be a positive integer or 'all'")
            .min(total),
        Err(_) => DEFAULT_MATRIX_LIMIT.min(total),
    }
}

fn matrix_mode() -> MatrixMode {
    if let Ok(value) = env::var("ICYDB_SQL_PERF_MATRIX_MODE") {
        return parse_matrix_mode(&value);
    }

    assert!(
        env::var_os("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").is_none()
            && env::var_os("ICYDB_SQL_PERF_MATRIX_SEED").is_none(),
        "set ICYDB_SQL_PERF_MATRIX_MODE=random before using random matrix controls"
    );
    MatrixMode::Deterministic
}

fn parse_matrix_mode(value: &str) -> MatrixMode {
    match value {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!(
            "ICYDB_SQL_PERF_MATRIX_MODE should be 'deterministic' or 'random', got '{other}'"
        ),
    }
}

fn random_case_count() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES").map_or(DEFAULT_RANDOM_CASE_COUNT, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_RANDOM_CASES should be a positive integer")
    })
}

fn random_seed() -> u64 {
    env::var("ICYDB_SQL_PERF_MATRIX_SEED").map_or(DEFAULT_RANDOM_SEED, |value| {
        value
            .parse::<u64>()
            .expect("ICYDB_SQL_PERF_MATRIX_SEED should be an unsigned integer")
    })
}

fn top_n() -> usize {
    env::var("ICYDB_SQL_PERF_MATRIX_TOP").map_or(DEFAULT_TOP_N, |value| {
        value
            .parse::<usize>()
            .expect("ICYDB_SQL_PERF_MATRIX_TOP should be a positive integer")
    })
}

fn install_sql_perf_canister_fixture() -> StandaloneCanisterFixture {
    install_fixture_canister("sql_perf")
}

fn query_surface_with_perf(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<SqlQueryPerfResult, Error> {
    fixture
        .query_call(scenario.surface.query_method(), (scenario.sql.clone(),))
        .unwrap_or_else(|err| panic!("{} should decode: {err}", scenario.surface.query_method()))
}

fn summarize_perf_outcome(result: &SqlQueryResult) -> MatrixOutcome {
    match result {
        SqlQueryResult::Count { entity, row_count } => MatrixOutcome {
            result_kind: "count",
            entity: entity.clone(),
            row_count: usize::try_from(*row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Projection(rows) => MatrixOutcome {
            result_kind: "projection",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Grouped(rows) => MatrixOutcome {
            result_kind: "grouped",
            entity: rows.entity.clone(),
            row_count: usize::try_from(rows.row_count).unwrap_or(usize::MAX),
        },
        SqlQueryResult::Explain { entity, .. } => MatrixOutcome {
            result_kind: "explain",
            entity: entity.clone(),
            row_count: 1,
        },
        SqlQueryResult::Describe(entity) => MatrixOutcome {
            result_kind: "describe",
            entity: entity.entity_name().to_string(),
            row_count: entity.fields().len(),
        },
        SqlQueryResult::ShowIndexes { entity, indexes } => MatrixOutcome {
            result_kind: "show_indexes",
            entity: entity.clone(),
            row_count: indexes.len(),
        },
        SqlQueryResult::ShowColumns { entity, columns } => MatrixOutcome {
            result_kind: "show_columns",
            entity: entity.clone(),
            row_count: columns.len(),
        },
        SqlQueryResult::ShowEntities { entities, .. } => MatrixOutcome {
            result_kind: "show_entities",
            entity: String::new(),
            row_count: entities.len(),
        },
        SqlQueryResult::ShowStores { stores, .. } => MatrixOutcome {
            result_kind: "show_stores",
            entity: String::new(),
            row_count: stores.len(),
        },
        SqlQueryResult::ShowMemory { memory } => MatrixOutcome {
            result_kind: "show_memory",
            entity: String::new(),
            row_count: memory.len(),
        },
        SqlQueryResult::Ddl { entity, .. } => MatrixOutcome {
            result_kind: "__icydb_ddl",
            entity: entity.clone(),
            row_count: 1,
        },
    }
}

fn sample_scenario(
    fixture: &StandaloneCanisterFixture,
    scenario: &MatrixScenario,
) -> Result<MatrixSample, Box<MatrixFailure>> {
    let perf = query_surface_with_perf(fixture, scenario)
        .map_err(|err| Box::new(matrix_failure_from_error(scenario, err)))?;
    let attribution = perf.attribution;
    let grouped = attribution.grouped;
    let pure_covering = attribution.pure_covering;

    Ok(MatrixSample {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        compile_local_instructions: attribution.compile_local_instructions,
        execute_local_instructions: attribution.execute_local_instructions,
        planner_local_instructions: attribution.execution.planner_local_instructions,
        store_local_instructions: attribution.execution.store_local_instructions,
        executor_local_instructions: attribution.execution.executor_local_instructions,
        grouped_stream_local_instructions: grouped
            .map_or(0, |grouped| grouped.stream_local_instructions),
        grouped_fold_local_instructions: grouped
            .map_or(0, |grouped| grouped.fold_local_instructions),
        grouped_finalize_local_instructions: grouped
            .map_or(0, |grouped| grouped.finalize_local_instructions),
        pure_covering_decode_local_instructions: pure_covering
            .map_or(0, |pure_covering| pure_covering.decode_local_instructions),
        pure_covering_row_assembly_local_instructions: pure_covering.map_or(0, |pure_covering| {
            pure_covering.row_assembly_local_instructions
        }),
        store_get_calls: attribution.store_get_calls,
        sql_compiled_command_hits: attribution.cache.sql_compiled_command_hits,
        sql_compiled_command_misses: attribution.cache.sql_compiled_command_misses,
        shared_query_plan_hits: attribution.cache.shared_query_plan_hits,
        shared_query_plan_misses: attribution.cache.shared_query_plan_misses,
        total_local_instructions: attribution.total_local_instructions,
        outcome: summarize_perf_outcome(&perf.result),
    })
}

fn matrix_failure_from_error(scenario: &MatrixScenario, err: Error) -> MatrixFailure {
    let diagnostic_code = err.diagnostic_code();
    MatrixFailure {
        key: scenario.key.clone(),
        source: scenario.source.label().to_string(),
        surface: scenario.surface.label().to_string(),
        family: scenario.family.clone(),
        sql: scenario.sql.clone(),
        code: err.code().raw(),
        diagnostic_code: diagnostic_code.error_code().raw(),
        diagnostic_label: diagnostic_label(diagnostic_code),
        class: error_class_label(err.class()).to_string(),
        origin: format!("{:?}", err.origin()),
    }
}

const fn diagnostic_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "QueryValidate",
        DiagnosticCode::QueryIntent => "QueryIntent",
        DiagnosticCode::QueryPlan => "QueryPlan",
        DiagnosticCode::QueryAccessRequirement => "QueryAccessRequirement",
        DiagnosticCode::QueryUnorderedPagination => "QueryUnorderedPagination",
        DiagnosticCode::QueryInvalidContinuationCursor => "QueryInvalidContinuationCursor",
        DiagnosticCode::QueryNotFound => "QueryNotFound",
        DiagnosticCode::QueryNotUnique => "QueryNotUnique",
        DiagnosticCode::QueryNumericOverflow => "QueryNumericOverflow",
        DiagnosticCode::QueryNumericNotRepresentable => "QueryNumericNotRepresentable",
        DiagnosticCode::QueryUnknownAggregateTargetField => "QueryUnknownAggregateTargetField",
        DiagnosticCode::QueryUnsupportedProjection => "QueryUnsupportedProjection",
        DiagnosticCode::QueryResultShapeMismatch => "QueryResultShapeMismatch",
        DiagnosticCode::QueryUnsupportedSqlFeature => "QueryUnsupportedSqlFeature",
        DiagnosticCode::QuerySqlSurfaceMismatch => "QuerySqlSurfaceMismatch",
        DiagnosticCode::QuerySqlWriteBoundary => "QuerySqlWriteBoundary",
        DiagnosticCode::SchemaDdlAdmission => "SchemaDdlAdmission",
        DiagnosticCode::StoreNotFound => "StoreNotFound",
        DiagnosticCode::StoreCorruption => "StoreCorruption",
        DiagnosticCode::StoreInvariantViolation => "StoreInvariantViolation",
        DiagnosticCode::RuntimeCorruption => "RuntimeCorruption",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => "RuntimeIncompatiblePersistedFormat",
        DiagnosticCode::RuntimeInvariantViolation => "RuntimeInvariantViolation",
        DiagnosticCode::RuntimeConflict => "RuntimeConflict",
        DiagnosticCode::RuntimeNotFound => "RuntimeNotFound",
        DiagnosticCode::RuntimeUnsupported => "RuntimeUnsupported",
        DiagnosticCode::RuntimeInternal => "RuntimeInternal",
    }
}

const fn error_class_label(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Query => "Query",
        ErrorClass::Corruption => "Corruption",
        ErrorClass::IncompatiblePersistedFormat => "IncompatiblePersistedFormat",
        ErrorClass::NotFound => "NotFound",
        ErrorClass::Internal => "Internal",
        ErrorClass::Conflict => "Conflict",
        ErrorClass::Unsupported => "Unsupported",
        ErrorClass::InvariantViolation => "InvariantViolation",
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live two levels below workspace root")
        .to_path_buf()
}

fn report_stem(mode: MatrixMode) -> PathBuf {
    env::var("ICYDB_SQL_PERF_MATRIX_OUT").map_or_else(
        |_| {
            workspace_root()
                .join("artifacts/perf-audit")
                .join(mode.default_report_stem())
        },
        PathBuf::from,
    )
}

fn write_matrix_reports(report: &MatrixReport) {
    let stem = report_stem(matrix_mode_from_report(report));
    if let Some(parent) = stem.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("matrix report directory should be created: {err}"));
    }

    let json_path = stem.with_extension("json");
    let md_path = stem.with_extension("md");
    let json = serde_json::to_string_pretty(report).expect("matrix report should serialize");
    fs::write(&json_path, json)
        .unwrap_or_else(|err| panic!("matrix JSON report should write: {err}"));
    fs::write(&md_path, matrix_markdown(report))
        .unwrap_or_else(|err| panic!("matrix Markdown report should write: {err}"));

    println!("matrix JSON: {}", json_path.display());
    println!("matrix Markdown: {}", md_path.display());
}

fn matrix_markdown(report: &MatrixReport) -> String {
    let mut output = String::new();
    let mode = matrix_mode_from_report(report);
    writeln!(output, "# {}", mode.title()).expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(output, "- matrix mode: {}", report.matrix_mode)
        .expect("write to string should succeed");
    writeln!(
        output,
        "- generated scenarios: {}",
        report.generated_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- executed scenarios: {}",
        report.executed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "- failed scenarios: {}",
        report.failed_scenario_count
    )
    .expect("write to string should succeed");
    writeln!(output, "- matrix limit: {}", report.matrix_limit)
        .expect("write to string should succeed");
    if let Some(seed) = report.random_seed {
        writeln!(output, "- random seed: {seed}").expect("write to string should succeed");
        writeln!(output, "- random cases: {}", report.random_case_count)
            .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");

    append_ranked_table(
        &mut output,
        "Top Total Instructions",
        ranked_by(&report.samples, |sample| sample.total_local_instructions),
    );
    append_ranked_table(
        &mut output,
        "Top Compile Instructions",
        ranked_by(&report.samples, |sample| sample.compile_local_instructions),
    );
    append_ranked_table(
        &mut output,
        "Top Execute Instructions",
        ranked_by(&report.samples, |sample| sample.execute_local_instructions),
    );
    append_ranked_table(
        &mut output,
        "Top Store Instructions",
        ranked_by(&report.samples, |sample| sample.store_local_instructions),
    );
    append_ranked_table(
        &mut output,
        "Top Executor Instructions",
        ranked_by(&report.samples, |sample| sample.executor_local_instructions),
    );
    append_ranked_table(
        &mut output,
        "Top Store Gets",
        ranked_by(&report.samples, |sample| sample.store_get_calls),
    );
    append_storage_backend_comparison_table(&mut output, &report.samples);
    append_failure_table(&mut output, &report.failures);

    output
}

fn matrix_mode_from_report(report: &MatrixReport) -> MatrixMode {
    match report.matrix_mode.as_str() {
        "deterministic" => MatrixMode::Deterministic,
        "random" => MatrixMode::Random,
        other => panic!("matrix report mode should be known, got '{other}'"),
    }
}

fn ranked_by<F>(samples: &[MatrixSample], key: F) -> Vec<&MatrixSample>
where
    F: Fn(&MatrixSample) -> u64,
{
    let mut ranked = samples.iter().collect::<Vec<_>>();
    ranked.sort_by_key(|sample| Reverse(key(sample)));
    ranked.truncate(top_n());
    ranked
}

fn append_ranked_table(output: &mut String, title: &str, samples: Vec<&MatrixSample>) {
    writeln!(output, "## {title}").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Total | Compile | Execute | Planner | Store | Executor | store.get | Rows | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for sample in samples {
        writeln!(
            output,
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            sample.key,
            sample.surface,
            sample.total_local_instructions,
            sample.compile_local_instructions,
            sample.execute_local_instructions,
            sample.planner_local_instructions,
            sample.store_local_instructions,
            sample.executor_local_instructions,
            sample.store_get_calls,
            sample.outcome.row_count,
            sample.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn append_storage_backend_comparison_table(output: &mut String, samples: &[MatrixSample]) {
    let stable_samples =
        storage_samples_by_suffix(samples, MatrixSurface::StableUser, "stable_user.");
    let heap_samples = storage_samples_by_suffix(samples, MatrixSurface::HeapUser, "heap_user.");
    let journaled_samples =
        storage_samples_by_suffix(samples, MatrixSurface::JournaledUser, "journaled_user.");

    let mut rows = stable_samples
        .iter()
        .filter_map(|(suffix, stable)| {
            let stable = *stable;
            let heap = *heap_samples.get(suffix)?;
            let journaled = *journaled_samples.get(suffix)?;

            Some((suffix.as_str(), stable, heap, journaled))
        })
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return;
    }

    rows.sort_by_key(|(_, stable, heap, journaled)| {
        Reverse(
            absolute_delta(
                heap.total_local_instructions,
                stable.total_local_instructions,
            )
            .max(absolute_delta(
                journaled.total_local_instructions,
                stable.total_local_instructions,
            )),
        )
    });
    rows.truncate(top_n());

    writeln!(output, "## Stable vs Heap vs Journaled Storage Mirror")
        .expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Stable Total | Heap Total | Heap Delta | Heap Ratio | Journaled Total | Journaled Delta | Journaled Ratio | Stable Store | Heap Store | Journaled Store | SQL |",
    )
    .expect("write to string should succeed");
    writeln!(
        output,
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    .expect("write to string should succeed");
    for (suffix, stable, heap, journaled) in rows {
        writeln!(
            output,
            "| `{suffix}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | `{}` |",
            stable.total_local_instructions,
            heap.total_local_instructions,
            signed_delta(
                heap.total_local_instructions,
                stable.total_local_instructions
            ),
            ratio_text(
                heap.total_local_instructions,
                stable.total_local_instructions
            ),
            journaled.total_local_instructions,
            signed_delta(
                journaled.total_local_instructions,
                stable.total_local_instructions
            ),
            ratio_text(
                journaled.total_local_instructions,
                stable.total_local_instructions
            ),
            stable.store_local_instructions,
            heap.store_local_instructions,
            journaled.store_local_instructions,
            journaled.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

fn storage_samples_by_suffix<'a>(
    samples: &'a [MatrixSample],
    surface: MatrixSurface,
    prefix: &str,
) -> BTreeMap<String, &'a MatrixSample> {
    samples
        .iter()
        .filter(|sample| sample.surface == surface.label())
        .filter_map(|sample| {
            sample
                .key
                .strip_prefix(prefix)
                .map(|suffix| (suffix.to_string(), sample))
        })
        .collect()
}

const fn absolute_delta(value: u64, baseline: u64) -> u64 {
    value.abs_diff(baseline)
}

fn signed_delta(value: u64, baseline: u64) -> String {
    if value >= baseline {
        format!("+{}", value - baseline)
    } else {
        format!("-{}", baseline - value)
    }
}

fn ratio_text(value: u64, baseline: u64) -> String {
    if baseline == 0 {
        return "n/a".to_string();
    }

    let scaled = value.saturating_mul(100) / baseline;
    format!("{}.{:02}x", scaled / 100, scaled % 100)
}

fn append_failure_table(output: &mut String, failures: &[MatrixFailure]) {
    if failures.is_empty() {
        return;
    }

    writeln!(output, "## Failed Generated Scenarios").expect("write to string should succeed");
    writeln!(output).expect("write to string should succeed");
    writeln!(
        output,
        "| Scenario | Surface | Code | Diagnostic | Class | Origin | SQL |"
    )
    .expect("write to string should succeed");
    writeln!(output, "|---|---|---:|---|---|---|---|").expect("write to string should succeed");
    for failure in failures.iter().take(top_n()) {
        writeln!(
            output,
            "| `{}` | {} | {} | {} ({}) | {} | {} | `{}` |",
            failure.key,
            failure.surface,
            failure.code,
            failure.diagnostic_label,
            failure.diagnostic_code,
            failure.class,
            failure.origin,
            failure.sql.replace('|', "\\|"),
        )
        .expect("write to string should succeed");
    }
    writeln!(output).expect("write to string should succeed");
}

#[test]
fn sql_perf_matrix_failures_use_stable_diagnostic_labels() {
    let scenario = scenario(
        "user.failure.query_plan",
        MatrixSurface::User,
        "failure.query_plan",
        "SELECT id FROM PerfAuditUser ORDER BY unsupported_expression",
    );
    let failure = matrix_failure_from_error(
        &scenario,
        Error::from_code(DiagnosticCode::QueryPlan, ErrorOrigin::Query),
    );

    assert_eq!(failure.code, 3);
    assert_eq!(failure.diagnostic_code, 3);
    assert_eq!(failure.diagnostic_label, "QueryPlan");
    assert_eq!(failure.class, "Query");
    assert_eq!(failure.origin, "Query");
}

fn print_matrix_summary(report: &MatrixReport) {
    println!("{}", matrix_markdown(report));
}

#[test]
fn sql_perf_generated_matrix_has_stable_shape() {
    let deterministic = deterministic_matrix();
    assert!(
        deterministic.len() >= 1_000,
        "deterministic matrix should be broad enough to hunt hotspots; got {}",
        deterministic.len(),
    );
    assert_eq!(
        deterministic.first().map(|scenario| scenario.key.as_str()),
        Some("user.select.pk.all.pk_asc.limit1"),
    );

    let mut keys = HashSet::new();
    for scenario in &deterministic {
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate generated scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT")
                || scenario.sql.starts_with("EXPLAIN")
                || scenario.sql.starts_with("DESCRIBE")
                || scenario.sql.starts_with("SHOW"),
            "generated scenario '{}' should use supported SQL syntax",
            scenario.key,
        );
    }
}

#[test]
fn sql_perf_random_matrix_has_seeded_stable_shape() {
    let random = random_matrix(DEFAULT_RANDOM_SEED, 20);
    assert_eq!(random.len(), 20);
    assert_eq!(
        random.first().map(|scenario| scenario.key.as_str()),
        Some("random.1cdb018200000001.0000.blob"),
    );

    let mut keys = HashSet::new();
    for scenario in &random {
        assert_eq!(scenario.source, MatrixSource::Random);
        assert!(
            keys.insert(scenario.key.as_str()),
            "duplicate random scenario key '{}'",
            scenario.key,
        );
        assert!(
            scenario.sql.starts_with("SELECT"),
            "random scenario '{}' should use supported SELECT syntax",
            scenario.key,
        );
    }
}

#[test]
fn sql_perf_matrix_storage_backend_comparison_pairs_all_storage_mirrors() {
    let samples = vec![
        storage_matrix_sample(
            "stable_user.select.pk.all.pk_asc.limit1",
            "stable_user",
            100,
            30,
        ),
        storage_matrix_sample("heap_user.select.pk.all.pk_asc.limit1", "heap_user", 80, 10),
        storage_matrix_sample(
            "journaled_user.select.pk.all.pk_asc.limit1",
            "journaled_user",
            70,
            12,
        ),
    ];
    let report = MatrixReport {
        matrix_mode: MatrixMode::Deterministic.label().to_string(),
        generated_scenario_count: samples.len(),
        executed_scenario_count: samples.len(),
        failed_scenario_count: 0,
        matrix_limit: samples.len(),
        random_seed: None,
        random_case_count: 0,
        samples,
        failures: Vec::new(),
    };

    let markdown = matrix_markdown(&report);

    assert!(
        markdown.contains("Stable vs Heap vs Journaled Storage Mirror"),
        "storage mirror report should include the comparison table",
    );
    assert!(
        markdown.contains("Heap Total"),
        "storage mirror report should include heap totals",
    );
    assert!(
        markdown.contains("| `select.pk.all.pk_asc.limit1` | 100 | 80 | -20 | 0.80x | 70 | -30 | 0.70x | 30 | 10 | 12 |"),
        "storage mirror report should pair stable, heap, and journaled by scenario suffix",
    );
}

fn storage_matrix_sample(key: &str, surface: &str, total: u64, store: u64) -> MatrixSample {
    MatrixSample {
        key: key.to_string(),
        source: MatrixSource::Deterministic.label().to_string(),
        surface: surface.to_string(),
        family: "select.pk.all.pk_asc".to_string(),
        sql: "SELECT id FROM PerfAuditStableUser ORDER BY id ASC LIMIT 1".to_string(),
        compile_local_instructions: 1,
        execute_local_instructions: total.saturating_sub(1),
        planner_local_instructions: 0,
        store_local_instructions: store,
        executor_local_instructions: total.saturating_sub(store),
        grouped_stream_local_instructions: 0,
        grouped_fold_local_instructions: 0,
        grouped_finalize_local_instructions: 0,
        pure_covering_decode_local_instructions: 0,
        pure_covering_row_assembly_local_instructions: 0,
        store_get_calls: 1,
        sql_compiled_command_hits: 0,
        sql_compiled_command_misses: 1,
        shared_query_plan_hits: 0,
        shared_query_plan_misses: 1,
        total_local_instructions: total,
        outcome: MatrixOutcome {
            result_kind: "projection",
            entity: "PerfAuditStableUser".to_string(),
            row_count: 1,
        },
    }
}

#[test]
#[ignore = "expensive PocketIC hotspot scan; run manually with --ignored --nocapture"]
fn sql_perf_generated_matrix_reports_hotspots() {
    let fixture = install_sql_perf_canister_fixture();
    reset_icydb_fixtures(&fixture);

    let mode = matrix_mode();
    let scenarios = generated_matrix(mode);
    let generated_scenario_count = scenarios.len();
    let matrix_limit = matrix_limit(generated_scenario_count);
    let selected = scenarios.into_iter().take(matrix_limit).collect::<Vec<_>>();
    let mut samples = Vec::new();
    let mut failures = Vec::new();
    for scenario in &selected {
        match sample_scenario(&fixture, scenario) {
            Ok(sample) => samples.push(sample),
            Err(failure) => failures.push(*failure),
        }
    }
    let random_case_count = if mode == MatrixMode::Random {
        random_case_count()
    } else {
        0
    };

    let report = MatrixReport {
        matrix_mode: mode.label().to_string(),
        generated_scenario_count,
        executed_scenario_count: samples.len(),
        failed_scenario_count: failures.len(),
        matrix_limit,
        random_seed: (mode == MatrixMode::Random).then(random_seed),
        random_case_count,
        samples,
        failures,
    };

    write_matrix_reports(&report);
    print_matrix_summary(&report);
}
