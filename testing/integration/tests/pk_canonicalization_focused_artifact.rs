use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use candid::CandidType;
use icydb_testing_integration::{install_fixture_canister, reset_icydb_fixtures};
use serde::Deserialize;
use serde_json::{Map, Value};

const MANIFEST_RELATIVE_PATH: &str =
    "docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.json";
const FOCUSED_BEFORE_ENV: &str = "ICYDB_197_PK_FOCUSED_BEFORE_JSON";
const FOCUSED_AFTER_ENV: &str = "ICYDB_197_PK_FOCUSED_AFTER_JSON";
const FOCUSED_CURRENT_OUT_ENV: &str = "ICYDB_197_PK_FOCUSED_CURRENT_OUT";
const FOCUSED_DELTA_ENV: &str = "ICYDB_197_PK_FOCUSED_DELTA_JSON";
const FOCUSED_DELTA_OUT_ENV: &str = "ICYDB_197_PK_FOCUSED_DELTA_OUT";

#[derive(Debug, Deserialize)]
struct FocusedManifest {
    required_fields: Vec<String>,
    scenarios: Vec<FocusedManifestScenario>,
    closeout_gate_failures: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FocusedManifestScenario {
    scenario_key: String,
    surface: String,
    store: String,
    primary_key_kind: String,
    query_shape: String,
    expected_canonicalization_result: String,
    expected_behavior_change: bool,
    semantic_evidence: Vec<String>,
}

#[derive(CandidType, Debug, Deserialize)]
struct FocusedPkPerfRow {
    scenario_key: String,
    terminal: String,
    selected_access: String,
    admission_result: String,
    error_code: Option<String>,
    total_instructions: u64,
    planner_instructions: u64,
    execute_instructions: u64,
    store_instructions: u64,
    data_store_get: u64,
    index_ranges: u64,
    rows_decoded: u64,
    rows_returned: u64,
    result_signature: String,
    canonicalization_result: String,
    raw_key_count: u32,
    deduplicated_key_count: u32,
    explanation: String,
}

#[test]
fn pk_canonicalization_focused_manifest_is_complete_and_gateable() {
    let manifest = read_manifest();
    let scenario_keys = scenario_keys(&manifest);

    assert_eq!(
        scenario_keys.len(),
        manifest.scenarios.len(),
        "focused 0.197 scenario keys must be unique",
    );
    assert!(
        scenario_keys.len() >= 33,
        "focused 0.197 manifest must keep the full exact-key scenario set",
    );
    for required_key in REQUIRED_SCENARIO_KEYS {
        assert!(
            scenario_keys.contains(*required_key),
            "focused 0.197 manifest is missing required scenario key {required_key}",
        );
    }
    for required_field in REQUIRED_DELTA_FIELDS {
        assert!(
            manifest
                .required_fields
                .iter()
                .any(|field| field == required_field),
            "focused 0.197 manifest is missing required delta field {required_field}",
        );
    }
    for required_failure in REQUIRED_CLOSEOUT_FAILURES {
        assert!(
            manifest
                .closeout_gate_failures
                .iter()
                .any(|failure| failure == required_failure),
            "focused 0.197 manifest is missing closeout failure {required_failure}",
        );
    }
    for scenario in &manifest.scenarios {
        assert!(
            EXPECTED_CANONICALIZATION_RESULTS
                .contains(&scenario.expected_canonicalization_result.as_str()),
            "{} uses unknown canonicalization result {}",
            scenario.scenario_key,
            scenario.expected_canonicalization_result,
        );
        assert!(
            !scenario.semantic_evidence.is_empty(),
            "{} must cite at least one semantic evidence test",
            scenario.scenario_key,
        );
    }
}

#[test]
#[ignore = "reads saved 0.197 focused delta artifact; run manually after focused before/after capture"]
fn pk_canonicalization_focused_delta_covers_manifest() {
    let manifest = read_manifest();
    let delta_path = focused_delta_path();
    let delta = read_json(delta_path.as_path());
    assert_focused_delta_covers_manifest(&manifest, &delta);
}

#[test]
#[ignore = "reads saved 0.197 focused before/after artifacts and writes delta JSON/Markdown"]
fn pk_canonicalization_focused_delta_writes_from_saved_before_after_artifacts() {
    let manifest = read_manifest();
    let before = read_json(required_env_path(FOCUSED_BEFORE_ENV).as_path());
    let after = read_json(required_env_path(FOCUSED_AFTER_ENV).as_path());
    let delta = build_focused_delta(&manifest, &before, &after);
    let delta_path = focused_delta_output_path();
    let markdown_path = delta_path.with_extension("md");

    write_json(delta_path.as_path(), &delta);
    write_markdown(
        markdown_path.as_path(),
        focused_delta_markdown(&delta).as_str(),
    );
    assert_focused_delta_covers_manifest(&manifest, &delta);
}

#[test]
#[ignore = "manual PocketIC capture; writes the current 0.197 focused exact-key artifact"]
fn pk_canonicalization_focused_current_artifact_writes_from_pocketic() {
    let manifest = read_manifest();
    let fixture = install_fixture_canister("sql_perf");
    reset_icydb_fixtures(&fixture);

    let rows = manifest
        .scenarios
        .iter()
        .map(|scenario| {
            let captured_result: Result<FocusedPkPerfRow, icydb::Error> = fixture
                .update_call(
                    "capture_pk_canonicalization_focused_scenario",
                    (scenario.scenario_key.clone(),),
                )
                .unwrap_or_else(|err| {
                    panic!(
                        "{} focused capture should decode from PocketIC: {err}",
                        scenario.scenario_key,
                    )
                });
            let captured = captured_result.unwrap_or_else(|err| {
                panic!(
                    "{} focused capture should succeed: {err:?}",
                    scenario.scenario_key,
                )
            });
            assert_eq!(
                captured.scenario_key, scenario.scenario_key,
                "focused capture scenario key drifted",
            );

            Value::Object(current_row_for_scenario(scenario, &captured))
        })
        .collect::<Vec<_>>();
    let artifact = focused_current_artifact(rows);
    let output_path = focused_current_output_path();
    let markdown_path = output_path.with_extension("md");

    write_json(output_path.as_path(), &artifact);
    write_markdown(
        markdown_path.as_path(),
        focused_current_markdown(&artifact).as_str(),
    );
    assert_focused_current_covers_manifest(&manifest, &artifact);
}

fn assert_focused_delta_covers_manifest(manifest: &FocusedManifest, delta: &Value) {
    let rows = focused_delta_rows(delta);
    let manifest_by_key = manifest
        .scenarios
        .iter()
        .map(|scenario| (scenario.scenario_key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut row_keys = BTreeSet::new();

    for row in rows {
        let scenario_key = string_field(row, "scenario_key");
        row_keys.insert(scenario_key.to_string());
        let scenario = manifest_by_key
            .get(scenario_key)
            .unwrap_or_else(|| panic!("focused delta has unknown scenario key {scenario_key}"));
        for required_field in &manifest.required_fields {
            assert!(
                row.contains_key(required_field),
                "{scenario_key} is missing focused delta field {required_field}",
            );
        }
        assert!(
            row.contains_key("before_result_signature"),
            "{scenario_key} is missing before_result_signature",
        );
        assert!(
            row.contains_key("after_result_signature"),
            "{scenario_key} is missing after_result_signature",
        );
        assert_eq!(
            string_field(row, "canonicalization_result"),
            scenario.expected_canonicalization_result,
            "{scenario_key} canonicalization result drifted",
        );
        assert_eq!(
            bool_field(row, "expected_behavior_change"),
            scenario.expected_behavior_change,
            "{scenario_key} expected behavior-change flag drifted",
        );
        if bool_field(row, "result_signature_changed") {
            assert!(
                !string_field(row, "explanation").is_empty(),
                "{scenario_key} changed result signature without explanation",
            );
        }
    }

    let manifest_keys = scenario_keys(manifest);
    assert_eq!(
        row_keys, manifest_keys,
        "focused delta scenario keys must exactly match the 0.197 manifest",
    );
}

fn assert_focused_current_covers_manifest(manifest: &FocusedManifest, artifact: &Value) {
    let rows = focused_delta_rows(artifact);
    let manifest_by_key = manifest
        .scenarios
        .iter()
        .map(|scenario| (scenario.scenario_key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut row_keys = BTreeSet::new();

    for row in rows {
        let scenario_key = string_field(row, "scenario_key");
        row_keys.insert(scenario_key.to_string());
        let scenario = manifest_by_key
            .get(scenario_key)
            .unwrap_or_else(|| panic!("focused current artifact has unknown key {scenario_key}"));
        for required_field in [
            "scenario_key",
            "surface",
            "store",
            "primary_key_kind",
            "query_shape",
            "terminal",
            "selected_access",
            "admission_result",
            "error_code",
            "total_instructions",
            "planner_instructions",
            "execute_instructions",
            "store_instructions",
            "data_store_get",
            "index_ranges",
            "rows_decoded",
            "rows_returned",
            "result_signature",
            "canonicalization_result",
            "raw_key_count",
            "deduplicated_key_count",
            "expected_behavior_change",
            "explanation",
        ] {
            assert!(
                row.contains_key(required_field),
                "{scenario_key} is missing focused current field {required_field}",
            );
        }
        assert_eq!(
            string_field(row, "canonicalization_result"),
            scenario.expected_canonicalization_result,
            "{scenario_key} canonicalization result drifted",
        );
        assert_eq!(
            bool_field(row, "expected_behavior_change"),
            scenario.expected_behavior_change,
            "{scenario_key} expected behavior-change flag drifted",
        );
        assert!(
            !string_field(row, "explanation").is_empty(),
            "{scenario_key} must explain measured or contract-only status",
        );
    }

    let manifest_keys = scenario_keys(manifest);
    assert_eq!(
        row_keys, manifest_keys,
        "focused current scenario keys must exactly match the 0.197 manifest",
    );
}

const REQUIRED_SCENARIO_KEYS: &[&str] = &[
    "pk.scalar.generated.filter.existing.try_one",
    "pk.scalar.generated.filter.missing.try_one",
    "pk.scalar.generated.by_id.existing.try_one",
    "pk.scalar.external.filter.existing.try_one",
    "pk.scalar.external.by_id.existing.try_one",
    "pk.sql.literal.generated.existing",
    "pk.sql.literal.generated.commuted",
    "pk.sql.parameter.unsupported",
    "pk.sql.literal.generated.wrong_type",
    "pk.in.fluent.empty",
    "pk.in.fluent.one",
    "pk.in.fluent.duplicates",
    "pk.in.fluent.multiple_mixed",
    "pk.in.fluent.raw_terms_over_budget",
    "pk.in.fluent.deduped_over_budget",
    "pk.in.fluent.by_ids.raw_terms_over_budget",
    "pk.in.sql.duplicates.order_asc",
    "pk.in.sql.payload_over_budget",
    "pk.residual.eq.true",
    "pk.residual.eq.false",
    "pk.residual.eq.invalid_existing",
    "pk.residual.eq.invalid_missing",
    "pk.empty.contradictory_eq",
    "pk.empty.eq_and_excluding_in",
    "pk.empty.count",
    "pk.empty.require_one",
    "pk.store.heap.existing",
    "pk.store.journaled.existing",
    "pk.store.heap.deleted",
    "pk.store.journaled.deleted",
    "pk.noncanonical.unique_secondary",
    "pk.noncanonical.partial_composite",
    "pk.noncanonical.expression_wrapped",
];

const REQUIRED_DELTA_FIELDS: &[&str] = &[
    "scenario_key",
    "surface",
    "store",
    "primary_key_kind",
    "query_shape",
    "terminal",
    "before_selected_access",
    "after_selected_access",
    "before_admission_result",
    "after_admission_result",
    "before_error_code",
    "after_error_code",
    "before_total_instructions",
    "after_total_instructions",
    "before_planner_instructions",
    "after_planner_instructions",
    "before_execute_instructions",
    "after_execute_instructions",
    "before_store_instructions",
    "after_store_instructions",
    "before_data_store_get",
    "after_data_store_get",
    "before_index_ranges",
    "after_index_ranges",
    "before_rows_decoded",
    "after_rows_decoded",
    "before_rows_returned",
    "after_rows_returned",
    "before_result_signature",
    "after_result_signature",
    "canonicalization_result",
    "raw_key_count",
    "deduplicated_key_count",
    "result_signature_changed",
    "expected_behavior_change",
    "explanation",
];

const REQUIRED_CLOSEOUT_FAILURES: &[&str] = &[
    "missing_required_scenario_key",
    "missing_route_or_access_facts",
    "missing_instruction_totals",
    "expected_exact_key_scenario_not_by_key_by_keys_or_empty",
    "invalid_or_over_budget_shape_fell_back_to_scan",
    "unexplained_result_signature_change",
    "focused_target_missing_before_or_after_data",
    "performance_claim_without_access_counter_evidence",
];

const EXPECTED_CANONICALIZATION_RESULTS: &[&str] = &[
    "ByKey",
    "ByKeys",
    "Empty",
    "NotApplied",
    "ValidationFailure",
    "UnsupportedByContract",
];

fn read_manifest() -> FocusedManifest {
    let path = workspace_root().join(MANIFEST_RELATIVE_PATH);
    serde_json::from_value(read_json(path.as_path()))
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()))
}

fn focused_delta_path() -> PathBuf {
    env::var(FOCUSED_DELTA_ENV).map_or_else(
        |_| {
            workspace_root().join(
                "docs/design/0.197-deterministic-optimizer-canonicalization/\
                 sql_perf_197_pk_canonicalization_delta.json",
            )
        },
        PathBuf::from,
    )
}

fn focused_delta_output_path() -> PathBuf {
    env::var(FOCUSED_DELTA_OUT_ENV).map_or_else(|_| focused_delta_path(), PathBuf::from)
}

fn focused_current_output_path() -> PathBuf {
    env::var(FOCUSED_CURRENT_OUT_ENV).map_or_else(
        |_| {
            workspace_root().join(
                "docs/design/0.197-deterministic-optimizer-canonicalization/\
                 sql_perf_197_pk_canonicalization_after.json",
            )
        },
        PathBuf::from,
    )
}

fn required_env_path(env_name: &str) -> PathBuf {
    env::var(env_name).map_or_else(
        |_| panic!("set {env_name} to the focused saved-artifact path"),
        PathBuf::from,
    )
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("integration crate should live under testing/integration")
        .to_path_buf()
}

fn read_json(path: &Path) -> Value {
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    serde_json::from_str(raw.as_str())
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()))
}

fn scenario_keys(manifest: &FocusedManifest) -> BTreeSet<String> {
    manifest
        .scenarios
        .iter()
        .map(|scenario| scenario.scenario_key.clone())
        .collect()
}

fn build_focused_delta(manifest: &FocusedManifest, before: &Value, after: &Value) -> Value {
    let before_rows = rows_by_key(before, "before");
    let after_rows = rows_by_key(after, "after");
    let scenarios = manifest
        .scenarios
        .iter()
        .map(|scenario| {
            let before_row = before_rows
                .get(scenario.scenario_key.as_str())
                .copied()
                .unwrap_or_else(|| {
                    panic!(
                        "focused before artifact is missing scenario key {}",
                        scenario.scenario_key
                    )
                });
            let after_row = after_rows
                .get(scenario.scenario_key.as_str())
                .copied()
                .unwrap_or_else(|| {
                    panic!(
                        "focused after artifact is missing scenario key {}",
                        scenario.scenario_key
                    )
                });

            Value::Object(delta_row_for_scenario(scenario, before_row, after_row))
        })
        .collect::<Vec<_>>();

    let mut delta = Map::new();
    delta.insert("line".to_string(), Value::String("0.197".to_string()));
    delta.insert(
        "artifact".to_string(),
        Value::String("sql_perf_197_pk_canonicalization_delta".to_string()),
    );
    delta.insert(
        "generated_from".to_string(),
        Value::String("saved_before_after_artifacts".to_string()),
    );
    delta.insert(
        "scenario_count".to_string(),
        Value::Number((scenarios.len() as u64).into()),
    );
    delta.insert("scenarios".to_string(), Value::Array(scenarios));
    Value::Object(delta)
}

fn focused_current_artifact(rows: Vec<Value>) -> Value {
    let mut artifact = Map::new();
    artifact.insert("line".to_string(), Value::String("0.197".to_string()));
    artifact.insert(
        "artifact".to_string(),
        Value::String("sql_perf_197_pk_canonicalization_after".to_string()),
    );
    artifact.insert(
        "generated_from".to_string(),
        Value::String("pocketic_current_capture".to_string()),
    );
    artifact.insert(
        "scenario_count".to_string(),
        Value::Number((rows.len() as u64).into()),
    );
    artifact.insert("scenarios".to_string(), Value::Array(rows));
    Value::Object(artifact)
}

fn current_row_for_scenario(
    scenario: &FocusedManifestScenario,
    captured: &FocusedPkPerfRow,
) -> Map<String, Value> {
    let mut row = Map::new();
    insert_string(&mut row, "scenario_key", scenario.scenario_key.as_str());
    insert_string(&mut row, "surface", scenario.surface.as_str());
    insert_string(&mut row, "store", scenario.store.as_str());
    insert_string(
        &mut row,
        "primary_key_kind",
        scenario.primary_key_kind.as_str(),
    );
    insert_string(&mut row, "query_shape", scenario.query_shape.as_str());
    insert_string(&mut row, "terminal", captured.terminal.as_str());
    insert_string(
        &mut row,
        "selected_access",
        captured.selected_access.as_str(),
    );
    insert_string(
        &mut row,
        "admission_result",
        captured.admission_result.as_str(),
    );
    row.insert(
        "error_code".to_string(),
        captured
            .error_code
            .as_ref()
            .map_or(Value::Null, |code| Value::String(code.clone())),
    );
    insert_u64(&mut row, "total_instructions", captured.total_instructions);
    insert_u64(
        &mut row,
        "planner_instructions",
        captured.planner_instructions,
    );
    insert_u64(
        &mut row,
        "execute_instructions",
        captured.execute_instructions,
    );
    insert_u64(&mut row, "store_instructions", captured.store_instructions);
    insert_u64(&mut row, "data_store_get", captured.data_store_get);
    insert_u64(&mut row, "index_ranges", captured.index_ranges);
    insert_u64(&mut row, "rows_decoded", captured.rows_decoded);
    insert_u64(&mut row, "rows_returned", captured.rows_returned);
    insert_string(
        &mut row,
        "result_signature",
        captured.result_signature.as_str(),
    );
    insert_string(
        &mut row,
        "canonicalization_result",
        captured.canonicalization_result.as_str(),
    );
    insert_u64(&mut row, "raw_key_count", u64::from(captured.raw_key_count));
    insert_u64(
        &mut row,
        "deduplicated_key_count",
        u64::from(captured.deduplicated_key_count),
    );
    row.insert(
        "expected_behavior_change".to_string(),
        Value::Bool(scenario.expected_behavior_change),
    );
    insert_string(&mut row, "explanation", captured.explanation.as_str());
    row
}

fn delta_row_for_scenario(
    scenario: &FocusedManifestScenario,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
) -> Map<String, Value> {
    let mut row = Map::new();
    insert_string(&mut row, "scenario_key", scenario.scenario_key.as_str());
    insert_string(&mut row, "surface", scenario.surface.as_str());
    insert_string(&mut row, "store", scenario.store.as_str());
    insert_string(
        &mut row,
        "primary_key_kind",
        scenario.primary_key_kind.as_str(),
    );
    insert_string(&mut row, "query_shape", scenario.query_shape.as_str());
    insert_preferred_value(&mut row, "terminal", after, before, "terminal");

    insert_pair(&mut row, before, after, "selected_access");
    insert_pair(&mut row, before, after, "admission_result");
    insert_optional_pair(&mut row, before, after, "error_code");
    insert_pair(&mut row, before, after, "total_instructions");
    insert_pair(&mut row, before, after, "planner_instructions");
    insert_pair(&mut row, before, after, "execute_instructions");
    insert_pair(&mut row, before, after, "store_instructions");
    insert_pair(&mut row, before, after, "data_store_get");
    insert_pair(&mut row, before, after, "index_ranges");
    insert_pair(&mut row, before, after, "rows_decoded");
    insert_pair(&mut row, before, after, "rows_returned");

    let before_signature = required_value(before, "result_signature");
    let after_signature = required_value(after, "result_signature");
    row.insert(
        "before_result_signature".to_string(),
        before_signature.clone(),
    );
    row.insert(
        "after_result_signature".to_string(),
        after_signature.clone(),
    );
    row.insert(
        "result_signature_changed".to_string(),
        Value::Bool(before_signature != after_signature),
    );

    insert_required_value(
        &mut row,
        "canonicalization_result",
        after,
        "canonicalization_result",
    );
    insert_required_value(&mut row, "raw_key_count", after, "raw_key_count");
    insert_required_value(
        &mut row,
        "deduplicated_key_count",
        after,
        "deduplicated_key_count",
    );
    row.insert(
        "expected_behavior_change".to_string(),
        Value::Bool(scenario.expected_behavior_change),
    );
    insert_explanation(&mut row, before, after);
    row
}

fn rows_by_key<'a>(
    artifact: &'a Value,
    artifact_name: &str,
) -> BTreeMap<&'a str, &'a Map<String, Value>> {
    let mut rows_by_key = BTreeMap::new();
    for row in focused_delta_rows(artifact) {
        let scenario_key = string_field(row, "scenario_key");
        assert!(
            rows_by_key.insert(scenario_key, row).is_none(),
            "focused {artifact_name} artifact has duplicate scenario key {scenario_key}",
        );
    }
    rows_by_key
}

fn focused_delta_rows(delta: &Value) -> Vec<&Map<String, Value>> {
    let rows = delta
        .as_array()
        .or_else(|| delta.get("scenarios").and_then(Value::as_array))
        .expect("focused delta JSON must be either an array or an object with a scenarios array");

    rows.iter()
        .map(|row| {
            row.as_object()
                .expect("focused delta scenario rows must be JSON objects")
        })
        .collect()
}

fn insert_pair(
    output: &mut Map<String, Value>,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
    field: &str,
) {
    insert_required_value(output, format!("before_{field}").as_str(), before, field);
    insert_required_value(output, format!("after_{field}").as_str(), after, field);
}

fn insert_optional_pair(
    output: &mut Map<String, Value>,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
    field: &str,
) {
    output.insert(format!("before_{field}"), optional_value(before, field));
    output.insert(format!("after_{field}"), optional_value(after, field));
}

fn insert_preferred_value(
    output: &mut Map<String, Value>,
    output_field: &str,
    preferred: &Map<String, Value>,
    fallback: &Map<String, Value>,
    input_field: &str,
) {
    let value = preferred
        .get(input_field)
        .or_else(|| fallback.get(input_field))
        .cloned()
        .unwrap_or_else(|| panic!("focused capture row is missing required field {input_field}"));
    output.insert(output_field.to_string(), value);
}

fn insert_required_value(
    output: &mut Map<String, Value>,
    output_field: &str,
    input: &Map<String, Value>,
    input_field: &str,
) {
    output.insert(output_field.to_string(), required_value(input, input_field));
}

fn insert_string(output: &mut Map<String, Value>, field: &str, value: &str) {
    output.insert(field.to_string(), Value::String(value.to_string()));
}

fn insert_u64(output: &mut Map<String, Value>, field: &str, value: u64) {
    output.insert(field.to_string(), Value::Number(value.into()));
}

fn insert_explanation(
    output: &mut Map<String, Value>,
    before: &Map<String, Value>,
    after: &Map<String, Value>,
) {
    let explanation = after
        .get("explanation")
        .or_else(|| before.get("explanation"))
        .cloned()
        .unwrap_or_else(|| Value::String(String::new()));
    assert!(
        explanation.is_string(),
        "focused capture explanation must be a string",
    );
    output.insert("explanation".to_string(), explanation);
}

fn required_value(row: &Map<String, Value>, field: &str) -> Value {
    row.get(field)
        .cloned()
        .unwrap_or_else(|| panic!("focused capture row is missing required field {field}"))
}

fn optional_value(row: &Map<String, Value>, field: &str) -> Value {
    row.get(field).cloned().unwrap_or(Value::Null)
}

fn string_field<'a>(row: &'a Map<String, Value>, field: &str) -> &'a str {
    row.get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("focused delta field {field} must be a string"))
}

fn bool_field(row: &Map<String, Value>, field: &str) -> bool {
    row.get(field)
        .and_then(Value::as_bool)
        .unwrap_or_else(|| panic!("focused delta field {field} must be a bool"))
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("failed to create {}: {err}", parent.display()));
    }
    let raw = serde_json::to_string_pretty(value)
        .unwrap_or_else(|err| panic!("failed to serialize {}: {err}", path.display()));
    fs::write(path, format!("{raw}\n"))
        .unwrap_or_else(|err| panic!("failed to write {}: {err}", path.display()));
}

fn write_markdown(path: &Path, markdown: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .unwrap_or_else(|err| panic!("failed to create {}: {err}", parent.display()));
    }
    fs::write(path, markdown)
        .unwrap_or_else(|err| panic!("failed to write {}: {err}", path.display()));
}

fn focused_delta_markdown(delta: &Value) -> String {
    let rows = focused_delta_rows(delta);
    let changed_results = rows
        .iter()
        .filter(|row| bool_field(row, "result_signature_changed"))
        .count();
    let expected_behavior_changes = rows
        .iter()
        .filter(|row| bool_field(row, "expected_behavior_change"))
        .count();
    let mut markdown = String::new();

    writeln!(
        markdown,
        "# 0.197 Focused Primary-Key Canonicalization Delta\n"
    )
    .expect("writing markdown to String should not fail");
    writeln!(markdown, "- Scenario rows: {}", rows.len())
        .expect("writing markdown to String should not fail");
    writeln!(markdown, "- Result-signature changes: {changed_results}")
        .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "- Expected behavior-change scenarios: {expected_behavior_changes}\n"
    )
    .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "| Scenario | Canonicalization | Access Before | Access After | Data Gets Before | Data Gets After | Rows Decoded Before | Rows Decoded After | Result Changed |"
    )
    .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |"
    )
    .expect("writing markdown to String should not fail");

    for row in rows {
        writeln!(
            markdown,
            "| `{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} | {} |",
            string_field(row, "scenario_key"),
            string_field(row, "canonicalization_result"),
            markdown_cell(row, "before_selected_access"),
            markdown_cell(row, "after_selected_access"),
            markdown_cell(row, "before_data_store_get"),
            markdown_cell(row, "after_data_store_get"),
            markdown_cell(row, "before_rows_decoded"),
            markdown_cell(row, "after_rows_decoded"),
            bool_field(row, "result_signature_changed"),
        )
        .expect("writing markdown to String should not fail");
    }

    markdown
}

fn focused_current_markdown(artifact: &Value) -> String {
    let rows = focused_delta_rows(artifact);
    let counter_measured = rows
        .iter()
        .filter(|row| {
            row.get("total_instructions")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                > 0
        })
        .count();
    let admitted_counter_measured = rows
        .iter()
        .filter(|row| {
            string_field(row, "admission_result") == "admitted"
                && row
                    .get("total_instructions")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    > 0
        })
        .count();
    let not_measured = rows
        .iter()
        .filter(|row| string_field(row, "admission_result").contains("not_measured"))
        .count();
    let non_admitted = rows
        .iter()
        .filter(|row| string_field(row, "admission_result") != "admitted")
        .count();
    let mut markdown = String::new();

    writeln!(
        markdown,
        "# 0.197 Focused Primary-Key Canonicalization Current Capture\n"
    )
    .expect("writing markdown to String should not fail");
    writeln!(markdown, "- Scenario rows: {}", rows.len())
        .expect("writing markdown to String should not fail");
    writeln!(markdown, "- Counter-measured rows: {counter_measured}")
        .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "- Admitted counter-measured rows: {admitted_counter_measured}"
    )
    .expect("writing markdown to String should not fail");
    writeln!(markdown, "- Contract/not-measured rows: {not_measured}")
        .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "- Non-admitted/fail-closed rows: {non_admitted}\n"
    )
    .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "| Scenario | Canonicalization | Access | Admission | Error | Instructions | data_store.get | Rows Decoded | Rows Returned | Result |"
    )
    .expect("writing markdown to String should not fail");
    writeln!(
        markdown,
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |"
    )
    .expect("writing markdown to String should not fail");

    for row in rows {
        writeln!(
            markdown,
            "| `{}` | `{}` | `{}` | `{}` | `{}` | {} | {} | {} | {} | `{}` |",
            string_field(row, "scenario_key"),
            string_field(row, "canonicalization_result"),
            markdown_cell(row, "selected_access"),
            markdown_cell(row, "admission_result"),
            markdown_cell(row, "error_code"),
            markdown_cell(row, "total_instructions"),
            markdown_cell(row, "data_store_get"),
            markdown_cell(row, "rows_decoded"),
            markdown_cell(row, "rows_returned"),
            markdown_cell(row, "result_signature"),
        )
        .expect("writing markdown to String should not fail");
    }

    markdown
}

fn markdown_cell(row: &Map<String, Value>, field: &str) -> String {
    row.get(field).map_or_else(String::new, markdown_value)
}

fn markdown_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.replace('|', "\\|"),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string().replace('|', "\\|"),
    }
}
