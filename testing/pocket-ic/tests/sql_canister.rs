use candid::{Principal, decode_one, encode_one};
use icydb::db::sql::{SqlQueryResult, SqlQueryRowsOutput};
use icydb_testing_integration::build_quickstart_canister;
use pocket_ic::{PocketIc, PocketIcBuilder};
use std::{
    env, fs,
    path::PathBuf,
    sync::{Mutex, OnceLock},
};

const INIT_CYCLES: u128 = 2_000_000_000_000;
const POCKET_IC_BIN_ENV: &str = "POCKET_IC_BIN";
static POCKET_IC_TEST_LOCK: Mutex<()> = Mutex::new(());
static QUICKSTART_CANISTER_WASM: OnceLock<Vec<u8>> = OnceLock::new();

// PocketIC reuses one per-process port file under the system temp dir.
// Clearing it before every builder run forces each serialized test to connect
// to the server it just spawned instead of inheriting an older port from a
// previous test's server lifecycle.
fn clear_stale_pocket_ic_port_file() {
    let port_file = env::temp_dir().join(format!("pocket_ic_{}.port", std::process::id()));
    let _ = fs::remove_file(&port_file);
}

// Build Pocket-IC with an explicit server binary to avoid implicit network
// downloads during local test execution.
fn new_pocket_ic() -> Option<PocketIc> {
    clear_stale_pocket_ic_port_file();

    let Some(server_binary_raw) = std::env::var_os(POCKET_IC_BIN_ENV) else {
        eprintln!(
            "skipping PocketIC SQL canister integration test: set {POCKET_IC_BIN_ENV} \
             to an executable pocket-ic server binary"
        );

        return None;
    };
    let server_binary = PathBuf::from(server_binary_raw);
    assert!(
        server_binary.is_file(),
        "{POCKET_IC_BIN_ENV} points to {}, but that file does not exist",
        server_binary.display()
    );

    Some(
        PocketIcBuilder::new()
            // Match PocketIc::new() topology expectations: at least one subnet.
            .with_application_subnet()
            .with_server_binary(server_binary)
            .build(),
    )
}

fn build_quickstart_canister_wasm() -> Vec<u8> {
    QUICKSTART_CANISTER_WASM
        .get_or_init(|| {
            let wasm_path = build_quickstart_canister().expect("build quickstart canister");
            fs::read(&wasm_path).unwrap_or_else(|err| {
                panic!(
                    "failed to read built canister wasm at {}: {err}",
                    wasm_path.display()
                )
            })
        })
        .clone()
}

// Execute one PocketIC integration test body and keep teardown panics from
// masking the primary failure when the test is already unwinding.
fn run_with_pocket_ic(test_body: impl FnOnce(&PocketIc)) {
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

    // PocketIC tests must not run concurrently.
    // The PocketIC server and test canister install path are not stable under
    // parallel execution in CI; serialize test bodies to keep runs deterministic.
    let _guard = POCKET_IC_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let Some(pic) = new_pocket_ic() else {
        return;
    };
    let test_result = catch_unwind(AssertUnwindSafe(|| test_body(&pic)));
    let cleanup_result = catch_unwind(AssertUnwindSafe(|| drop(pic)));

    match test_result {
        Ok(()) => {
            if let Err(cleanup_panic) = cleanup_result {
                resume_unwind(cleanup_panic);
            }
        }
        Err(test_panic) => {
            if cleanup_result.is_err() {
                eprintln!(
                    "suppressed secondary PocketIC cleanup panic while propagating primary test panic"
                );
            }
            resume_unwind(test_panic);
        }
    }
}

fn query_result(
    pic: &PocketIc,
    canister_id: Principal,
    sql: &str,
) -> Result<SqlQueryResult, icydb::Error> {
    let query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query",
            encode_one(sql.to_string()).expect("encode query SQL args"),
        )
        .expect("query call should return encoded Result");

    decode_one(&query_bytes).expect("decode query response")
}

fn query_projection_rows(
    pic: &PocketIc,
    canister_id: Principal,
    sql: &str,
    context: &str,
) -> SqlQueryRowsOutput {
    let payload = query_result(pic, canister_id, sql).expect(context);
    match payload {
        SqlQueryResult::Projection(rows) => rows,
        other => panic!("{context}: expected Projection payload, got {other:?}"),
    }
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_smoke_flow() {
    run_with_pocket_ic(|pic| {
        let canister_id = pic.create_canister();
        pic.add_cycles(canister_id, INIT_CYCLES);

        let wasm = build_quickstart_canister_wasm();
        pic.install_canister(
            canister_id,
            wasm,
            encode_one(()).expect("encode init args"),
            None,
        );

        let entities_bytes = pic
            .query_call(
                canister_id,
                Principal::anonymous(),
                "sql_entities",
                encode_one(()).expect("encode sql_entities args"),
            )
            .expect("sql_entities query call should succeed");
        let entities: Vec<String> =
            decode_one(&entities_bytes).expect("decode sql_entities response");
        assert!(entities.iter().any(|name| name == "User"));
        assert!(entities.iter().any(|name| name == "Order"));
        assert!(entities.iter().any(|name| name == "Character"));

        let show_entities_payload = query_result(pic, canister_id, "SHOW ENTITIES")
            .expect("SHOW ENTITIES query should return an Ok payload");
        let show_entities_lines = show_entities_payload.render_lines();
        assert_eq!(
            show_entities_lines.first().map(String::as_str),
            Some("surface=entities"),
            "SHOW ENTITIES output should be tagged as entity-list surface",
        );
        match show_entities_payload {
            SqlQueryResult::ShowEntities {
                entities: show_entities,
            } => {
                assert!(
                    show_entities.iter().any(|entity| entity == "User"),
                    "SHOW ENTITIES payload should include User",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "Order"),
                    "SHOW ENTITIES payload should include Order",
                );
                assert!(
                    show_entities.iter().any(|entity| entity == "Character"),
                    "SHOW ENTITIES payload should include Character",
                );
            }
            other => panic!("SHOW ENTITIES should return ShowEntities payload, got {other:?}"),
        }

        let load_bytes = pic
            .update_call(
                canister_id,
                Principal::anonymous(),
                "fixtures_load_default",
                encode_one(()).expect("encode fixtures_load_default args"),
            )
            .expect("fixtures_load_default update call should succeed");
        let load_result: Result<(), icydb::Error> =
            decode_one(&load_bytes).expect("decode fixtures_load_default response");
        assert!(
            load_result.is_ok(),
            "fixtures_load_default returned error: {load_result:?}"
        );

        let explain_payload = query_result(
            pic,
            canister_id,
            "EXPLAIN SELECT name FROM User ORDER BY name LIMIT 1",
        )
        .expect("EXPLAIN query should return an Ok payload");
        let explain_lines = explain_payload.render_lines();
        assert!(
            !explain_lines.is_empty(),
            "EXPLAIN output should be non-empty"
        );
        assert_eq!(
            explain_lines.first().map(String::as_str),
            Some("surface=explain"),
            "EXPLAIN output should be tagged as explain surface",
        );
        match explain_payload {
            SqlQueryResult::Explain { entity, explain } => {
                assert_eq!(entity, "User");
                assert!(
                    !explain.is_empty(),
                    "EXPLAIN payload should include non-empty explain text",
                );
            }
            other => panic!("EXPLAIN should return Explain payload, got {other:?}"),
        }

        let query_sql = "SELECT name FROM User ORDER BY name LIMIT 1";
        let projection =
            query_projection_rows(pic, canister_id, query_sql, "query endpoint should project");
        assert_eq!(projection.entity, "User");
        assert_eq!(projection.row_count, 1);
        assert_eq!(projection.columns, vec!["name".to_string()]);
        assert_eq!(projection.rows, vec![vec!["alice".to_string()]]);

        let projection_lines = query_result(pic, canister_id, query_sql)
            .expect("projection query should return an Ok payload")
            .render_lines();
        assert!(
            projection_lines
                .first()
                .is_some_and(|line| line.contains("surface=projection")),
            "projection output should be tagged as projection surface",
        );
        assert!(
            projection_lines.iter().any(|line| line.contains("alice")),
            "projection output should include projected row values",
        );

        let reset_bytes = pic
            .update_call(
                canister_id,
                Principal::anonymous(),
                "fixtures_reset",
                encode_one(()).expect("encode fixtures_reset args"),
            )
            .expect("fixtures_reset update call should succeed");
        let reset_result: Result<(), icydb::Error> =
            decode_one(&reset_bytes).expect("decode fixtures_reset response");
        assert!(
            reset_result.is_ok(),
            "fixtures_reset returned error: {reset_result:?}"
        );
    });
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_dispatch_is_entity_keyed_and_deterministic() {
    run_with_pocket_ic(|pic| {
        let canister_id = pic.create_canister();
        pic.add_cycles(canister_id, INIT_CYCLES);

        let wasm = build_quickstart_canister_wasm();
        pic.install_canister(
            canister_id,
            wasm,
            encode_one(()).expect("encode init args"),
            None,
        );

        let load_bytes = pic
            .update_call(
                canister_id,
                Principal::anonymous(),
                "fixtures_load_default",
                encode_one(()).expect("encode fixtures_load_default args"),
            )
            .expect("fixtures_load_default update call should succeed");
        let load_result: Result<(), icydb::Error> =
            decode_one(&load_bytes).expect("decode fixtures_load_default response");
        assert!(
            load_result.is_ok(),
            "fixtures_load_default returned error: {load_result:?}"
        );

        // Property 1: resolution is by parsed SQL entity name for Character.
        let character_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT name FROM Character ORDER BY name ASC LIMIT 1",
            "Character query should return projection rows",
        );
        assert_eq!(character_rows.entity, "Character");
        assert_eq!(character_rows.columns, vec!["name".to_string()]);
        assert_eq!(character_rows.row_count, 1);
        assert_eq!(character_rows.rows, vec![vec!["Alex Ander".to_string()]]);

        // Property 1: resolution is by parsed SQL entity name for User.
        let user_rows = query_projection_rows(
            pic,
            canister_id,
            "SELECT name FROM User ORDER BY name ASC LIMIT 1",
            "User query should return projection rows",
        );
        assert_eq!(user_rows.entity, "User");
        assert_eq!(user_rows.columns, vec!["name".to_string()]);
        assert_eq!(user_rows.row_count, 1);
        assert_eq!(user_rows.rows, vec![vec!["alice".to_string()]]);

        // Property 3: no fallthrough; invalid field on User must be validated as User.
        let bad_user_field_error = query_result(
            pic,
            canister_id,
            "SELECT total_cents FROM User ORDER BY id ASC LIMIT 1",
        )
        .expect_err("bad User field should return error");
        assert!(
            bad_user_field_error
                .message()
                .contains("unknown expression field 'total_cents'"),
            "bad User field should stay on User route: {bad_user_field_error:?}",
        );
        assert!(
            !bad_user_field_error.message().contains("last_error"),
            "bad User field must not include fallback chaining text: {bad_user_field_error:?}",
        );

        // Property 3: no fallthrough; invalid field on Character must be validated as Character.
        let bad_character_field_error = query_result(
            pic,
            canister_id,
            "SELECT age FROM Character ORDER BY id ASC LIMIT 1",
        )
        .expect_err("bad Character field should return error");
        assert!(
            bad_character_field_error
                .message()
                .contains("unknown expression field 'age'"),
            "bad Character field should stay on Character route: {bad_character_field_error:?}",
        );
        assert!(
            !bad_character_field_error.message().contains("last_error"),
            "bad Character field must not include fallback chaining text: {bad_character_field_error:?}",
        );

        // Property 2: unsupported entity errors are immediate, deterministic, and enumerate support.
        let unknown_entity_error =
            query_result(pic, canister_id, "SELECT * FROM MissingEntity LIMIT 1")
                .expect_err("MissingEntity query should return error");
        assert!(
            matches!(
                unknown_entity_error.kind(),
                icydb::error::ErrorKind::Runtime(icydb::error::RuntimeErrorKind::Unsupported)
            ),
            "MissingEntity should map to Runtime::Unsupported: {unknown_entity_error:?}",
        );
        assert!(
            unknown_entity_error
                .message()
                .contains("query endpoint does not support entity 'MissingEntity'"),
            "MissingEntity dispatch error should include unsupported entity detail: {unknown_entity_error:?}",
        );
        assert!(
            unknown_entity_error.message().contains("User")
                && unknown_entity_error.message().contains("Order")
                && unknown_entity_error.message().contains("Character"),
            "MissingEntity dispatch error should enumerate supported entities: {unknown_entity_error:?}",
        );
        assert!(
            !unknown_entity_error.message().contains("last_error"),
            "MissingEntity dispatch error must not include fallback trial chaining details: {unknown_entity_error:?}",
        );

        // EXPLAIN failures should preserve execution parity and expose SQL-surface guidance.
        let explain_unordered_error =
            query_result(pic, canister_id, "EXPLAIN SELECT * FROM Character LIMIT 1")
                .expect_err("unordered EXPLAIN should return error");
        assert!(
            matches!(
                explain_unordered_error.kind(),
                icydb::error::ErrorKind::Query(icydb::error::QueryErrorKind::UnorderedPagination)
            ),
            "unordered EXPLAIN should map to Query::UnorderedPagination: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("Cannot EXPLAIN this SQL statement."),
            "unordered EXPLAIN should include SQL-surface heading: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("SQL:\nSELECT * FROM Character LIMIT 1"),
            "unordered EXPLAIN should include wrapped SQL statement: {explain_unordered_error:?}",
        );
        assert!(
            explain_unordered_error
                .message()
                .contains("EXPLAIN SELECT * FROM Character ORDER BY id ASC LIMIT 1"),
            "unordered EXPLAIN should include stable-order fix suggestion: {explain_unordered_error:?}",
        );
    });
}

#[test]
#[expect(clippy::too_many_lines)]
#[expect(clippy::redundant_closure_for_method_calls)]
fn sql_canister_query_lane_supports_describe_show_indexes_and_show_columns() {
    run_with_pocket_ic(|pic| {
        let canister_id = pic.create_canister();
        pic.add_cycles(canister_id, INIT_CYCLES);

        let wasm = build_quickstart_canister_wasm();
        pic.install_canister(
            canister_id,
            wasm,
            encode_one(()).expect("encode init args"),
            None,
        );

        let load_bytes = pic
            .update_call(
                canister_id,
                Principal::anonymous(),
                "fixtures_load_default",
                encode_one(()).expect("encode fixtures_load_default args"),
            )
            .expect("fixtures_load_default update call should succeed");
        let load_result: Result<(), icydb::Error> =
            decode_one(&load_bytes).expect("decode fixtures_load_default response");
        assert!(
            load_result.is_ok(),
            "fixtures_load_default returned error: {load_result:?}"
        );

        let describe_payload = query_result(pic, canister_id, "DESCRIBE Character")
            .expect("query DESCRIBE should return an Ok payload");
        let describe_lines = describe_payload.render_lines();
        match describe_payload {
            SqlQueryResult::Describe(description) => {
                assert_eq!(description.entity_name(), "Character");
                assert_eq!(description.primary_key(), "id");
                assert!(
                    description
                        .fields()
                        .iter()
                        .any(|field| field.name() == "name"),
                    "describe payload should include the name field",
                );
            }
            other => panic!("query DESCRIBE should return Describe payload, got {other:?}"),
        }
        assert!(
            describe_lines
                .iter()
                .any(|line| line == "entity: Character"),
            "DESCRIBE lines should include canonical entity name",
        );

        let describe_normalized_payload =
            query_result(pic, canister_id, " dEsCrIbE public.Character; ")
                .expect("query normalized DESCRIBE should return an Ok payload");
        match describe_normalized_payload {
            SqlQueryResult::Describe(description) => {
                assert_eq!(description.entity_name(), "Character");
            }
            other => {
                panic!("query normalized DESCRIBE should return Describe payload, got {other:?}")
            }
        }

        let show_indexes_payload = query_result(pic, canister_id, "SHOW INDEXES Character")
            .expect("query SHOW INDEXES should return an Ok payload");
        let show_indexes_lines = show_indexes_payload.render_lines();
        match show_indexes_payload {
            SqlQueryResult::ShowIndexes { entity, indexes } => {
                assert_eq!(entity, "Character");
                assert!(
                    indexes.iter().any(|index| index.contains("PRIMARY KEY")),
                    "SHOW INDEXES payload should include at least the primary-key row",
                );
            }
            other => panic!("query SHOW INDEXES should return ShowIndexes payload, got {other:?}"),
        }
        assert!(
            show_indexes_lines
                .first()
                .is_some_and(|line| line.starts_with("surface=indexes entity=Character")),
            "SHOW INDEXES lines should include deterministic surface header",
        );

        let show_indexes_normalized_payload =
            query_result(pic, canister_id, "sHoW InDeXeS public.Character;")
                .expect("query normalized SHOW INDEXES should return an Ok payload");
        match show_indexes_normalized_payload {
            SqlQueryResult::ShowIndexes { entity, .. } => {
                assert_eq!(entity, "Character");
            }
            other => panic!(
                "query normalized SHOW INDEXES should return ShowIndexes payload, got {other:?}"
            ),
        }

        let show_columns_payload = query_result(pic, canister_id, "SHOW COLUMNS Character")
            .expect("query SHOW COLUMNS should return an Ok payload");
        let show_columns_lines = show_columns_payload.render_lines();
        match show_columns_payload {
            SqlQueryResult::ShowColumns { entity, columns } => {
                assert_eq!(entity, "Character");
                assert!(
                    columns.iter().any(|column| column.name() == "name"),
                    "SHOW COLUMNS payload should include the name field",
                );
                assert!(
                    columns.iter().any(|column| column.primary_key()),
                    "SHOW COLUMNS payload should include one primary-key field",
                );
            }
            other => panic!("query SHOW COLUMNS should return ShowColumns payload, got {other:?}"),
        }
        assert!(
            show_columns_lines
                .first()
                .is_some_and(|line| line.starts_with("surface=columns entity=Character")),
            "SHOW COLUMNS lines should include deterministic surface header",
        );

        let show_columns_normalized_payload =
            query_result(pic, canister_id, "sHoW CoLuMnS public.Character;")
                .expect("query normalized SHOW COLUMNS should return an Ok payload");
        match show_columns_normalized_payload {
            SqlQueryResult::ShowColumns { entity, .. } => {
                assert_eq!(entity, "Character");
            }
            other => panic!(
                "query normalized SHOW COLUMNS should return ShowColumns payload, got {other:?}"
            ),
        }
    });
}
