use candid::{Principal, decode_one, encode_one};
use icydb::db::sql::SqlQueryRowsOutput;
use icydb_testing_integration::build_sql_test_canister;
use pocket_ic::{PocketIc, PocketIcBuilder};
use std::{fs, path::PathBuf};

const INIT_CYCLES: u128 = 2_000_000_000_000;
const POCKET_IC_BIN_ENV: &str = "POCKET_IC_BIN";

// Build Pocket-IC with an explicit server binary to avoid implicit network
// downloads during local test execution.
fn new_pocket_ic() -> PocketIc {
    let Some(server_binary_raw) = std::env::var_os(POCKET_IC_BIN_ENV) else {
        panic!(
            "set {POCKET_IC_BIN_ENV} to an executable pocket-ic server binary; \
             these tests disable implicit PocketIC downloads"
        );
    };
    let server_binary = PathBuf::from(server_binary_raw);
    assert!(
        server_binary.is_file(),
        "{POCKET_IC_BIN_ENV} points to {}, but that file does not exist",
        server_binary.display()
    );

    PocketIcBuilder::new()
        // Match PocketIc::new() topology expectations: at least one subnet.
        .with_application_subnet()
        .with_server_binary(server_binary)
        .build()
}

fn build_sql_test_canister_wasm() -> Vec<u8> {
    let wasm_path = build_sql_test_canister().expect("build sql_test canister");
    fs::read(&wasm_path).unwrap_or_else(|err| {
        panic!(
            "failed to read built canister wasm at {}: {err}",
            wasm_path.display()
        )
    })
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_smoke_flow() {
    let pic = new_pocket_ic();

    let canister_id = pic.create_canister();
    pic.add_cycles(canister_id, INIT_CYCLES);

    let wasm = build_sql_test_canister_wasm();
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
    let entities: Vec<String> = decode_one(&entities_bytes).expect("decode sql_entities response");
    assert!(entities.iter().any(|name| name == "User"));
    assert!(entities.iter().any(|name| name == "Order"));
    assert!(entities.iter().any(|name| name == "Character"));

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

    let explain_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query",
            encode_one("EXPLAIN SELECT name FROM User ORDER BY name LIMIT 1".to_string())
                .expect("encode explain query args"),
        )
        .expect("EXPLAIN query call should succeed");
    let explain_result: Result<Vec<String>, icydb::Error> =
        decode_one(&explain_bytes).expect("decode explain query response");
    let explain_output = explain_result.expect("EXPLAIN query should return an Ok payload");
    assert!(
        !explain_output.is_empty(),
        "EXPLAIN output should be non-empty"
    );
    assert_eq!(
        explain_output.first().map(String::as_str),
        Some("surface=explain"),
        "EXPLAIN output should be tagged as explain surface",
    );

    let query_sql = "SELECT name FROM User ORDER BY name LIMIT 1".to_string();
    let query_arg = encode_one(query_sql.clone()).expect("encode query args");
    let query_bytes = pic
        .query_call(canister_id, Principal::anonymous(), "query", query_arg)
        .expect("query call should succeed");
    let query_result: Result<Vec<String>, icydb::Error> =
        decode_one(&query_bytes).expect("decode query response");
    let output = query_result.expect("query endpoint should return an Ok projection");
    assert!(
        output
            .first()
            .is_some_and(|line| line.contains("surface=projection")),
        "query output should be tagged as projection surface",
    );
    assert!(
        output.iter().any(|line| line.contains("alice")),
        "pretty query output should include projected row values",
    );

    let query_rows_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one(query_sql).expect("encode query_rows args"),
        )
        .expect("query_rows call should succeed");
    let query_rows_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&query_rows_bytes).expect("decode query_rows response");
    let query_rows = query_rows_result.expect("query_rows endpoint should return structured rows");
    assert_eq!(query_rows.entity, "User");
    assert_eq!(query_rows.row_count, 1);
    assert_eq!(query_rows.columns, vec!["name".to_string()]);
    assert_eq!(query_rows.rows, vec![vec!["alice".to_string()]]);

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
}

#[test]
#[expect(clippy::too_many_lines)]
fn sql_canister_dispatch_is_entity_keyed_and_deterministic() {
    let pic = new_pocket_ic();

    let canister_id = pic.create_canister();
    pic.add_cycles(canister_id, INIT_CYCLES);

    let wasm = build_sql_test_canister_wasm();
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
    let character_query_rows_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one("SELECT name FROM Character ORDER BY name ASC LIMIT 1".to_string())
                .expect("encode Character query_rows args"),
        )
        .expect("Character query_rows call should succeed");
    let character_query_rows_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&character_query_rows_bytes).expect("decode Character query_rows response");
    let character_query_rows =
        character_query_rows_result.expect("Character query_rows should return Ok");
    assert_eq!(character_query_rows.entity, "Character");
    assert_eq!(character_query_rows.columns, vec!["name".to_string()]);
    assert_eq!(character_query_rows.row_count, 1);
    assert_eq!(
        character_query_rows.rows,
        vec![vec!["Alex Ander".to_string()]]
    );

    // Property 1: resolution is by parsed SQL entity name for User.
    let user_query_rows_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one("SELECT name FROM User ORDER BY name ASC LIMIT 1".to_string())
                .expect("encode User query_rows args"),
        )
        .expect("User query_rows call should succeed");
    let user_query_rows_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&user_query_rows_bytes).expect("decode User query_rows response");
    let user_query_rows = user_query_rows_result.expect("User query_rows should return Ok");
    assert_eq!(user_query_rows.entity, "User");
    assert_eq!(user_query_rows.columns, vec!["name".to_string()]);
    assert_eq!(user_query_rows.row_count, 1);
    assert_eq!(user_query_rows.rows, vec![vec!["alice".to_string()]]);

    // Property 3: no fallthrough; invalid field on User must be validated as User.
    let bad_user_field_query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one("SELECT total_cents FROM User ORDER BY id ASC LIMIT 1".to_string())
                .expect("encode bad User field query"),
        )
        .expect("bad User field query call should succeed");
    let bad_user_field_query_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&bad_user_field_query_bytes).expect("decode bad User field response");
    let bad_user_field_error =
        bad_user_field_query_result.expect_err("bad User field should return error");
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
    let bad_character_field_query_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one("SELECT age FROM Character ORDER BY id ASC LIMIT 1".to_string())
                .expect("encode bad Character field query"),
        )
        .expect("bad Character field query call should succeed");
    let bad_character_field_query_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&bad_character_field_query_bytes).expect("decode bad Character field response");
    let bad_character_field_error =
        bad_character_field_query_result.expect_err("bad Character field should return error");
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
    let unknown_query_rows_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query_rows",
            encode_one("SELECT * FROM MissingEntity LIMIT 1".to_string())
                .expect("encode MissingEntity query_rows args"),
        )
        .expect("MissingEntity query_rows call should succeed");
    let unknown_query_rows_result: Result<SqlQueryRowsOutput, icydb::Error> =
        decode_one(&unknown_query_rows_bytes).expect("decode MissingEntity query_rows response");
    let unknown_entity_error =
        unknown_query_rows_result.expect_err("MissingEntity query should return error");

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
    let explain_unordered_bytes = pic
        .query_call(
            canister_id,
            Principal::anonymous(),
            "query",
            encode_one("EXPLAIN SELECT * FROM Character LIMIT 1".to_string())
                .expect("encode unordered EXPLAIN query"),
        )
        .expect("unordered EXPLAIN query call should succeed");
    let explain_unordered_result: Result<Vec<String>, icydb::Error> =
        decode_one(&explain_unordered_bytes).expect("decode unordered EXPLAIN query response");
    let explain_unordered_error =
        explain_unordered_result.expect_err("unordered EXPLAIN should return error");

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
}
