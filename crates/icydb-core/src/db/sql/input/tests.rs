use super::*;
use crate::{
    db::{
        QueryError,
        query::admission::input::{
            MAX_QUERY_INPUT_BYTES, MAX_QUERY_INPUT_DEPTH, MAX_QUERY_INPUT_NODES,
        },
        sql::{
            lowering::{prepare_sql_statement, validate_sql_bindings},
            parser::{SqlExprUnaryOp, SqlParseError, parse_sql},
        },
    },
    value::{InputValue, Value},
};

fn input_error(reason: QueryReadAdmissionCode) -> SqlParseError {
    SqlParseError::InputAdmission { reason }
}

#[test]
fn sql_membership_obeys_the_shared_exact_node_boundary() {
    for (count, admitted) in [
        (MAX_QUERY_INPUT_NODES - 5, true),
        (MAX_QUERY_INPUT_NODES - 4, false),
    ] {
        let values = vec!["NULL"; count].join(",");
        let sql = format!("SELECT id FROM E WHERE id IN ({values})");
        let result = parse_sql(&sql);
        if admitted {
            result.expect("exact node boundary");
        } else {
            assert_eq!(
                result.expect_err("one excess membership node"),
                input_error(QueryReadAdmissionCode::InputNodesExceeded)
            );
        }
    }
}

#[test]
fn sql_payload_obeys_the_shared_exact_content_boundary() {
    for (count, admitted) in [
        (MAX_QUERY_INPUT_BYTES - 5, true),
        (MAX_QUERY_INPUT_BYTES - 4, false),
    ] {
        let sql = format!("SELECT id FROM E WHERE id = '{}'", "x".repeat(count));
        let result = parse_sql(&sql);
        if admitted {
            result.expect("exact payload boundary");
        } else {
            assert_eq!(
                result.expect_err("one excess payload byte"),
                input_error(QueryReadAdmissionCode::InputBytesExceeded)
            );
        }
    }
}

#[test]
fn sql_components_and_statement_families_share_one_budget() {
    let payload = "x".repeat(MAX_QUERY_INPUT_BYTES);
    for sql in [
        format!("SELECT id FROM E WHERE id = '{payload}'"),
        format!("DELETE FROM E WHERE id = '{payload}'"),
        format!("UPDATE E SET id = '{payload}'"),
        format!("INSERT INTO E (id) VALUES ('{payload}')"),
        format!("INSERT INTO E (id) SELECT id FROM E WHERE id = '{payload}'"),
        format!("EXPLAIN SELECT id FROM E WHERE id = '{payload}'"),
        format!("EXPLAIN DELETE FROM E WHERE id = '{payload}'"),
        format!("ALTER TABLE E ADD CONSTRAINT c CHECK (id = '{payload}')"),
    ] {
        assert_eq!(
            parse_sql(&sql).expect_err("whole statement payload limit"),
            input_error(QueryReadAdmissionCode::InputBytesExceeded)
        );
    }
    let mut statement = parse_sql("SELECT id FROM E WHERE id = 'x' ORDER BY id").expect("syntax");
    let SqlStatement::Select(select) = &mut statement else {
        panic!("select");
    };
    // A repeated alias slot still occupies the input vector, even with no text.
    select.projection_aliases = vec![None; MAX_QUERY_INPUT_NODES];
    assert_eq!(
        validate_sql_statement_input(&statement, &[]),
        Err(QueryReadAdmissionCode::InputNodesExceeded)
    );
}

#[test]
fn between_admits_both_copies_before_cloning() {
    let sql = format!(
        "SELECT id FROM E WHERE '{}' BETWEEN 'a' AND 'z'",
        "x".repeat(MAX_QUERY_INPUT_BYTES / 2)
    );
    assert_eq!(
        parse_sql(&sql).expect_err("duplicated left payload exceeds shared input"),
        input_error(QueryReadAdmissionCode::InputBytesExceeded)
    );
}

#[test]
fn bindings_charge_effective_copies_and_leave_reusable_syntax_unchanged() {
    let mut predicate = "CASE WHEN id = ? THEN 1 ELSE 0 END".to_string();
    for _ in 0..5 {
        predicate = format!("CASE WHEN ({predicate}) BETWEEN 1 AND 1 THEN 1 ELSE 0 END");
    }
    let sql = format!("SELECT id FROM E WHERE ({predicate}) = 1");
    let statement = parse_sql(&sql).expect("bounded parameterized syntax");
    let original = statement.clone();
    validate_sql_bindings(&statement, &[InputValue::text("small".into())]).expect("small A");
    let error = validate_sql_bindings(&statement, &[InputValue::text("x".repeat(64 * 1024))])
        .expect_err("32 retained operand copies");
    assert_eq!(
        error.diagnostic(),
        QueryError::from(QueryReadAdmissionCode::InputBytesExceeded).diagnostic()
    );
    validate_sql_bindings(&statement, &[InputValue::text("small".into())]).expect("small A again");
    assert_eq!(statement, original);
}

#[test]
fn bound_literal_value_depth_is_checked_before_substitution() {
    let mut statement = parse_sql("SELECT id FROM E WHERE ?").expect("unbound syntax");
    let mut expr = SqlExpr::Param { index: 0 };
    for _ in 1..MAX_QUERY_INPUT_DEPTH {
        expr = SqlExpr::Unary {
            op: SqlExprUnaryOp::Not,
            expr: Box::new(expr),
        };
    }
    let SqlStatement::Select(select) = &mut statement else {
        panic!("select");
    };
    select.predicate = Some(expr);
    validate_sql_statement_input(&statement, &[]).expect("unbound maximum depth");
    let error = validate_sql_bindings(&statement, &[InputValue::boolean(false)])
        .expect_err("literal Value adds one level");
    assert_eq!(
        error.diagnostic(),
        QueryError::from(QueryReadAdmissionCode::InputDepthExceeded).diagnostic()
    );
}

#[test]
fn rejected_sql_and_parser_errors_drop_on_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            let chain = format!("SELECT id FROM E WHERE id{}", " IS NULL".repeat(10_000));
            assert_eq!(
                parse_sql(&chain).expect_err("deep postfix input"),
                input_error(QueryReadAdmissionCode::InputDepthExceeded)
            );
            assert!(matches!(
                parse_sql(&format!("{chain} IS 1")),
                Err(SqlParseError::InvalidSyntax { .. })
            ));
            assert_eq!(
                parse_sql(&format!("{chain} BETWEEN 0 AND 1"))
                    .expect_err("check before BETWEEN clone"),
                input_error(QueryReadAdmissionCode::InputDepthExceeded)
            );

            let mut statement = parse_sql("SELECT id FROM E").expect("small source");
            let mut value = Value::Null;
            for _ in 0..20_000 {
                value = Value::List(vec![value]);
            }
            let SqlStatement::Select(select) = &mut statement else {
                panic!("select");
            };
            select.predicate = Some(SqlExpr::Literal(value));
            let error = crate::db::query::preparation::with_preparation_work(|work| {
                prepare_sql_statement(&statement, "E", work)
            })
            .expect_err("reject before preparation clone");
            assert_eq!(
                QueryError::from_sql_lowering_error(error).diagnostic(),
                QueryError::from(QueryReadAdmissionCode::InputDepthExceeded).diagnostic()
            );
            drop(statement);
        })
        .expect("small-stack SQL probe")
        .join()
        .expect("parser and preparation rejection cleanup");
}
