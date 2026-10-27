//! Text expansion rejects before allocation across maintained SQL consumers.

use super::*;
use crate::db::{
    RequestExecutionRoot,
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag as Fact,
    RuntimeBoundaryCode,
};

#[test]
fn sql_replace_expansion_preserves_typed_budget_errors_on_cold_and_warm_routes() {
    let setup = initialize();
    seed_singleton(&setup);
    let label = "a".repeat(400);
    setup
        .execute_trusted_sql_exact_update(
            &format!("UPDATE Singleton SET label = '{label}' WHERE label IS NOT NULL"),
            1,
        )
        .unwrap();
    let replacement = "z".repeat(512);
    for sql in [
        format!("SELECT REPLACE(label, 'a', '{replacement}') FROM Singleton"),
        format!("SELECT label FROM Singleton WHERE REPLACE(label, 'a', '{replacement}') = 'none'"),
        format!(
            "SELECT REPLACE(label, 'a', '{replacement}'), COUNT(*) FROM Singleton GROUP BY label"
        ),
        format!(
            "SELECT label, COUNT(*) FROM Singleton GROUP BY label HAVING REPLACE(label, 'a', '{replacement}') = 'none'"
        ),
    ] {
        for warm in [false, true] {
            if warm {
                setup.compile_sql_query_for_tests(&sql).unwrap();
            }
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, 64 * 1024),
            );
            let error = new_request_session_with_root(&root)
                .execute_trusted_sql_query(&sql)
                .unwrap_err();
            assert_eq!(
                error.diagnostic().detail(),
                Some(&DiagnosticDetail::RuntimeBoundary {
                    boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
                })
            );
            let facts = error.diagnostic_facts();
            assert!(facts.contains(&(Fact::BudgetResource, Resource::TemporaryBytes.raw())));
            assert!(facts.contains(&(Fact::Limit, 64 * 1024)));
            assert!(
                facts
                    .iter()
                    .any(|(tag, value)| *tag == Fact::Actual && *value >= 400 * 512)
            );
        }
    }
    let SqlStatementResult::Projection { rows, .. } = new_request_session()
        .execute_trusted_sql_query("SELECT label, REPLACE(label, 'a', 'z') FROM Singleton")
        .unwrap()
    else {
        panic!("source and bounded replacement should remain readable");
    };
    assert_eq!(
        rows,
        vec![vec![
            OutputValue::text(label),
            OutputValue::text("z".repeat(400))
        ]]
    );
}

#[test]
fn sql_text_construction_preserves_budget_failures_through_aggregate_inputs_and_filters() {
    let setup = initialize();
    seed_singleton(&setup);
    let label = "ΐ".repeat(100);
    setup
        .execute_trusted_sql_exact_update(
            &format!("UPDATE Singleton SET label = '{label}' WHERE label IS NOT NULL"),
            1,
        )
        .unwrap();
    // The intermediate REPLACE fits the allowance. A later casing/copy
    // constructor must refuse cumulatively, rather than relying on REPLACE to
    // reject or losing its typed refusal at an aggregate expression boundary.
    let expanded = format!("REPLACE(label, 'ΐ', '{}')", "ΐ".repeat(64));
    let control_root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 64 * 1024),
    );
    new_request_session_with_root(&control_root)
        .execute_trusted_sql_query(&format!("SELECT {expanded} FROM Singleton"))
        .unwrap();
    for function in ["LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM"] {
        let mut expr = expanded.clone();
        let repeats = match function {
            "UPPER" => 1,
            "LOWER" => 2,
            _ => 5,
        };
        for _ in 0..repeats {
            expr = format!("{function}({expr})");
        }
        for sql in [
            format!("SELECT {expr} FROM Singleton"),
            format!("SELECT label FROM Singleton WHERE {expr} = 'none'"),
            format!("SELECT {expr}, COUNT(*) FROM Singleton GROUP BY label"),
            format!("SELECT label, COUNT(*) FROM Singleton GROUP BY label HAVING {expr} = 'none'"),
            format!("SELECT SUM(LENGTH({expr})) FROM Singleton GROUP BY label"),
            format!("SELECT COUNT(*) FILTER (WHERE {expr} = 'none') FROM Singleton GROUP BY label"),
        ] {
            for warm in [false, true] {
                if warm {
                    setup.compile_sql_query_for_tests(&sql).unwrap();
                }
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(Resource::TemporaryBytes, 64 * 1024),
                );
                let error = new_request_session_with_root(&root)
                    .execute_trusted_sql_query(&sql)
                    .unwrap_err();
                assert_eq!(
                    error.diagnostic().detail(),
                    Some(&DiagnosticDetail::RuntimeBoundary {
                        boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
                    }),
                    "{sql}"
                );
                let facts = error.diagnostic_facts();
                assert!(
                    facts.contains(&(Fact::BudgetResource, Resource::TemporaryBytes.raw())),
                    "{sql}"
                );
                assert!(facts.contains(&(Fact::Limit, 64 * 1024)), "{sql}");
                assert!(
                    facts.contains(&(
                        Fact::ExecutionBudgetScope,
                        icydb_diagnostic_code::DiagnosticExecutionBudgetScope::Request.raw()
                    )),
                    "{sql}: {facts:?}"
                );
                // Preparation uses a zero shape prefix. The inherited request
                // allowance must be exhausted from the entered execution plan.
                assert!(
                    facts.iter().any(
                        |(tag, value)| *tag == Fact::QueryShapeFingerprintPrefix && *value != 0
                    ),
                    "{sql}: {facts:?}"
                );
            }
        }
    }
    let SqlStatementResult::Projection { rows, .. } = new_request_session()
        .execute_trusted_sql_query("SELECT label, LOWER(label), UPPER(label) FROM Singleton")
        .unwrap()
    else {
        panic!("text outputs remain available after rejection");
    };
    assert_eq!(
        rows,
        vec![vec![
            OutputValue::text(label.clone()),
            OutputValue::text(label.to_lowercase()),
            OutputValue::text(label.to_uppercase())
        ]]
    );
}
