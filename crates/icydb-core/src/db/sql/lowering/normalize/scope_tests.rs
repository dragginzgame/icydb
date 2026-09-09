use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::preparation::PreparationWork,
    sql::lowering::{SqlLoweringError, normalize::sql_statement_scope_candidates},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
};

fn root(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn candidates(root: &RequestExecutionRoot) -> Result<Vec<String>, QueryError> {
    PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
        sql_statement_scope_candidates("app.users", "users", Some("u"), work).map_err(|error| {
            match error {
                SqlLoweringError::Query(error) => *error,
                other => panic!("unexpected lowering error: {other:?}"),
            }
        })
    })
}

#[test]
fn scope_construction_preserves_order_with_exact_backing_and_work_limits() {
    for (resource, required) in [
        (
            Resource::TemporaryBytes,
            6 * size_of::<String>() as u64 + 26,
        ),
        (Resource::PredicateExpressionSteps, 47),
    ] {
        let exact = root(resource, required);
        assert_eq!(
            candidates(&exact).unwrap(),
            ["app.users", "users", "u", "users", "users", "u"]
        );
        assert_eq!(exact.observed(resource), required);
        let short = root(resource, required - 1);
        let error = candidates(&short).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
        );
        let observed = short.observed(resource);
        assert!(candidates(&short).is_err());
        assert!(short.observed(resource) > observed);
    }
}

#[test]
#[cfg(feature = "sql")]
fn preparation_variants_share_scope_admission_before_lowering() {
    use crate::db::sql::{lowering::prepare::prepare_sql_statement, parser::parse_sql};

    for sql in [
        "SELECT u.id FROM E u",
        "DELETE FROM E WHERE id = 1",
        "UPDATE E SET id = 2 WHERE id = 1",
        "INSERT INTO E (id) SELECT u.id FROM E u",
        "EXPLAIN SELECT u.id FROM E u",
    ] {
        let syntax = parse_sql(sql).expect("supported statement");
        let request = root(Resource::TemporaryBytes, 0);
        let result = PreparationWork::run(
            &request.scope(),
            DiagnosticExecutionLane::PublicRead,
            |work| {
                prepare_sql_statement(&syntax, "E", work).map_err(|error| match error {
                    SqlLoweringError::Query(error) => *error,
                    other => panic!("unexpected lowering error: {other:?}"),
                })
            },
        );
        let error = result.expect_err("scope construction rejects");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
    }
}
