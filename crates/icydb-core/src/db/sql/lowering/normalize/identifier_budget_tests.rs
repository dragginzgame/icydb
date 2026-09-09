use crate::db::{
    QueryError, RequestExecutionRoot,
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    query::preparation::PreparationWork,
    sql::{
        lowering::{
            SqlLoweringError,
            normalize::{
                normalize_field_identifier_expr_to_scope, normalize_field_path_to_scope,
                normalize_identifier, normalize_sql_expr_to_scope,
            },
        },
        parser::{SqlExpr, SqlExprBinaryOp},
    },
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

fn run<T>(
    request: &RequestExecutionRoot,
    f: impl FnOnce(&PreparationWork<'_>) -> Result<T, SqlLoweringError>,
) -> Result<T, QueryError> {
    PreparationWork::run(
        &request.scope(),
        DiagnosticExecutionLane::PublicRead,
        |work| {
            f(work).map_err(|error| match error {
                SqlLoweringError::Query(error) => *error,
                other => panic!("unexpected lowering error: {other:?}"),
            })
        },
    )
}

#[test]
fn owned_path_matching_keeps_longest_tail_policy_without_new_backing() {
    for (head, tail, scopes, expected_head, expected_tail) in [
        (
            "a",
            vec!["b", "c", "name"],
            vec!["a", "a.b"],
            "c",
            vec!["name"],
        ),
        ("other", vec!["b", "name"], vec!["public.b"], "name", vec![]),
        (
            "profile",
            vec!["address", "city"],
            vec!["users"],
            "profile",
            vec!["address", "city"],
        ),
        ("app.users", vec!["name"], vec!["users"], "name", vec![]),
        ("a.b", vec!["x.y", "z"], vec!["y"], "z", vec![]),
        ("u", vec!["", "name"], vec![""], "name", vec![]),
        ("é", vec!["名"], vec!["é"], "名", vec![]),
        ("é", vec!["名"], vec!["É"], "é", vec!["名"]),
    ] {
        let scopes: Vec<_> = scopes.into_iter().map(str::to_string).collect();
        let tail = tail.into_iter().map(str::to_string).collect();
        let request = root(Resource::TemporaryBytes, 0);
        let result = run(&request, |work| {
            normalize_field_path_to_scope(head.into(), tail, &scopes, work)
        })
        .unwrap();
        let expected = if expected_tail.is_empty() {
            SqlExpr::Field(expected_head.into())
        } else {
            SqlExpr::FieldPath {
                root: expected_head.into(),
                segments: expected_tail.into_iter().map(str::to_string).collect(),
            }
        };
        assert_eq!(result, expected);
        assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn dotted_field_construction_charges_exact_requested_backing() {
    let bytes = size_of::<String>() as u64 + 11;
    let request = root(Resource::TemporaryBytes, bytes);
    let result = run(&request, |work| {
        normalize_field_identifier_expr_to_scope("profile.city".into(), &[], work)
    })
    .unwrap();
    assert_eq!(
        result,
        SqlExpr::FieldPath {
            root: "profile".into(),
            segments: vec!["city".into()]
        }
    );
    assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
    assert!(
        run(&root(Resource::TemporaryBytes, bytes - 1), |work| {
            normalize_field_identifier_expr_to_scope("profile.city".into(), &[], work)
        })
        .is_err()
    );
}

#[test]
fn scope_reduction_has_a_charged_matching_allowance_and_cumulative_rejection() {
    let scopes = ["u".to_string()];
    let invoke = |request: &RequestExecutionRoot| {
        run(request, |work| {
            normalize_identifier("u.name".into(), &scopes, work)
        })
    };
    let exact = root(Resource::PredicateExpressionSteps, 35);
    assert_eq!(invoke(&exact).unwrap(), "name");
    assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 35);
    let short = root(Resource::PredicateExpressionSteps, 34);
    let error = invoke(&short).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw()
    )));
    let observed = short.observed(Resource::PredicateExpressionSteps);
    assert!(invoke(&short).is_err());
    assert!(short.observed(Resource::PredicateExpressionSteps) > observed);
}

#[test]
fn rejected_identifier_walk_stops_before_later_siblings() {
    let expr = SqlExpr::Binary {
        op: SqlExprBinaryOp::Add,
        left: Box::new(SqlExpr::Field("left".into())),
        right: Box::new(SqlExpr::Field("right".into())),
    };
    let request = root(Resource::PredicateExpressionSteps, 1);
    assert!(
        run(&request, |work| normalize_sql_expr_to_scope(
            expr,
            &[],
            work
        ))
        .is_err()
    );
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 2);
}
