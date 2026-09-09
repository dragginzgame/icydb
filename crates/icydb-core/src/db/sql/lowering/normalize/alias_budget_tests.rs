use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        query::preparation::PreparationWork,
        sql::{
            lowering::{
                SqlLoweringError, copy::copy_select_item_expr, normalize::normalize_scalar_aliases,
            },
            parser::{SqlExpr, SqlProjection, SqlSelectItem, SqlStatement, parse_sql},
        },
    },
    value::Value,
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
fn alias_walk_lookup_and_copy_have_exact_limits_and_accumulate_retries() {
    let projection = SqlProjection::Items(vec![SqlSelectItem::Field("value".into())]);
    let aliases = [Some("alias".into())];
    for (resource, required) in [
        (Resource::TemporaryBytes, 5),
        (Resource::PredicateExpressionSteps, 18),
    ] {
        let invoke = |request: &RequestExecutionRoot| {
            run(request, |work| {
                normalize_scalar_aliases(
                    SqlExpr::Field("ALIAS".into()),
                    &projection,
                    &aliases,
                    work,
                )
            })
        };
        let exact = root(resource, required);
        assert_eq!(invoke(&exact).unwrap(), SqlExpr::Field("value".into()));
        assert_eq!(exact.observed(resource), required);
        let short = root(resource, required - 1);
        let error = invoke(&short).unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
        let observed = short.observed(resource);
        assert!(invoke(&short).is_err());
        assert!(short.observed(resource) > observed);
    }
}

#[test]
fn copied_projection_families_preserve_sql_shape_and_source_on_failure() {
    for expr in [
        "name",
        "profile.city",
        "COUNT(*)",
        "SUM(amount) FILTER (WHERE age > 1)",
        "CASE WHEN name IN (?, 'b') THEN name ELSE 'other' END",
        "CASE WHEN name IS NULL THEN name ELSE 'other' END",
        "CASE WHEN name LIKE 'a%' THEN name ELSE 'other' END",
        "LOWER(name)",
        "CASE WHEN NOT active THEN name ELSE 'other' END",
        "age + 1",
        "CASE WHEN active THEN name ELSE 'other' END",
        "?",
    ] {
        let SqlStatement::Select(statement) =
            parse_sql(&format!("SELECT {expr} FROM E")).expect("copy fixture")
        else {
            panic!("select");
        };
        let SqlProjection::Items(items) = statement.projection else {
            panic!("items");
        };
        let item = &items[0];
        let expected = SqlExpr::from_select_item(item);
        let original = item.clone();
        let request = root(Resource::TemporaryBytes, 16_000_000);
        assert_eq!(
            run(&request, |work| copy_select_item_expr(item, work)).unwrap(),
            expected
        );
        let retained = request.observed(Resource::TemporaryBytes);
        if retained > 0 {
            assert!(
                run(&root(Resource::TemporaryBytes, retained - 1), |work| {
                    copy_select_item_expr(item, work)
                })
                .is_err()
            );
        }
        assert_eq!(*item, original);
    }
}

#[test]
fn alias_operand_copies_charge_nested_payload_and_requested_container_backing() {
    let item = SqlSelectItem::Expr(SqlExpr::Literal(Value::List(vec![Value::Text(
        "abc".into(),
    )])));
    let bytes = size_of::<Value>() as u64 + 3;
    let request = root(Resource::TemporaryBytes, bytes);
    assert_eq!(
        run(&request, |work| copy_select_item_expr(&item, work)).unwrap(),
        SqlExpr::from_select_item(&item)
    );
    assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
    assert_eq!(request.observed(Resource::NestedValueSteps), 2);
    assert!(
        run(&root(Resource::TemporaryBytes, bytes - 1), |work| {
            copy_select_item_expr(&item, work)
        })
        .is_err()
    );
}

#[test]
fn rejected_alias_child_stops_before_later_siblings() {
    let expr = SqlExpr::Binary {
        op: crate::db::sql::parser::SqlExprBinaryOp::Add,
        left: Box::new(SqlExpr::Field("left".into())),
        right: Box::new(SqlExpr::Field("right".into())),
    };
    let request = root(Resource::PredicateExpressionSteps, 1);
    assert!(
        run(&request, |work| normalize_scalar_aliases(
            expr,
            &SqlProjection::All,
            &[],
            work
        ))
        .is_err()
    );
    assert_eq!(request.observed(Resource::PredicateExpressionSteps), 2);
    assert_eq!(request.observed(Resource::TemporaryBytes), 0);
}
