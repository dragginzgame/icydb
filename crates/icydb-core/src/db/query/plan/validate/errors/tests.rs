use super::{ExprPlanError, GroupPlanError, OrderPlanError, PlanError, diagnostic_index};
use crate::db::{
    QueryError, ValidateError,
    query::plan::{
        AggregateKind,
        expr::{BinaryOp, ExprType, Function, NumericSubtype},
    },
};
use icydb_diagnostic_code::{
    DiagnosticAggregateKind, DiagnosticFactTag, DiagnosticFunctionKind, DiagnosticOperatorKind,
    DiagnosticTypeFamily, MAX_PUBLIC_QUERY_FIELD_BYTES, QueryFieldRole,
};

fn attached_context(error: PlanError, role: QueryFieldRole) -> Option<(QueryFieldRole, String)> {
    error
        .attach_query_field(role)
        .query_field_context()
        .map(|(role, field)| (role, field.to_string()))
}

fn assert_attachment_roles(
    make_error: impl Fn() -> PlanError,
    field: &str,
    admitted_roles: &[QueryFieldRole],
) {
    for role in [
        QueryFieldRole::Predicate,
        QueryFieldRole::Projection,
        QueryFieldRole::GroupBy,
        QueryFieldRole::Having,
        QueryFieldRole::OrderBy,
        QueryFieldRole::AggregateTarget,
    ] {
        let expected = admitted_roles
            .contains(&role)
            .then(|| (role, field.to_string()));
        assert_eq!(attached_context(make_error(), role), expected);
    }
}

#[test]
fn planner_attachment_matrix_is_closed_and_exact() {
    assert_attachment_roles(
        || {
            PlanError::from(ValidateError::UnknownField {
                field: "predicate_field".to_string(),
            })
        },
        "predicate_field",
        &[QueryFieldRole::Predicate],
    );
    assert_attachment_roles(
        || PlanError::from(ExprPlanError::unknown_expr_field("expression_field")),
        "expression_field",
        &[QueryFieldRole::Projection, QueryFieldRole::OrderBy],
    );
    assert_attachment_roles(
        || PlanError::from(ExprPlanError::unknown_field("sql_field")),
        "sql_field",
        &[
            QueryFieldRole::Predicate,
            QueryFieldRole::Projection,
            QueryFieldRole::GroupBy,
            QueryFieldRole::AggregateTarget,
        ],
    );
    assert_attachment_roles(
        || PlanError::from(OrderPlanError::unknown_field(2, "order_field")),
        "order_field",
        &[QueryFieldRole::OrderBy],
    );
    assert_attachment_roles(
        || PlanError::from(GroupPlanError::unknown_group_field_at(1, "group_field")),
        "group_field",
        &[QueryFieldRole::GroupBy],
    );
    assert_attachment_roles(
        || {
            PlanError::from(GroupPlanError::having_non_group_field_reference(
                3,
                "having_field",
            ))
        },
        "having_field",
        &[QueryFieldRole::Having],
    );
    assert_attachment_roles(
        || {
            PlanError::from(GroupPlanError::unknown_aggregate_target_field(
                4,
                "aggregate_field",
            ))
        },
        "aggregate_field",
        &[QueryFieldRole::AggregateTarget],
    );
    assert_eq!(
        attached_context(
            PlanError::from(OrderPlanError::unorderable_field(0)),
            QueryFieldRole::OrderBy,
        ),
        None
    );
}

#[test]
fn planner_context_bound_preserves_exact_utf8_or_omits_it() {
    let exact = "é".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES / 2);
    let over = format!("{exact}a");
    let empty = String::new();

    assert_eq!(
        attached_context(
            PlanError::from(OrderPlanError::unknown_field(0, exact.clone())),
            QueryFieldRole::OrderBy,
        ),
        Some((QueryFieldRole::OrderBy, exact))
    );
    for field in [over, empty] {
        assert_eq!(
            attached_context(
                PlanError::from(OrderPlanError::unknown_field(0, field)),
                QueryFieldRole::OrderBy,
            ),
            None
        );
    }
}

#[test]
fn order_diagnostics_retain_exact_term_and_component_positions() {
    assert_eq!(
        OrderPlanError::duplicate_order_field(2, 5).diagnostic_facts(),
        vec![
            (DiagnosticFactTag::FirstTermIndex, 2),
            (DiagnosticFactTag::DuplicateTermIndex, 5),
        ],
    );
    assert_eq!(
        OrderPlanError::missing_primary_key_tie_break(3).diagnostic_facts(),
        vec![(DiagnosticFactTag::ComponentIndex, 3)],
    );
    assert_eq!(diagnostic_index(usize::MAX), usize::MAX as u64);

    let query_error =
        QueryError::from(PlanError::from(OrderPlanError::unknown_field(7, "missing")));
    assert_eq!(
        query_error.diagnostic_facts(),
        vec![(DiagnosticFactTag::TermIndex, 7)],
    );
}

#[test]
fn group_diagnostics_retain_clause_aggregate_count_and_kind() {
    assert_eq!(
        GroupPlanError::having_aggregate_index_out_of_bounds(1, 4, 3).diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ClauseIndex, 1),
            (DiagnosticFactTag::AggregateIndex, 4),
            (DiagnosticFactTag::ActualCount, 3),
        ],
    );
    assert_eq!(
        GroupPlanError::field_target_aggregates_unsupported(
            6,
            AggregateKind::Last,
            "private-name",
        )
        .diagnostic_facts(),
        vec![
            (DiagnosticFactTag::AggregateIndex, 6),
            (
                DiagnosticFactTag::AggregateKind,
                DiagnosticAggregateKind::Last.raw(),
            ),
        ],
    );
    assert_eq!(
        GroupPlanError::unknown_group_field_at(3, "private-name").diagnostic_facts(),
        vec![(DiagnosticFactTag::GroupIndex, 3)],
    );
    assert_eq!(
        GroupPlanError::duplicate_group_field(4, "private-name").diagnostic_facts(),
        vec![(DiagnosticFactTag::GroupIndex, 4)],
    );
}

#[test]
fn expression_diagnostics_retain_function_arity_and_argument_types() {
    assert_eq!(
        ExprPlanError::invalid_function_arity(Function::InList, 2, 3).diagnostic_facts(),
        vec![
            (
                DiagnosticFactTag::FunctionKind,
                DiagnosticFunctionKind::InList.raw(),
            ),
            (DiagnosticFactTag::ExpectedArity, 2),
            (DiagnosticFactTag::ActualArity, 3),
        ],
    );
    assert_eq!(
        ExprPlanError::incompatible_function_arguments(
            Function::Coalesce,
            0,
            2,
            &ExprType::Text,
            &ExprType::Numeric(NumericSubtype::Integer),
        )
        .diagnostic_facts(),
        vec![
            (
                DiagnosticFactTag::FunctionKind,
                DiagnosticFunctionKind::Coalesce.raw(),
            ),
            (DiagnosticFactTag::ArgumentIndex, 0),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Text.raw(),
            ),
            (DiagnosticFactTag::ArgumentIndex, 2),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Numeric.raw(),
            ),
        ],
    );
}

#[test]
fn expression_diagnostics_retain_operator_and_branch_positions() {
    assert_eq!(
        ExprPlanError::invalid_binary_operands(BinaryOp::Add, &ExprType::Text, &ExprType::Bool,)
            .diagnostic_facts(),
        vec![
            (
                DiagnosticFactTag::OperatorKind,
                DiagnosticOperatorKind::Add.raw(),
            ),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Text.raw(),
            ),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Bool.raw(),
            ),
        ],
    );
    assert_eq!(
        ExprPlanError::incompatible_case_branch_types(
            Some(1),
            None,
            &ExprType::Blob,
            &ExprType::Structured,
        )
        .diagnostic_facts(),
        vec![
            (DiagnosticFactTag::BranchIndex, 1),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Blob.raw(),
            ),
            (
                DiagnosticFactTag::TypeFamily,
                DiagnosticTypeFamily::Structured.raw(),
            ),
        ],
    );
}
