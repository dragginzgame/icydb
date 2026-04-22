use crate::db::QueryError;
use crate::db::sql::lowering::{
    LoweredSqlCommand, LoweredSqlCommandInner, LoweredSqlQuery, PreparedSqlParameterContract,
    PreparedSqlParameterTypeFamily, PreparedSqlStatement, SqlLoweringError,
    aggregate::{is_sql_global_aggregate_statement, lower_global_aggregate_select_shape},
    expr::{
        PreparedSqlTemplateExprScope, SqlExprPhase, lower_sql_binary_op, lower_sql_expr,
        lower_sql_scalar_function, sql_expr_contains_any_literal, sql_expr_contains_param,
        sql_expr_first_param_index, sql_expr_first_param_index_from_args,
        sql_expr_first_param_index_from_pair, sql_expr_is_boolean_shape,
        sql_expr_uses_general_template_expr_parameters,
    },
    normalize::{
        adapt_sql_predicate_identifiers_to_scope, ensure_entity_matches_expected,
        normalize_order_terms, normalize_select_statement_to_expected_entity,
        sql_entity_scope_candidates,
    },
    select::{lower_delete_shape, lower_select_shape, select_item_contains_aggregate},
};
use crate::db::sql::parser::{
    SqlAggregateCall, SqlAggregateKind, SqlCaseArm, SqlDeleteStatement, SqlExplainMode,
    SqlExplainStatement, SqlExplainTarget, SqlExpr, SqlExprBinaryOp, SqlInsertSource,
    SqlInsertStatement, SqlOrderTerm, SqlProjection, SqlScalarFunction, SqlSelectItem,
    SqlSelectStatement, SqlStatement,
};
use crate::db::{
    query::plan::expr::{
        Expr, ExprCoarseTypeFamily, Function, binary_operand_coarse_family,
        coarse_family_for_field_kind, coarse_family_for_literal,
        dynamic_function_arg_coarse_family, function_arg_coarse_family,
        function_result_coarse_family, infer_case_result_exprs_coarse_family,
        infer_dynamic_function_result_exprs_coarse_family, infer_expr_coarse_family,
    },
    schema::SchemaInfo,
};
use crate::model::entity::EntityModel;
use crate::model::field::{FieldKind, FieldModel};
use crate::types::{Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Timestamp};
use crate::value::Value;

/// Prepare one parsed SQL statement for one expected entity route.
#[inline(never)]
pub(crate) fn prepare_sql_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<PreparedSqlStatement, SqlLoweringError> {
    let statement = prepare_statement(statement, expected_entity)?;

    Ok(PreparedSqlStatement { statement })
}

/// Lower one prepared SQL statement into one shared generic-free command shape.
#[inline(never)]
pub(crate) fn lower_sql_command_from_prepared_statement(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    lower_prepared_statement(prepared.statement, model)
}

pub(in crate::db) fn collect_prepared_statement_parameter_contracts(
    statement: &SqlStatement,
    model: &'static EntityModel,
) -> Result<Vec<PreparedSqlParameterContract>, SqlLoweringError> {
    let SqlStatement::Select(statement) = statement else {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            "parameterized prepare currently supports SQL SELECT query shapes only",
        ));
    };

    let mut contracts = Vec::new();

    reject_params_in_projection(statement.projection.clone())?;
    for order_term in &statement.order_by {
        reject_params_in_expr(&order_term.field, "ORDER BY")?;
    }
    if let Some(predicate) = &statement.predicate {
        collect_where_param_contracts(predicate, model, &mut contracts)?;
    }
    for having_expr in &statement.having {
        collect_having_param_contracts(having_expr, model, &mut contracts)?;
    }

    Ok(contracts)
}

pub(in crate::db) fn prepared_statement_uses_general_template_expr_parameters(
    statement: &SqlStatement,
) -> bool {
    let SqlStatement::Select(select) = statement else {
        return false;
    };

    select.predicate.as_ref().is_some_and(|expr| {
        sql_expr_uses_general_template_expr_parameters(PreparedSqlTemplateExprScope::Filter, expr)
    }) || select.having.iter().any(|expr| {
        sql_expr_uses_general_template_expr_parameters(PreparedSqlTemplateExprScope::Having, expr)
    })
}

pub(in crate::db) fn bind_prepared_statement_literals(
    statement: &SqlStatement,
    bindings: &[Value],
) -> Result<SqlStatement, QueryError> {
    match statement {
        SqlStatement::Select(select) => Ok(SqlStatement::Select(SqlSelectStatement {
            projection: bind_projection_literals(&select.projection, bindings)?,
            projection_aliases: select.projection_aliases.clone(),
            entity: select.entity.clone(),
            predicate: select
                .predicate
                .as_ref()
                .map(|expr| bind_sql_expr_literals(expr, bindings))
                .transpose()?,
            distinct: select.distinct,
            group_by: select.group_by.clone(),
            having: select
                .having
                .iter()
                .map(|expr| bind_sql_expr_literals(expr, bindings))
                .collect::<Result<Vec<_>, _>>()?,
            order_by: select
                .order_by
                .iter()
                .map(|term| {
                    Ok(SqlOrderTerm {
                        field: bind_sql_expr_literals(&term.field, bindings)?,
                        direction: term.direction,
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
            limit: select.limit,
            offset: select.offset,
        })),
        _ => Err(QueryError::unsupported_query(
            "prepared SQL binding currently supports SELECT query shapes only",
        )),
    }
}

// Walk one parsed SQL statement and report whether it already contains any
// ordinary literal equal to one of the supplied values. Lowering owns this
// parser-shape traversal so session binding can keep the prepared template
// policy while consuming one shared statement-level scan.
pub(in crate::db) fn sql_statement_contains_any_literal(
    statement: &SqlStatement,
    values: &[Value],
) -> bool {
    match statement {
        SqlStatement::Select(select) => sql_select_contains_any_literal(select, values),
        SqlStatement::Delete(delete) => delete
            .predicate
            .as_ref()
            .is_some_and(|expr| sql_expr_contains_any_literal(expr, values)),
        SqlStatement::Explain(explain) => match &explain.statement {
            SqlExplainTarget::Select(select) => sql_select_contains_any_literal(select, values),
            SqlExplainTarget::Delete(delete) => delete
                .predicate
                .as_ref()
                .is_some_and(|expr| sql_expr_contains_any_literal(expr, values)),
        },
        SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Describe(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowEntities(_) => false,
    }
}

// Return one simple same-field lower/upper range pair when the parsed SQL
// predicate is exactly `field >= ? AND field < ?` (or the analogous strict/
// inclusive variants) over one admitted prepared compare-family field.
pub(in crate::db) fn prepared_sql_simple_range_slots(
    predicate: Option<&SqlExpr>,
    model: &'static EntityModel,
    contracts: &[PreparedSqlParameterContract],
) -> Option<(FieldKind, usize, usize)> {
    let SqlExpr::Binary {
        op: SqlExprBinaryOp::And,
        left,
        right,
    } = predicate?
    else {
        return None;
    };
    let first = sql_range_compare_descriptor(left)?;
    let second = sql_range_compare_descriptor(right)?;
    if first.field != second.field {
        return None;
    }

    let (lower_slot, upper_slot) = match (first.bound, second.bound) {
        (SqlRangeBoundKind::Lower, SqlRangeBoundKind::Upper) => {
            (first.slot_index, second.slot_index)
        }
        (SqlRangeBoundKind::Upper, SqlRangeBoundKind::Lower) => {
            (second.slot_index, first.slot_index)
        }
        _ => return None,
    };
    if contracts.get(lower_slot)?.type_family() != contracts.get(upper_slot)?.type_family() {
        return None;
    }

    let field_kind = model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == first.field)
        .map(FieldModel::kind)?;

    Some((field_kind, lower_slot, upper_slot))
}

#[inline(never)]
fn prepare_statement(
    statement: SqlStatement,
    expected_entity: &'static str,
) -> Result<SqlStatement, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(SqlStatement::Select(prepare_select_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Delete(statement) => Ok(SqlStatement::Delete(prepare_delete_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Insert(statement) => Ok(SqlStatement::Insert(prepare_insert_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Update(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::Update(statement))
        }
        SqlStatement::Explain(statement) => Ok(SqlStatement::Explain(prepare_explain_statement(
            statement,
            expected_entity,
        )?)),
        SqlStatement::Describe(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::Describe(statement))
        }
        SqlStatement::ShowIndexes(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowIndexes(statement))
        }
        SqlStatement::ShowColumns(statement) => {
            ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

            Ok(SqlStatement::ShowColumns(statement))
        }
        SqlStatement::ShowEntities(statement) => Ok(SqlStatement::ShowEntities(statement)),
    }
}

fn prepare_explain_statement(
    statement: SqlExplainStatement,
    expected_entity: &'static str,
) -> Result<SqlExplainStatement, SqlLoweringError> {
    let target = match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            SqlExplainTarget::Select(prepare_select_statement(select_statement, expected_entity)?)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            SqlExplainTarget::Delete(prepare_delete_statement(delete_statement, expected_entity)?)
        }
    };

    Ok(SqlExplainStatement {
        mode: statement.mode,
        verbose: statement.verbose,
        statement: target,
    })
}

fn prepare_select_statement(
    statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    normalize_select_statement_to_expected_entity(statement, expected_entity)
}

fn prepare_delete_statement(
    mut statement: SqlDeleteStatement,
    expected_entity: &'static str,
) -> Result<SqlDeleteStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;
    let entity_scope = sql_entity_scope_candidates(statement.entity.as_str(), expected_entity);
    statement.predicate = statement.predicate.map(|predicate| {
        adapt_sql_predicate_identifiers_to_scope(predicate, entity_scope.as_slice())
    });
    statement.order_by = normalize_order_terms(statement.order_by, entity_scope.as_slice());

    Ok(statement)
}

fn prepare_insert_statement(
    mut statement: SqlInsertStatement,
    expected_entity: &'static str,
) -> Result<SqlInsertStatement, SqlLoweringError> {
    ensure_entity_matches_expected(statement.entity.as_str(), expected_entity)?;

    if let SqlInsertSource::Select(select) = statement.source {
        statement.source = SqlInsertSource::Select(Box::new(prepare_insert_select_source(
            *select,
            expected_entity,
        )?));
    }

    Ok(statement)
}

// Walk one parsed SQL expression and report whether it references any runtime
// parameter slot.
fn prepare_insert_select_source(
    statement: SqlSelectStatement,
    expected_entity: &'static str,
) -> Result<SqlSelectStatement, SqlLoweringError> {
    let statement = prepare_select_statement(statement, expected_entity)?;

    if !statement.group_by.is_empty() || !statement.having.is_empty() {
        return Err(QueryError::unsupported_query(
            "SQL INSERT SELECT requires scalar SELECT source in this release",
        )
        .into());
    }

    if let SqlProjection::Items(items) = &statement.projection {
        for item in items {
            if select_item_contains_aggregate(item) {
                return Err(QueryError::unsupported_query(
                    "SQL INSERT SELECT does not support aggregate source projection in this release",
                )
                .into());
            }
        }
    }

    Ok(statement)
}

// Walk one parsed SQL SELECT statement and report whether it already contains
// any ordinary literal equal to one of the supplied values.
fn sql_select_contains_any_literal(select: &SqlSelectStatement, values: &[Value]) -> bool {
    sql_projection_contains_any_literal(&select.projection, values)
        || select
            .predicate
            .as_ref()
            .is_some_and(|expr| sql_expr_contains_any_literal(expr, values))
        || select
            .having
            .iter()
            .any(|expr| sql_expr_contains_any_literal(expr, values))
        || select
            .order_by
            .iter()
            .any(|term| sql_expr_contains_any_literal(&term.field, values))
}

// Walk one parsed SQL projection and report whether it already contains any
// ordinary literal equal to one of the supplied values.
fn sql_projection_contains_any_literal(projection: &SqlProjection, values: &[Value]) -> bool {
    match projection {
        SqlProjection::All => false,
        SqlProjection::Items(items) => items
            .iter()
            .any(|item| sql_expr_contains_any_literal(&SqlExpr::from_select_item(item), values)),
    }
}

///
/// SqlRangeBoundKind
///
/// Parsed SQL range-side classification for the bounded prepared simple-range
/// detector. Lowering owns this parser-shape split so grouped symbolic lane
/// policy can consume one prepared range descriptor instead of rebuilding it.
///

enum SqlRangeBoundKind {
    Lower,
    Upper,
}

///
/// SqlRangeCompareDescriptor
///
/// One parser-owned compare descriptor for the bounded prepared simple-range
/// detector. This exists only to keep same-field lower/upper slot pairing on
/// one shared lowering-owned shape instead of re-deriving it in session code.
///

struct SqlRangeCompareDescriptor<'a> {
    field: &'a str,
    slot_index: usize,
    bound: SqlRangeBoundKind,
}

// Return one parser-owned compare descriptor when the expression is one direct
// `field <op> ?` range edge on the admitted prepared compare family.
fn sql_range_compare_descriptor(expr: &SqlExpr) -> Option<SqlRangeCompareDescriptor<'_>> {
    let SqlExpr::Binary { op, left, right } = expr else {
        return None;
    };
    let (SqlExpr::Field(field), SqlExpr::Param { index }) = (&**left, &**right) else {
        return None;
    };

    let bound = match op {
        SqlExprBinaryOp::Gt | SqlExprBinaryOp::Gte => SqlRangeBoundKind::Lower,
        SqlExprBinaryOp::Lt | SqlExprBinaryOp::Lte => SqlRangeBoundKind::Upper,
        _ => return None,
    };

    Some(SqlRangeCompareDescriptor {
        field,
        slot_index: *index,
        bound,
    })
}

#[inline(never)]
fn lower_prepared_statement(
    statement: SqlStatement,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    match statement {
        SqlStatement::Select(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Select(lower_select_shape(statement, model)?),
        ))),
        SqlStatement::Delete(statement) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Query(
            LoweredSqlQuery::Delete(lower_delete_shape(statement)?),
        ))),
        SqlStatement::Insert(_) | SqlStatement::Update(_) => {
            Err(SqlLoweringError::unexpected_query_lane_statement())
        }
        SqlStatement::Explain(statement) => lower_explain_prepared(statement, model),
        SqlStatement::Describe(_) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::DescribeEntity)),
        SqlStatement::ShowIndexes(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowIndexesEntity))
        }
        SqlStatement::ShowColumns(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowColumnsEntity))
        }
        SqlStatement::ShowEntities(_) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::ShowEntities))
        }
    }
}

fn lower_explain_prepared(
    statement: SqlExplainStatement,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    let mode = statement.mode;
    let verbose = statement.verbose;

    match statement.statement {
        SqlExplainTarget::Select(select_statement) => {
            lower_explain_select_prepared(select_statement, mode, verbose, model)
        }
        SqlExplainTarget::Delete(delete_statement) => {
            Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
                mode,
                verbose,
                query: LoweredSqlQuery::Delete(lower_delete_shape(delete_statement)?),
            }))
        }
    }
}

fn lower_explain_select_prepared(
    statement: SqlSelectStatement,
    mode: SqlExplainMode,
    verbose: bool,
    model: &'static EntityModel,
) -> Result<LoweredSqlCommand, SqlLoweringError> {
    if is_sql_global_aggregate_statement(&SqlStatement::Select(statement.clone())) {
        let command = lower_global_aggregate_select_shape(statement)?;

        return Ok(LoweredSqlCommand(
            LoweredSqlCommandInner::ExplainGlobalAggregate {
                mode,
                verbose,
                command,
            },
        ));
    }

    match lower_select_shape(statement.clone(), model) {
        Ok(query) => Ok(LoweredSqlCommand(LoweredSqlCommandInner::Explain {
            mode,
            verbose,
            query: LoweredSqlQuery::Select(query),
        })),
        Err(SqlLoweringError::UnsupportedSelectProjection) => {
            let command = lower_global_aggregate_select_shape(statement)?;

            Ok(LoweredSqlCommand(
                LoweredSqlCommandInner::ExplainGlobalAggregate {
                    mode,
                    verbose,
                    command,
                },
            ))
        }
        Err(err) => Err(err),
    }
}

fn reject_params_in_projection(projection: SqlProjection) -> Result<(), SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(());
    };

    for item in items {
        match item {
            SqlSelectItem::Field(_) => {}
            SqlSelectItem::Aggregate(aggregate) => {
                if let Some(input) = aggregate.input.as_deref() {
                    reject_params_in_expr(input, "SELECT aggregate input")?;
                }
                if let Some(filter) = aggregate.filter_expr.as_deref() {
                    reject_params_in_expr(filter, "SELECT aggregate FILTER")?;
                }
            }
            SqlSelectItem::Expr(expr) => {
                reject_params_in_expr(&expr, "SELECT projection")?;
            }
        }
    }

    Ok(())
}

fn reject_params_in_expr(expr: &SqlExpr, clause: &str) -> Result<(), SqlLoweringError> {
    match expr {
        SqlExpr::Param { index } => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            format!("parameterized {clause} is not supported in 0.98 v1"),
        )),
        SqlExpr::Field(_) | SqlExpr::Literal(_) => Ok(()),
        SqlExpr::Aggregate(aggregate) => {
            if let Some(input) = aggregate.input.as_deref() {
                reject_params_in_expr(input, clause)?;
            }
            if let Some(filter) = aggregate.filter_expr.as_deref() {
                reject_params_in_expr(filter, clause)?;
            }

            Ok(())
        }
        SqlExpr::Membership { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => reject_params_in_expr(expr, clause),
        SqlExpr::FunctionCall { args, .. } => {
            for arg in args {
                reject_params_in_expr(arg, clause)?;
            }

            Ok(())
        }
        SqlExpr::Binary { left, right, .. } => {
            reject_params_in_expr(left, clause)?;
            reject_params_in_expr(right, clause)
        }
        SqlExpr::Case { arms, else_expr } => {
            for arm in arms {
                reject_params_in_expr(&arm.condition, clause)?;
                reject_params_in_expr(&arm.result, clause)?;
            }
            if let Some(else_expr) = else_expr {
                reject_params_in_expr(else_expr, clause)?;
            }

            Ok(())
        }
    }
}

fn collect_where_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    collect_clause_param_contracts(
        PreparedSqlClauseContractScope::Where,
        expr,
        model,
        contracts,
    )
}

const fn prepared_family_from_expr_coarse_family(
    family: ExprCoarseTypeFamily,
) -> PreparedSqlParameterTypeFamily {
    match family {
        ExprCoarseTypeFamily::Bool => PreparedSqlParameterTypeFamily::Bool,
        ExprCoarseTypeFamily::Numeric => PreparedSqlParameterTypeFamily::Numeric,
        ExprCoarseTypeFamily::Text => PreparedSqlParameterTypeFamily::Text,
    }
}

///
/// PreparedSqlFallbackExprScope
///
/// Shared fallback-inference scope for prepared expression-owned parameter
/// contracts. `WHERE` and grouped/global `HAVING` now share one fallback
/// family model, but they still keep distinct lane-admission policy elsewhere,
/// so the scope remains explicit instead of being flattened away.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedSqlFallbackExprScope {
    Where,
    Having,
}

impl PreparedSqlFallbackExprScope {
    const fn phase(self) -> SqlExprPhase {
        match self {
            Self::Where => SqlExprPhase::PreAggregate,
            Self::Having => SqlExprPhase::PostAggregate,
        }
    }

    const fn expression_position_label(self) -> &'static str {
        match self {
            Self::Where => "expression-owned WHERE position",
            Self::Having => "expression-owned HAVING position",
        }
    }
}

fn infer_sql_expr_prepared_family(
    expr: &SqlExpr,
    phase: SqlExprPhase,
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    let lowered = lower_sql_expr(expr, phase)?;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let family = infer_expr_coarse_family(&lowered, schema)
        .map_err(|error| SqlLoweringError::from(QueryError::from(error)))?;

    Ok(family.map(prepared_family_from_expr_coarse_family))
}

///
/// PreparedSqlDynamicFunctionFamilyMetadata
///
/// Shared metadata for fallback-only dynamic function family inference.
/// This keeps the supported dynamic-family functions on one bounded table so
/// result-family inference and argument-family propagation stay aligned.
///

struct PreparedSqlDynamicFunctionFamilyMetadata {
    missing_message: &'static str,
    conflict_label: &'static str,
}

fn dynamic_function_family_metadata(
    scope: PreparedSqlFallbackExprScope,
    function: Function,
    args: &[SqlExpr],
) -> Result<PreparedSqlDynamicFunctionFamilyMetadata, SqlLoweringError> {
    match function {
        Function::Coalesce => Ok(PreparedSqlDynamicFunctionFamilyMetadata {
            missing_message: match scope {
                PreparedSqlFallbackExprScope::Where => {
                    "prepared SQL could not infer one COALESCE contract for the expression-owned WHERE position"
                }
                PreparedSqlFallbackExprScope::Having => {
                    "prepared SQL could not infer one COALESCE contract for the expression-owned HAVING position"
                }
            },
            conflict_label: "COALESCE argument",
        }),
        Function::NullIf => Ok(PreparedSqlDynamicFunctionFamilyMetadata {
            missing_message: match scope {
                PreparedSqlFallbackExprScope::Where => {
                    "prepared SQL could not infer one NULLIF contract for the expression-owned WHERE position"
                }
                PreparedSqlFallbackExprScope::Having => {
                    "prepared SQL could not infer one NULLIF contract for the expression-owned HAVING position"
                }
            },
            conflict_label: "NULLIF argument",
        }),
        _ => Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index_from_args(args),
            "prepared SQL function family is outside the aligned fallback typing surface",
        )),
    }
}

// Infer the coarse boolean-family result of one top-level SQL `WHERE` subtree.
// This keeps expression-owned prepared fallback contracts explicit without
// letting non-boolean subtrees masquerade as standalone predicates.
fn infer_where_bool_expr_param_family(
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    if !sql_expr_contains_param(expr) {
        let family = infer_fallback_value_family_hint(PreparedSqlFallbackExprScope::Where, expr, model)?
            .ok_or_else(|| {
                SqlLoweringError::unsupported_parameter_placement(
                    sql_expr_first_param_index(expr),
                    "prepared SQL could not infer one boolean contract for the expression-owned WHERE position",
                )
            })?;
        if family != PreparedSqlParameterTypeFamily::Bool {
            return Err(SqlLoweringError::unsupported_parameter_placement(
                sql_expr_first_param_index(expr),
                "prepared SQL WHERE parameters must stay on one boolean predicate surface",
            ));
        }

        return Ok(family);
    }

    if sql_expr_is_boolean_shape(expr) {
        Ok(PreparedSqlParameterTypeFamily::Bool)
    } else {
        Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index(expr),
            "prepared SQL WHERE parameters must stay on one boolean predicate surface",
        ))
    }
}

// Map one SQL literal onto the coarse prepared bind family used by v1
// parameter validation. `NULL` stays unresolved here and must inherit one
// family from surrounding context instead of inventing one locally.
fn value_type_family(value: &Value) -> Option<PreparedSqlParameterTypeFamily> {
    coarse_family_for_literal(value).map(prepared_family_from_expr_coarse_family)
}

fn collect_having_param_contracts(
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    collect_clause_param_contracts(
        PreparedSqlClauseContractScope::Having,
        expr,
        model,
        contracts,
    )
}

///
/// PreparedSqlClauseContractScope
///
/// Top-level prepared contract-collection scope for parsed SQL predicate
/// clauses. `WHERE` and grouped/global `HAVING` still keep different policy
/// edges, but they now share one recursive collector instead of duplicating
/// the same logical walk under separate clause-local ladders.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreparedSqlClauseContractScope {
    Where,
    Having,
}

impl PreparedSqlClauseContractScope {
    const fn clause_label(self) -> &'static str {
        match self {
            Self::Where => "WHERE",
            Self::Having => "HAVING",
        }
    }

    const fn fallback_scope(self) -> PreparedSqlFallbackExprScope {
        match self {
            Self::Where => PreparedSqlFallbackExprScope::Where,
            Self::Having => PreparedSqlFallbackExprScope::Having,
        }
    }

    const fn bare_parameter_message(self) -> &'static str {
        match self {
            Self::Where => "bare WHERE parameter is not supported in 0.98 v1",
            Self::Having => "bare HAVING parameter is not supported in 0.98 v1",
        }
    }
}

// Collect one parsed SQL clause under the shared prepared contract-collection
// walk, while leaving the clause-specific admission edges in one scoped helper.
fn collect_clause_param_contracts(
    scope: PreparedSqlClauseContractScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Literal(_) => Ok(()),
        SqlExpr::Param { index } => Err(SqlLoweringError::unsupported_parameter_placement(
            Some(*index),
            scope.bare_parameter_message(),
        )),
        SqlExpr::Aggregate(_) => match scope {
            PreparedSqlClauseContractScope::Where => {
                Err(SqlLoweringError::unsupported_parameter_placement(
                    None,
                    "WHERE does not admit aggregate parameter contracts",
                ))
            }
            PreparedSqlClauseContractScope::Having => Ok(()),
        },
        SqlExpr::Unary { expr, .. } => {
            collect_clause_param_contracts(scope, expr, model, contracts)
        }
        SqlExpr::Membership { .. }
        | SqlExpr::NullTest { .. }
        | SqlExpr::FunctionCall { .. }
        | SqlExpr::Case { .. } => {
            collect_general_clause_expr_param_contracts(scope, expr, model, contracts)
        }
        SqlExpr::Binary { op, left, right } => match op {
            SqlExprBinaryOp::And | SqlExprBinaryOp::Or => {
                collect_clause_param_contracts(scope, left, model, contracts)?;
                collect_clause_param_contracts(scope, right, model, contracts)
            }
            SqlExprBinaryOp::Eq
            | SqlExprBinaryOp::Ne
            | SqlExprBinaryOp::Lt
            | SqlExprBinaryOp::Lte
            | SqlExprBinaryOp::Gt
            | SqlExprBinaryOp::Gte => {
                if matches!(left.as_ref(), SqlExpr::Field(_) | SqlExpr::Aggregate(_))
                    && matches!(right.as_ref(), SqlExpr::Param { .. })
                {
                    collect_compare_param_contract(
                        left,
                        right,
                        model,
                        contracts,
                        scope.clause_label(),
                    )
                } else if sql_expr_contains_param(left) || sql_expr_contains_param(right) {
                    collect_fallback_compare_param_contracts(
                        scope.fallback_scope(),
                        left,
                        right,
                        model,
                        contracts,
                    )
                } else {
                    Ok(())
                }
            }
            SqlExprBinaryOp::Add
            | SqlExprBinaryOp::Sub
            | SqlExprBinaryOp::Mul
            | SqlExprBinaryOp::Div => collect_arithmetic_clause_param_contracts(
                scope, expr, left, right, model, contracts,
            ),
        },
    }
}

// Keep the clause-specific non-compare expression policy explicit while the
// outer recursive walk stays shared.
fn collect_general_clause_expr_param_contracts(
    scope: PreparedSqlClauseContractScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    match (scope, expr) {
        (PreparedSqlClauseContractScope::Where, SqlExpr::Membership { expr, .. }) => {
            if sql_expr_contains_param(expr) {
                let expected =
                    infer_fallback_membership_value_family(scope.fallback_scope(), expr, model)?;
                collect_fallback_value_param_contracts(
                    scope.fallback_scope(),
                    expr,
                    model,
                    Some(expected),
                    contracts,
                )?;
            }

            Ok(())
        }
        (PreparedSqlClauseContractScope::Where, SqlExpr::NullTest { expr, .. }) => {
            if sql_expr_contains_param(expr) {
                collect_fallback_value_param_contracts(
                    scope.fallback_scope(),
                    expr,
                    model,
                    None,
                    contracts,
                )?;
            }

            Ok(())
        }
        (PreparedSqlClauseContractScope::Where, SqlExpr::FunctionCall { args, .. }) => {
            if args.iter().any(sql_expr_contains_param) {
                let expected = infer_where_bool_expr_param_family(expr, model)?;
                collect_fallback_value_param_contracts(
                    scope.fallback_scope(),
                    expr,
                    model,
                    Some(expected),
                    contracts,
                )?;
            }

            Ok(())
        }
        (PreparedSqlClauseContractScope::Where, SqlExpr::Case { .. })
        | (
            PreparedSqlClauseContractScope::Having,
            SqlExpr::Membership { .. }
            | SqlExpr::NullTest { .. }
            | SqlExpr::FunctionCall { .. }
            | SqlExpr::Case { .. },
        ) => {
            if sql_expr_contains_param(expr) {
                collect_fallback_value_param_contracts(
                    scope.fallback_scope(),
                    expr,
                    model,
                    Some(PreparedSqlParameterTypeFamily::Bool),
                    contracts,
                )?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Keep the remaining arithmetic split explicit: `WHERE` still routes param
// arithmetic through fallback family inference, while grouped/global `HAVING`
// continues to fail closed on top-level arithmetic parameter shapes.
fn collect_arithmetic_clause_param_contracts(
    scope: PreparedSqlClauseContractScope,
    expr: &SqlExpr,
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    if !(sql_expr_contains_param(left) || sql_expr_contains_param(right)) {
        return Ok(());
    }

    match scope {
        PreparedSqlClauseContractScope::Where => {
            collect_fallback_value_param_contracts(
                scope.fallback_scope(),
                expr,
                model,
                None,
                contracts,
            )?;

            Ok(())
        }
        PreparedSqlClauseContractScope::Having => {
            Err(SqlLoweringError::unsupported_parameter_placement(
                sql_expr_first_param_index(expr),
                "arithmetic parameter expressions are not supported in 0.98 v1",
            ))
        }
    }
}

fn infer_fallback_value_family_hint(
    scope: PreparedSqlFallbackExprScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    if sql_expr_contains_param(expr) {
        return Ok(None);
    }

    infer_sql_expr_prepared_family(expr, scope.phase(), model)
}

fn infer_fallback_membership_value_family(
    scope: PreparedSqlFallbackExprScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    infer_fallback_value_family_hint(scope, expr, model)?.ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index(expr),
            format!(
                "prepared SQL could not infer one IN target contract for the {}",
                scope.expression_position_label(),
            ),
        )
    })
}

// Collect one expression-owned fallback subtree under the shared prepared
// family-inference model. `WHERE` and grouped/global `HAVING` now share this
// traversal, while the scope still keeps their phase and lane-boundary
// differences explicit in messages and aggregate admission.
fn collect_fallback_value_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    if !sql_expr_contains_param(expr) {
        return resolve_non_param_fallback_family(scope, expr, model, expected);
    }

    match expr {
        SqlExpr::Field(field) => field_compare_type_family(model, field),
        SqlExpr::Aggregate(aggregate) => match scope {
            PreparedSqlFallbackExprScope::Where => {
                Err(SqlLoweringError::unsupported_parameter_placement(
                    sql_expr_first_param_index(expr),
                    "WHERE does not admit aggregate parameter contracts",
                ))
            }
            PreparedSqlFallbackExprScope::Having => aggregate_compare_type_family(aggregate, model),
        },
        SqlExpr::Literal(value) => value_type_family(value).or(expected).ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                None,
                "NULL-only expression parameter positions are not supported in prepared SQL",
            )
        }),
        SqlExpr::Param { index } => {
            let expected = expected.ok_or_else(|| {
                SqlLoweringError::unsupported_parameter_placement(
                    Some(*index),
                    format!(
                        "prepared SQL could not infer a parameter contract for the {}",
                        scope.expression_position_label(),
                    ),
                )
            })?;
            contracts.push(PreparedSqlParameterContract::new(
                *index, expected, true, None,
            ));

            Ok(expected)
        }
        SqlExpr::Membership { expr, .. } => {
            collect_fallback_membership_param_contracts(scope, expr, model, expected, contracts)
        }
        SqlExpr::NullTest { expr, .. } => {
            collect_fallback_value_param_contracts(scope, expr, model, None, contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExpr::Unary { expr, .. } => {
            collect_fallback_value_param_contracts(
                scope,
                expr,
                model,
                Some(PreparedSqlParameterTypeFamily::Bool),
                contracts,
            )?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExpr::Binary { op, left, right } => {
            collect_fallback_binary_param_contracts(scope, *op, left, right, model, contracts)
        }
        SqlExpr::FunctionCall { function, args } => collect_fallback_function_param_contracts(
            scope,
            *function,
            args.as_slice(),
            model,
            expected,
            contracts,
        ),
        SqlExpr::Case { arms, else_expr } => collect_fallback_case_param_contracts(
            scope,
            arms,
            else_expr.as_deref(),
            model,
            expected,
            contracts,
        ),
    }
}

// Collect one fallback membership subtree by inferring the target family from
// either the outer requirement or the static admitted target subtree.
fn collect_fallback_membership_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let expected =
        expected.or_else(|| infer_fallback_membership_value_family(scope, expr, model).ok());
    collect_fallback_value_param_contracts(scope, expr, model, expected, contracts)?;

    Ok(PreparedSqlParameterTypeFamily::Bool)
}

// Collect one fallback binary subtree while preserving the existing coarse
// boolean/compare/numeric family split under one scoped traversal helper.
fn collect_fallback_binary_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    op: SqlExprBinaryOp,
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    match op {
        SqlExprBinaryOp::Or | SqlExprBinaryOp::And => {
            let expected_operand = binary_operand_coarse_family(lower_sql_binary_op(op))
                .map(prepared_family_from_expr_coarse_family)
                .expect("boolean SQL binary operators should keep one shared coarse family");
            collect_fallback_value_children(
                scope,
                [left, right],
                model,
                Some(expected_operand),
                contracts,
            )?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExprBinaryOp::Eq
        | SqlExprBinaryOp::Ne
        | SqlExprBinaryOp::Lt
        | SqlExprBinaryOp::Lte
        | SqlExprBinaryOp::Gt
        | SqlExprBinaryOp::Gte => {
            collect_fallback_compare_param_contracts(scope, left, right, model, contracts)?;

            Ok(PreparedSqlParameterTypeFamily::Bool)
        }
        SqlExprBinaryOp::Add
        | SqlExprBinaryOp::Sub
        | SqlExprBinaryOp::Mul
        | SqlExprBinaryOp::Div => {
            let expected_operand = binary_operand_coarse_family(lower_sql_binary_op(op))
                .map(prepared_family_from_expr_coarse_family)
                .expect("numeric SQL binary operators should keep one shared coarse family");
            collect_fallback_value_children(
                scope,
                [left, right],
                model,
                Some(expected_operand),
                contracts,
            )?;

            Ok(PreparedSqlParameterTypeFamily::Numeric)
        }
    }
}

// Collect one searched CASE subtree by forcing boolean condition contracts and
// one unified result family before descending into parameter-carrying branches.
fn collect_fallback_case_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    arms: &[SqlCaseArm],
    else_expr: Option<&SqlExpr>,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    collect_fallback_value_children(
        scope,
        arms.iter().map(|arm| &arm.condition),
        model,
        Some(PreparedSqlParameterTypeFamily::Bool),
        contracts,
    )?;
    let branch_family = expected
        .or_else(|| {
            infer_planner_case_result_family(scope, arms, else_expr, model)
                .ok()
                .flatten()
        })
        .ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                arms.iter()
                    .find_map(|arm| sql_expr_first_param_index(&arm.result))
                    .or_else(|| else_expr.and_then(sql_expr_first_param_index)),
                format!(
                    "prepared SQL could not infer one CASE result contract for the {}",
                    scope.expression_position_label(),
                ),
            )
        })?;
    collect_fallback_value_children(
        scope,
        arms.iter().map(|arm| &arm.result),
        model,
        Some(branch_family),
        contracts,
    )?;
    if let Some(else_expr) = else_expr {
        collect_fallback_value_param_contracts(
            scope,
            else_expr,
            model,
            Some(branch_family),
            contracts,
        )?;
    }

    Ok(branch_family)
}

// Collect one shared expected contract family across multiple children under
// the same scoped fallback traversal.
fn collect_fallback_value_children<'a>(
    scope: PreparedSqlFallbackExprScope,
    exprs: impl IntoIterator<Item = &'a SqlExpr>,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    for expr in exprs {
        collect_fallback_value_param_contracts(scope, expr, model, expected, contracts)?;
    }

    Ok(())
}

// Infer coarse compare-side parameter contracts for one expression-owned
// fallback compare subtree under the shared scoped collector.
fn collect_fallback_compare_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<(), SqlLoweringError> {
    let right_hint = infer_fallback_value_family_hint(scope, right, model)?;
    let left_family =
        collect_fallback_value_param_contracts(scope, left, model, right_hint, contracts)?;
    let left_hint = infer_fallback_value_family_hint(scope, left, model)?;
    let expected_right = left_hint.or(Some(left_family));
    let right_family =
        collect_fallback_value_param_contracts(scope, right, model, expected_right, contracts)?;

    if left_family != right_family {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index_from_pair(left, right),
            format!(
                "prepared SQL could not unify compare operand contracts for the {} ({left_family:?} vs {right_family:?})",
                scope.expression_position_label(),
            ),
        ));
    }

    Ok(())
}

// Infer one function-owned coarse parameter contract family and collect any
// nested parameter slots under the shared scoped fallback collector.
fn collect_fallback_function_param_contracts(
    scope: PreparedSqlFallbackExprScope,
    function: SqlScalarFunction,
    args: &[SqlExpr],
    model: &'static EntityModel,
    expected_result: Option<PreparedSqlParameterTypeFamily>,
    contracts: &mut Vec<PreparedSqlParameterContract>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let function = lower_sql_scalar_function(function);
    let shared_result_family =
        function_result_coarse_family(function).map(prepared_family_from_expr_coarse_family);
    let inferred_result_family = match shared_result_family {
        Some(family) => family,
        None => infer_dynamic_fallback_function_result_family(
            scope,
            function,
            args,
            model,
            expected_result,
        )?,
    };
    let result_family = expected_result
        .map(|expected| {
            if expected == inferred_result_family {
                Ok(expected)
            } else {
                Err(SqlLoweringError::unsupported_parameter_placement(
                    sql_expr_first_param_index_from_args(args),
                    format!(
                        "prepared SQL inferred {inferred_result_family:?} for one function-owned {} subtree where {expected:?} was required",
                        scope.expression_position_label(),
                    ),
                ))
            }
        })
        .transpose()?
        .unwrap_or(inferred_result_family);

    for (index, arg) in args.iter().enumerate() {
        let expected = function_arg_coarse_family(function, index)
            .map(prepared_family_from_expr_coarse_family)
            .or_else(|| {
                dynamic_function_arg_coarse_family(
                    function,
                    expr_coarse_family_from_prepared_family(result_family),
                )
                .map(prepared_family_from_expr_coarse_family)
            });
        collect_fallback_value_param_contracts(scope, arg, model, expected, contracts)?;
    }

    Ok(result_family)
}

// Lower the result-bearing searched CASE branches that are already visible
// without parameter binding and let the planner own the branch-family
// propagation from that reduced result set.
fn infer_planner_case_result_family(
    scope: PreparedSqlFallbackExprScope,
    arms: &[SqlCaseArm],
    else_expr: Option<&SqlExpr>,
    model: &'static EntityModel,
) -> Result<Option<PreparedSqlParameterTypeFamily>, SqlLoweringError> {
    let mut result_exprs = arms
        .iter()
        .filter(|arm| !sql_expr_contains_param(&arm.result))
        .map(|arm| lower_sql_expr(&arm.result, scope.phase()))
        .collect::<Result<Vec<_>, SqlLoweringError>>()?;
    if let Some(else_expr) = else_expr
        && !sql_expr_contains_param(else_expr)
    {
        result_exprs.push(lower_sql_expr(else_expr, scope.phase())?);
    } else if else_expr.is_none() {
        result_exprs.push(Expr::Literal(Value::Null));
    }
    let schema = SchemaInfo::cached_for_entity_model(model);
    let family = infer_case_result_exprs_coarse_family(result_exprs.iter(), schema)
        .map_err(|error| SqlLoweringError::from(QueryError::from(error)))?;

    Ok(family.map(prepared_family_from_expr_coarse_family))
}

const fn expr_coarse_family_from_prepared_family(
    family: PreparedSqlParameterTypeFamily,
) -> ExprCoarseTypeFamily {
    match family {
        PreparedSqlParameterTypeFamily::Bool => ExprCoarseTypeFamily::Bool,
        PreparedSqlParameterTypeFamily::Numeric => ExprCoarseTypeFamily::Numeric,
        PreparedSqlParameterTypeFamily::Text => ExprCoarseTypeFamily::Text,
    }
}

fn resolve_non_param_fallback_family(
    scope: PreparedSqlFallbackExprScope,
    expr: &SqlExpr,
    model: &'static EntityModel,
    expected: Option<PreparedSqlParameterTypeFamily>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let inferred = infer_sql_expr_prepared_family(expr, scope.phase(), model)?;
    let family = inferred.or(expected).ok_or_else(|| {
        SqlLoweringError::unsupported_parameter_placement(
            None,
            "NULL-only expression parameter positions are not supported in prepared SQL",
        )
    })?;
    if let Some(expected) = expected
        && family != expected
    {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index(expr),
            format!(
                "prepared SQL inferred {family:?} for one {} subtree where {expected:?} was required",
                scope.expression_position_label(),
            ),
        ));
    }

    Ok(family)
}

fn infer_dynamic_fallback_function_result_family(
    scope: PreparedSqlFallbackExprScope,
    function: Function,
    args: &[SqlExpr],
    model: &'static EntityModel,
    expected_result: Option<PreparedSqlParameterTypeFamily>,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    if let Some(expected) = expected_result {
        return Ok(expected);
    }

    let metadata = dynamic_function_family_metadata(scope, function, args)?;
    let lowered_args = args
        .iter()
        .filter(|arg| !sql_expr_contains_param(arg))
        .map(|arg| lower_sql_expr(arg, scope.phase()))
        .collect::<Result<Vec<_>, SqlLoweringError>>()?;
    let schema = SchemaInfo::cached_for_entity_model(model);
    let family = infer_dynamic_function_result_exprs_coarse_family(
        function,
        lowered_args.as_slice(),
        schema,
    )
    .map_err(|_| {
        SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index_from_args(args),
            format!(
                "prepared SQL could not unify {} contracts",
                metadata.conflict_label,
            ),
        )
    })?;

    family
        .map(prepared_family_from_expr_coarse_family)
        .ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(None, metadata.missing_message)
        })
}

fn collect_compare_param_contract(
    left: &SqlExpr,
    right: &SqlExpr,
    model: &'static EntityModel,
    contracts: &mut Vec<PreparedSqlParameterContract>,
    clause: &str,
) -> Result<(), SqlLoweringError> {
    if let SqlExpr::Param { index } = right {
        let contract = infer_compare_target_contract(left, model, *index)?.ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                Some(*index),
                format!(
                    "only field-compare and aggregate-compare {clause} parameter positions are supported in 0.98 v1"
                ),
            )
        })?;
        contracts.push(PreparedSqlParameterContract::new(
            *index,
            contract.family,
            true,
            contract.template_binding,
        ));

        return Ok(());
    }

    if sql_expr_contains_param(left) || sql_expr_contains_param(right) {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index_from_pair(left, right),
            format!("only right-hand compare parameters are supported in {clause} for 0.98 v1"),
        ));
    }

    Ok(())
}

///
/// PreparedSqlCompareTargetContract
///
/// Compare-target parameter contract metadata for one prepared right-hand
/// compare slot. This keeps the supported compare-family and its optional
/// template sentinel bound together so field and aggregate compares do not
/// assemble the same contract through parallel local paths.
///

struct PreparedSqlCompareTargetContract {
    family: PreparedSqlParameterTypeFamily,
    template_binding: Option<Value>,
}

fn infer_compare_target_contract(
    expr: &SqlExpr,
    model: &'static EntityModel,
    index: usize,
) -> Result<Option<PreparedSqlCompareTargetContract>, SqlLoweringError> {
    match expr {
        SqlExpr::Field(field) => {
            let field_kind = field_kind_for_name(model, field)?;

            Ok(Some(PreparedSqlCompareTargetContract {
                family: field_kind_type_family(field_kind)?,
                template_binding: template_binding_for_field_kind(field_kind, index),
            }))
        }
        SqlExpr::Aggregate(aggregate) => Ok(Some(aggregate_compare_target_contract(
            aggregate, model, index,
        )?)),
        _ => Ok(None),
    }
}

fn field_compare_type_family(
    model: &'static EntityModel,
    field: &str,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    let field_kind = field_kind_for_name(model, field)?;

    field_kind_type_family(field_kind)
}

// Resolve the one admitted compare contract for an aggregate target so
// compare-family inference and template binding read from the same aggregate
// shape owner instead of rediscovering MIN/MAX input details separately.
fn aggregate_compare_target_contract(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
    index: usize,
) -> Result<PreparedSqlCompareTargetContract, SqlLoweringError> {
    let family = aggregate_compare_type_family(aggregate, model)?;
    let template_binding = template_binding_for_aggregate_compare(aggregate, model, index)?;

    Ok(PreparedSqlCompareTargetContract {
        family,
        template_binding,
    })
}

fn template_binding_for_aggregate_compare(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
    index: usize,
) -> Result<Option<Value>, SqlLoweringError> {
    let Ok(index_u64) = u64::try_from(index) else {
        return Ok(None);
    };

    match aggregate.kind {
        SqlAggregateKind::Count | SqlAggregateKind::Sum | SqlAggregateKind::Avg => {
            Ok(Some(Value::Uint(u64::MAX.saturating_sub(index_u64))))
        }
        SqlAggregateKind::Min | SqlAggregateKind::Max => {
            let field_kind = aggregate_compare_input_field_kind(aggregate, model)?;

            Ok(template_binding_for_field_kind(field_kind, index))
        }
    }
}

fn aggregate_compare_type_family(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    match aggregate.kind {
        SqlAggregateKind::Count | SqlAggregateKind::Sum | SqlAggregateKind::Avg => {
            Ok(PreparedSqlParameterTypeFamily::Numeric)
        }
        SqlAggregateKind::Min | SqlAggregateKind::Max => {
            field_kind_type_family(aggregate_compare_input_field_kind(aggregate, model)?)
        }
    }
}

// Resolve the one field-backed MIN/MAX input shape admitted by the prepared
// compare surface so family inference and template sentinel construction do
// not drift on separate copies of the same aggregate input validation.
fn aggregate_compare_input_field_kind(
    aggregate: &SqlAggregateCall,
    model: &'static EntityModel,
) -> Result<FieldKind, SqlLoweringError> {
    let Some(input) = aggregate.input.as_deref() else {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            None,
            "target-less MIN/MAX parameter contracts are not supported in 0.98 v1",
        ));
    };
    let SqlExpr::Field(field) = input else {
        return Err(SqlLoweringError::unsupported_parameter_placement(
            sql_expr_first_param_index(input),
            "expression-backed MIN/MAX parameter contracts are not supported in 0.98 v1",
        ));
    };

    field_kind_for_name(model, field)
}

fn field_kind_for_name(
    model: &'static EntityModel,
    field: &str,
) -> Result<FieldKind, SqlLoweringError> {
    model
        .fields()
        .iter()
        .find(|candidate| candidate.name() == field)
        .map(FieldModel::kind)
        .ok_or_else(|| SqlLoweringError::unknown_field(field.to_string()))
}

fn field_kind_type_family(
    field_kind: FieldKind,
) -> Result<PreparedSqlParameterTypeFamily, SqlLoweringError> {
    coarse_family_for_field_kind(&field_kind)
        .map(prepared_family_from_expr_coarse_family)
        .ok_or_else(|| {
            SqlLoweringError::unsupported_parameter_placement(
                None,
                "field kind is outside the initial 0.98 v1 prepared compare-family surface",
            )
        })
}

fn template_binding_for_field_kind(field_kind: FieldKind, index: usize) -> Option<Value> {
    let index_u64 = u64::try_from(index).ok()?;

    match field_kind {
        FieldKind::Int => {
            let offset = i64::try_from(index_u64).ok()?;

            Some(Value::Int(i64::MAX.saturating_sub(offset)))
        }
        FieldKind::Int128 => Some(Value::Int128(Int128::from(
            i128::MAX - i128::from(index_u64),
        ))),
        FieldKind::IntBig => Some(Value::IntBig(Int::from(
            i32::MAX.saturating_sub(i32::try_from(index_u64).unwrap_or(i32::MAX)),
        ))),
        FieldKind::Uint => Some(Value::Uint(u64::MAX.saturating_sub(index_u64))),
        FieldKind::Uint128 => Some(Value::Uint128(Nat128::from(
            u128::MAX - u128::from(index_u64),
        ))),
        FieldKind::UintBig => Some(Value::UintBig(Nat::from(
            u64::MAX.saturating_sub(index_u64),
        ))),
        FieldKind::Float32 => {
            let offset = f32::from(u16::try_from(index_u64.min(1_000)).ok()?);

            Float32::try_new(f32::MAX - offset).map(Value::Float32)
        }
        FieldKind::Float64 => {
            let offset = f64::from(u16::try_from(index_u64.min(1_000)).ok()?);

            Float64::try_new(f64::MAX - offset).map(Value::Float64)
        }
        FieldKind::Decimal { scale } => Some(Value::Decimal(Decimal::from_i128_with_scale(
            i128::MAX - i128::from(index_u64),
            scale,
        ))),
        FieldKind::Duration => Some(Value::Duration(Duration::from_millis(
            u64::MAX.saturating_sub(index_u64),
        ))),
        FieldKind::Timestamp => {
            let offset = i64::try_from(index_u64).ok()?;

            Some(Value::Timestamp(Timestamp::from_millis(i64::MAX - offset)))
        }
        FieldKind::Text => Some(Value::Text(format!(
            "__icydb_prepared_param_text_{index_u64}__"
        ))),
        FieldKind::Relation { key_kind, .. } => template_binding_for_field_kind(*key_kind, index),
        FieldKind::Bool
        | FieldKind::Enum { .. }
        | FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Principal
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => None,
    }
}

fn bind_projection_literals(
    projection: &SqlProjection,
    bindings: &[Value],
) -> Result<SqlProjection, QueryError> {
    match projection {
        SqlProjection::All => Ok(SqlProjection::All),
        SqlProjection::Items(items) => Ok(SqlProjection::Items(
            items
                .iter()
                .map(|item| match item {
                    SqlSelectItem::Field(field) => Ok(SqlSelectItem::Field(field.clone())),
                    SqlSelectItem::Aggregate(aggregate) => Ok(SqlSelectItem::Aggregate(
                        bind_sql_aggregate_literals(aggregate, bindings)?,
                    )),
                    SqlSelectItem::Expr(expr) => {
                        Ok(SqlSelectItem::Expr(bind_sql_expr_literals(expr, bindings)?))
                    }
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
        )),
    }
}

fn bind_sql_aggregate_literals(
    aggregate: &SqlAggregateCall,
    bindings: &[Value],
) -> Result<SqlAggregateCall, QueryError> {
    Ok(SqlAggregateCall {
        kind: aggregate.kind,
        input: aggregate
            .input
            .as_ref()
            .map(|input| bind_sql_expr_literals(input, bindings).map(Box::new))
            .transpose()?,
        filter_expr: aggregate
            .filter_expr
            .as_ref()
            .map(|filter| bind_sql_expr_literals(filter, bindings).map(Box::new))
            .transpose()?,
        distinct: aggregate.distinct,
    })
}

fn bind_sql_expr_literals(expr: &SqlExpr, bindings: &[Value]) -> Result<SqlExpr, QueryError> {
    match expr {
        SqlExpr::Field(field) => Ok(SqlExpr::Field(field.clone())),
        SqlExpr::Aggregate(aggregate) => Ok(SqlExpr::Aggregate(bind_sql_aggregate_literals(
            aggregate, bindings,
        )?)),
        SqlExpr::Literal(value) => Ok(SqlExpr::Literal(value.clone())),
        SqlExpr::Param { index } => {
            let value = bindings.get(*index).ok_or_else(|| {
                QueryError::unsupported_query(format!(
                    "missing prepared SQL binding at index={index}",
                ))
            })?;

            Ok(SqlExpr::Literal(value.clone()))
        }
        SqlExpr::Membership {
            expr,
            values,
            negated,
        } => Ok(SqlExpr::Membership {
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
            values: values.clone(),
            negated: *negated,
        }),
        SqlExpr::NullTest { expr, negated } => Ok(SqlExpr::NullTest {
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
            negated: *negated,
        }),
        SqlExpr::FunctionCall { function, args } => Ok(SqlExpr::FunctionCall {
            function: *function,
            args: args
                .iter()
                .map(|arg| bind_sql_expr_literals(arg, bindings))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        SqlExpr::Unary { op, expr } => Ok(SqlExpr::Unary {
            op: *op,
            expr: Box::new(bind_sql_expr_literals(expr, bindings)?),
        }),
        SqlExpr::Binary { op, left, right } => Ok(SqlExpr::Binary {
            op: *op,
            left: Box::new(bind_sql_expr_literals(left, bindings)?),
            right: Box::new(bind_sql_expr_literals(right, bindings)?),
        }),
        SqlExpr::Case { arms, else_expr } => Ok(SqlExpr::Case {
            arms: arms
                .iter()
                .map(|arm| {
                    Ok(SqlCaseArm {
                        condition: bind_sql_expr_literals(&arm.condition, bindings)?,
                        result: bind_sql_expr_literals(&arm.result, bindings)?,
                    })
                })
                .collect::<Result<Vec<_>, QueryError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| bind_sql_expr_literals(else_expr, bindings).map(Box::new))
                .transpose()?,
        }),
    }
}
