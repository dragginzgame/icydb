#![allow(dead_code)]

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            pipeline::execute_initial_grouped_rows_for_canister,
        },
        query::builder,
        query::plan::{AccessPlannedQuery, LogicalPlan},
        session::sql::{
            SqlCompiledCommandCacheKey, SqlProjectionContract,
            projection::{
                SqlProjectionPayload, execute_sql_projection_rows_for_canister,
                grouped_sql_statement_result_from_page,
            },
        },
        sql::lowering::{
            LoweredSqlQuery, PreparedSqlParameterContract, PreparedSqlParameterTypeFamily,
            PreparedSqlStatement, lower_sql_command_from_prepared_statement,
        },
        sql::parser::{
            SqlAggregateCall, SqlDeleteStatement, SqlExplainTarget, SqlExpr, SqlProjection,
            SqlSelectStatement, SqlStatement,
        },
    },
    traits::{CanisterKind, EntityValue},
    value::Value,
};

///
/// PreparedSqlQuery
///
/// Session-owned prepared reduced-SQL query shape for v1 parameter binding.
/// This keeps parsing, normalization, and parameter-contract collection stable
/// across repeated executions while still reusing the existing bound SQL
/// execution path after literal substitution.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PreparedSqlQuery {
    source_sql: String,
    statement: PreparedSqlStatement,
    parameter_contracts: Vec<PreparedSqlParameterContract>,
    execution_template: Option<PreparedSqlExecutionTemplate>,
}

///
/// PreparedSqlExecutionTemplate
///
/// Internal prepared SQL execution shell for supported fixed-route parameter
/// families. This freezes one planner-owned template plan plus the outward SQL
/// projection contract so repeated executions can bind directly against the
/// template instead of re-entering SQL lowering and shared plan caches.
///

#[derive(Clone, Debug)]
struct PreparedSqlExecutionTemplate {
    authority: EntityAuthority,
    projection: SqlProjectionContract,
    plan: AccessPlannedQuery,
}

impl PreparedSqlQuery {
    #[must_use]
    pub(in crate::db) fn source_sql(&self) -> &str {
        &self.source_sql
    }

    #[must_use]
    pub(in crate::db) const fn parameter_contracts(&self) -> &[PreparedSqlParameterContract] {
        self.parameter_contracts.as_slice()
    }

    #[must_use]
    pub(in crate::db) const fn parameter_count(&self) -> usize {
        self.parameter_contracts.len()
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Prepare one parameterized reduced-SQL query shape for repeated execution.
    pub(in crate::db) fn prepare_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<PreparedSqlQuery, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let statement = crate::db::session::sql::parse_sql_statement_with_attribution(sql)
            .map(|(statement, _)| statement)?;
        Self::ensure_sql_query_statement_supported(&statement)?;

        let authority = EntityAuthority::for_type::<E>();
        let prepared = Self::prepare_sql_statement_for_authority(&statement, authority)?;
        let parameter_contracts = prepared
            .parameter_contracts(authority.model())
            .map_err(QueryError::from_sql_lowering_error)?;
        let execution_template =
            self.build_prepared_sql_execution_template(&prepared, &parameter_contracts, authority)?;

        Ok(PreparedSqlQuery {
            source_sql: sql.to_string(),
            statement: prepared,
            parameter_contracts,
            execution_template,
        })
    }

    /// Execute one prepared reduced-SQL query with one validated binding vector.
    pub(in crate::db) fn execute_prepared_sql_query<E>(
        &self,
        prepared: &PreparedSqlQuery,
        bindings: &[Value],
    ) -> Result<crate::db::session::sql::SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        validate_parameter_bindings(prepared.parameter_contracts(), bindings)?;

        if let Some(template) = prepared.execution_template.as_ref()
            && bindings.iter().all(|value| !matches!(value, Value::Null))
        {
            let bound_plan = bind_prepared_template_plan(
                template.plan.clone(),
                prepared.parameter_contracts(),
                bindings,
                template.authority,
            )?;
            let prepared_plan =
                SharedPreparedExecutionPlan::from_plan(template.authority, bound_plan);

            return self.execute_prepared_template_plan(prepared_plan, &template.projection);
        }

        let bound_statement = prepared.statement.bind_literals(bindings)?;
        let authority = EntityAuthority::for_type::<E>();
        let compiled_cache_key =
            SqlCompiledCommandCacheKey::query_for_entity::<E>(prepared.source_sql());
        let compiled = Self::compile_sql_statement_for_authority(
            &bound_statement,
            authority,
            compiled_cache_key,
        )?
        .0;

        self.execute_compiled_sql::<E>(&compiled)
    }

    // Build one internal fixed-route prepared SQL execution shell when every
    // parameter slot exposes a collision-resistant non-null template binding.
    fn build_prepared_sql_execution_template(
        &self,
        prepared: &PreparedSqlStatement,
        parameter_contracts: &[PreparedSqlParameterContract],
        authority: EntityAuthority,
    ) -> Result<Option<PreparedSqlExecutionTemplate>, QueryError> {
        if parameter_contracts.is_empty() {
            return Ok(None);
        }

        let Some(template_bindings) = parameter_contracts
            .iter()
            .map(|contract| contract.template_binding().cloned())
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        if prepared_statement_contains_template_literal_collision(
            prepared.statement(),
            template_bindings.as_slice(),
        ) {
            return Ok(None);
        }
        let bound_statement = prepared.bind_literals(template_bindings.as_slice())?;
        let normalized_prepared =
            Self::prepare_sql_statement_for_authority(&bound_statement, authority)?;
        let lowered =
            lower_sql_command_from_prepared_statement(normalized_prepared, authority.model())
                .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
            return Ok(None);
        };
        let structural = Self::structural_query_from_lowered_select(select, authority)?;
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;
        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority, plan.clone());
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        Ok(Some(PreparedSqlExecutionTemplate {
            authority,
            projection,
            plan,
        }))
    }

    // Execute one already-bound prepared SQL template plan without routing
    // back through the raw SQL compiled cache or the shared structural cache.
    fn execute_prepared_template_plan(
        &self,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: &SqlProjectionContract,
    ) -> Result<crate::db::session::sql::SqlStatementResult, QueryError> {
        if prepared_plan.logical_plan().grouped_plan().is_some() {
            let authority = prepared_plan.authority();
            let plan = prepared_plan.logical_plan().clone();
            let (columns, fixed_scales) = projection.clone().into_parts();
            let page =
                execute_initial_grouped_rows_for_canister(&self.db, self.debug, authority, plan)
                    .map_err(QueryError::execute)?;
            let statement_result =
                grouped_sql_statement_result_from_page(columns, fixed_scales, page)?;

            Ok(statement_result)
        } else {
            let (columns, fixed_scales) = projection.clone().into_parts();
            let projected =
                execute_sql_projection_rows_for_canister(&self.db, self.debug, prepared_plan)
                    .map_err(QueryError::execute)?;
            let (rows, row_count) = projected.into_parts();
            let payload = SqlProjectionPayload::new(columns, fixed_scales, rows, row_count);

            Ok(payload.into_statement_result())
        }
    }
}

// Refuse the fixed-route template lane when one chosen sentinel already exists
// as an ordinary SQL literal in the prepared statement. The current binding
// step still substitutes by literal-value equality, so reusing that sentinel
// would risk rewriting one user-authored constant during execution.
fn prepared_statement_contains_template_literal_collision(
    statement: &SqlStatement,
    template_bindings: &[Value],
) -> bool {
    match statement {
        SqlStatement::Select(select) => {
            sql_select_contains_template_literal_collision(select, template_bindings)
        }
        SqlStatement::Delete(delete) => {
            sql_delete_contains_template_literal_collision(delete, template_bindings)
        }
        SqlStatement::Explain(explain) => match &explain.statement {
            SqlExplainTarget::Select(select) => {
                sql_select_contains_template_literal_collision(select, template_bindings)
            }
            SqlExplainTarget::Delete(delete) => {
                sql_delete_contains_template_literal_collision(delete, template_bindings)
            }
        },
        SqlStatement::Insert(_)
        | SqlStatement::Update(_)
        | SqlStatement::Describe(_)
        | SqlStatement::ShowIndexes(_)
        | SqlStatement::ShowColumns(_)
        | SqlStatement::ShowEntities(_) => false,
    }
}

// Walk one parsed SQL SELECT statement and report whether it already contains
// one of the chosen template sentinel literals.
fn sql_select_contains_template_literal_collision(
    select: &SqlSelectStatement,
    template_bindings: &[Value],
) -> bool {
    sql_projection_contains_template_literal_collision(&select.projection, template_bindings)
        || select.predicate.as_ref().is_some_and(|expr| {
            sql_expr_contains_template_literal_collision(expr, template_bindings)
        })
        || select
            .having
            .iter()
            .any(|expr| sql_expr_contains_template_literal_collision(expr, template_bindings))
        || select.order_by.iter().any(|term| {
            sql_expr_contains_template_literal_collision(&term.field, template_bindings)
        })
}

// Delete-mode prepared statements only need to scan the predicate branch for
// ordinary literals equal to one chosen template sentinel.
fn sql_delete_contains_template_literal_collision(
    delete: &SqlDeleteStatement,
    template_bindings: &[Value],
) -> bool {
    delete
        .predicate
        .as_ref()
        .is_some_and(|expr| sql_expr_contains_template_literal_collision(expr, template_bindings))
}

// Walk one parsed SQL projection and report whether it already contains one of
// the chosen template sentinel literals.
fn sql_projection_contains_template_literal_collision(
    projection: &SqlProjection,
    template_bindings: &[Value],
) -> bool {
    match projection {
        SqlProjection::All => false,
        SqlProjection::Items(items) => items.iter().any(|item| {
            sql_expr_contains_template_literal_collision(
                &SqlExpr::from_select_item(item),
                template_bindings,
            )
        }),
    }
}

// Aggregate input/filter expressions can carry ordinary literals that would be
// rebound accidentally if they match one of the internal template values.
fn sql_aggregate_contains_template_literal_collision(
    aggregate: &SqlAggregateCall,
    template_bindings: &[Value],
) -> bool {
    aggregate
        .input
        .as_ref()
        .is_some_and(|expr| sql_expr_contains_template_literal_collision(expr, template_bindings))
        || aggregate.filter_expr.as_ref().is_some_and(|expr| {
            sql_expr_contains_template_literal_collision(expr, template_bindings)
        })
}

// Walk one parsed SQL expression tree and detect whether it already contains a
// literal equal to one of the template sentinel values chosen for prepared
// execution binding.
fn sql_expr_contains_template_literal_collision(
    expr: &SqlExpr,
    template_bindings: &[Value],
) -> bool {
    match expr {
        SqlExpr::Field(_) | SqlExpr::Param { .. } => false,
        SqlExpr::Aggregate(aggregate) => {
            sql_aggregate_contains_template_literal_collision(aggregate, template_bindings)
        }
        SqlExpr::Literal(value) => template_bindings.contains(value),
        SqlExpr::Membership { expr, values, .. } => {
            sql_expr_contains_template_literal_collision(expr, template_bindings)
                || values.iter().any(|value| template_bindings.contains(value))
        }
        SqlExpr::NullTest { expr, .. } | SqlExpr::Unary { expr, .. } => {
            sql_expr_contains_template_literal_collision(expr, template_bindings)
        }
        SqlExpr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| sql_expr_contains_template_literal_collision(arg, template_bindings)),
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_contains_template_literal_collision(left, template_bindings)
                || sql_expr_contains_template_literal_collision(right, template_bindings)
        }
        SqlExpr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                sql_expr_contains_template_literal_collision(&arm.condition, template_bindings)
                    || sql_expr_contains_template_literal_collision(&arm.result, template_bindings)
            }) || else_expr.as_ref().is_some_and(|expr| {
                sql_expr_contains_template_literal_collision(expr, template_bindings)
            })
        }
    }
}

fn bind_prepared_template_plan(
    mut plan: AccessPlannedQuery,
    contracts: &[PreparedSqlParameterContract],
    bindings: &[Value],
    authority: EntityAuthority,
) -> Result<AccessPlannedQuery, QueryError> {
    let replacements = contracts
        .iter()
        .filter_map(|contract| {
            contract.template_binding().map(|template_binding| {
                (
                    template_binding.clone(),
                    bindings
                        .get(contract.index())
                        .expect("validated binding vector must cover every contract index")
                        .clone(),
                )
            })
        })
        .collect::<Vec<_>>();

    // Phase 1: bind the logical predicate/HAVING surfaces back to concrete runtime literals.
    match &mut plan.logical {
        LogicalPlan::Scalar(scalar) => {
            scalar.predicate = scalar.predicate.take().map(|predicate| {
                bind_prepared_template_predicate(predicate, replacements.as_slice())
            });
        }
        LogicalPlan::Grouped(grouped) => {
            grouped.scalar.predicate = grouped.scalar.predicate.take().map(|predicate| {
                bind_prepared_template_predicate(predicate, replacements.as_slice())
            });
            grouped.having_expr = grouped
                .having_expr
                .take()
                .map(|expr| bind_prepared_template_expr(expr, replacements.as_slice()));
        }
    }

    // Phase 2: rewrite the concrete access payloads so the executor sees the
    // current bound values without reopening SQL lowering or route selection.
    plan.access = plan.access.bind_runtime_values(replacements.as_slice());

    // Phase 3: rebuild planner-frozen executor residents from the bound plan.
    plan.finalize_planner_route_profile_for_model(authority.model());
    plan.finalize_static_planning_shape_for_model(authority.model())
        .map_err(QueryError::execute)?;

    Ok(plan)
}

fn bind_prepared_template_predicate(
    predicate: crate::db::predicate::Predicate,
    replacements: &[(Value, Value)],
) -> crate::db::predicate::Predicate {
    match predicate {
        crate::db::predicate::Predicate::True => crate::db::predicate::Predicate::True,
        crate::db::predicate::Predicate::False => crate::db::predicate::Predicate::False,
        crate::db::predicate::Predicate::And(children) => crate::db::predicate::Predicate::And(
            children
                .into_iter()
                .map(|child| bind_prepared_template_predicate(child, replacements))
                .collect(),
        ),
        crate::db::predicate::Predicate::Or(children) => crate::db::predicate::Predicate::Or(
            children
                .into_iter()
                .map(|child| bind_prepared_template_predicate(child, replacements))
                .collect(),
        ),
        crate::db::predicate::Predicate::Not(child) => crate::db::predicate::Predicate::Not(
            Box::new(bind_prepared_template_predicate(*child, replacements)),
        ),
        crate::db::predicate::Predicate::Compare(compare) => {
            crate::db::predicate::Predicate::Compare(
                crate::db::predicate::ComparePredicate::with_coercion(
                    compare.field,
                    compare.op,
                    bind_prepared_template_value(compare.value, replacements),
                    compare.coercion.id,
                ),
            )
        }
        crate::db::predicate::Predicate::CompareFields(compare) => {
            crate::db::predicate::Predicate::CompareFields(compare)
        }
        crate::db::predicate::Predicate::IsNull { field } => {
            crate::db::predicate::Predicate::IsNull { field }
        }
        crate::db::predicate::Predicate::IsNotNull { field } => {
            crate::db::predicate::Predicate::IsNotNull { field }
        }
        crate::db::predicate::Predicate::IsMissing { field } => {
            crate::db::predicate::Predicate::IsMissing { field }
        }
        crate::db::predicate::Predicate::IsEmpty { field } => {
            crate::db::predicate::Predicate::IsEmpty { field }
        }
        crate::db::predicate::Predicate::IsNotEmpty { field } => {
            crate::db::predicate::Predicate::IsNotEmpty { field }
        }
        crate::db::predicate::Predicate::TextContains { field, value } => {
            crate::db::predicate::Predicate::TextContains {
                field,
                value: bind_prepared_template_value(value, replacements),
            }
        }
        crate::db::predicate::Predicate::TextContainsCi { field, value } => {
            crate::db::predicate::Predicate::TextContainsCi {
                field,
                value: bind_prepared_template_value(value, replacements),
            }
        }
    }
}

fn bind_prepared_template_expr(
    expr: crate::db::query::plan::expr::Expr,
    replacements: &[(Value, Value)],
) -> crate::db::query::plan::expr::Expr {
    match expr {
        crate::db::query::plan::expr::Expr::Field(field) => {
            crate::db::query::plan::expr::Expr::Field(field)
        }
        crate::db::query::plan::expr::Expr::Literal(value) => {
            crate::db::query::plan::expr::Expr::Literal(bind_prepared_template_value(
                value,
                replacements,
            ))
        }
        crate::db::query::plan::expr::Expr::FunctionCall { function, args } => {
            crate::db::query::plan::expr::Expr::FunctionCall {
                function,
                args: args
                    .into_iter()
                    .map(|arg| bind_prepared_template_expr(arg, replacements))
                    .collect(),
            }
        }
        crate::db::query::plan::expr::Expr::Unary { op, expr } => {
            crate::db::query::plan::expr::Expr::Unary {
                op,
                expr: Box::new(bind_prepared_template_expr(*expr, replacements)),
            }
        }
        crate::db::query::plan::expr::Expr::Binary { op, left, right } => {
            crate::db::query::plan::expr::Expr::Binary {
                op,
                left: Box::new(bind_prepared_template_expr(*left, replacements)),
                right: Box::new(bind_prepared_template_expr(*right, replacements)),
            }
        }
        crate::db::query::plan::expr::Expr::Case {
            when_then_arms,
            else_expr,
        } => crate::db::query::plan::expr::Expr::Case {
            when_then_arms: when_then_arms
                .into_iter()
                .map(|arm| {
                    crate::db::query::plan::expr::CaseWhenArm::new(
                        bind_prepared_template_expr(arm.condition().clone(), replacements),
                        bind_prepared_template_expr(arm.result().clone(), replacements),
                    )
                })
                .collect(),
            else_expr: Box::new(bind_prepared_template_expr(*else_expr, replacements)),
        },
        crate::db::query::plan::expr::Expr::Aggregate(aggregate) => {
            crate::db::query::plan::expr::Expr::Aggregate(bind_prepared_template_aggregate(
                aggregate,
                replacements,
            ))
        }
        #[cfg(test)]
        crate::db::query::plan::expr::Expr::Alias { expr, name } => {
            crate::db::query::plan::expr::Expr::Alias {
                expr: Box::new(bind_prepared_template_expr(*expr, replacements)),
                name,
            }
        }
    }
}

fn bind_prepared_template_aggregate(
    aggregate: crate::db::query::builder::AggregateExpr,
    replacements: &[(Value, Value)],
) -> crate::db::query::builder::AggregateExpr {
    let kind = aggregate.kind();
    let input_expr = aggregate
        .input_expr()
        .cloned()
        .map(|expr| bind_prepared_template_expr(expr, replacements));
    let filter_expr = aggregate
        .filter_expr()
        .cloned()
        .map(|expr| bind_prepared_template_expr(expr, replacements));
    let mut rebound = input_expr.map_or_else(
        || match kind {
            crate::db::query::plan::AggregateKind::Count => builder::aggregate::count(),
            crate::db::query::plan::AggregateKind::Exists => builder::aggregate::exists(),
            crate::db::query::plan::AggregateKind::First => builder::aggregate::first(),
            crate::db::query::plan::AggregateKind::Last => builder::aggregate::last(),
            crate::db::query::plan::AggregateKind::Min => builder::aggregate::min(),
            crate::db::query::plan::AggregateKind::Max => builder::aggregate::max(),
            crate::db::query::plan::AggregateKind::Sum
            | crate::db::query::plan::AggregateKind::Avg => {
                unreachable!("SUM/AVG aggregate templates must preserve one input expression")
            }
        },
        |expr| crate::db::query::builder::AggregateExpr::from_expression_input(kind, expr),
    );

    if let Some(filter_expr) = filter_expr {
        rebound = rebound.with_filter_expr(filter_expr);
    }
    if aggregate.is_distinct() {
        rebound = rebound.distinct();
    }

    rebound
}

fn bind_prepared_template_value(value: Value, replacements: &[(Value, Value)]) -> Value {
    replacements
        .iter()
        .find(|(template, _)| *template == value)
        .map_or(value, |(_, bound)| bound.clone())
}

fn validate_parameter_bindings(
    contracts: &[PreparedSqlParameterContract],
    bindings: &[Value],
) -> Result<(), QueryError> {
    if bindings.len() != contracts.len() {
        return Err(QueryError::unsupported_query(format!(
            "prepared SQL expected {} bindings, found {}",
            contracts.len(),
            bindings.len(),
        )));
    }

    for contract in contracts {
        let binding = bindings.get(contract.index()).ok_or_else(|| {
            QueryError::unsupported_query(format!(
                "missing prepared SQL binding at index={}",
                contract.index(),
            ))
        })?;
        if !binding_matches_contract(binding, contract) {
            return Err(QueryError::unsupported_query(format!(
                "prepared SQL binding at index={} does not match the required {:?} contract",
                contract.index(),
                contract.type_family(),
            )));
        }
    }

    Ok(())
}

const fn binding_matches_contract(value: &Value, contract: &PreparedSqlParameterContract) -> bool {
    if matches!(value, Value::Null) {
        return contract.null_allowed();
    }

    match contract.type_family() {
        PreparedSqlParameterTypeFamily::Numeric => matches!(
            value,
            Value::Int(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Uint(_)
                | Value::Uint128(_)
                | Value::UintBig(_)
                | Value::Float32(_)
                | Value::Float64(_)
                | Value::Decimal(_)
                | Value::Duration(_)
                | Value::Timestamp(_)
        ),
        PreparedSqlParameterTypeFamily::Text => {
            matches!(value, Value::Text(_) | Value::Enum(_))
        }
        PreparedSqlParameterTypeFamily::Bool => matches!(value, Value::Bool(_)),
    }
}
