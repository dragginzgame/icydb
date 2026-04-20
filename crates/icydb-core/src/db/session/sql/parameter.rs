#![allow(dead_code)]

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        access::{AccessPath, AccessPlan},
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
use std::ops::Bound;

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
/// Internal prepared SQL execution variants for supported fixed-route
/// parameter families.
/// `0.99` starts introducing symbolic slot ownership here without widening the
/// planner-wide IR, so legacy sentinel-backed templates and new symbolic
/// templates can coexist while the lane is migrated in bounded slices.
///

#[derive(Clone, Debug)]
enum PreparedSqlExecutionTemplate {
    SymbolicScalar(PreparedSqlSymbolicScalarTemplate),
    SymbolicGroupedHaving(PreparedSqlSymbolicGroupedHavingTemplate),
    Legacy(PreparedSqlLegacyExecutionTemplate),
}

///
/// PreparedSqlLegacyExecutionTemplate
///
/// Legacy fixed-route prepared SQL template that still freezes one concrete
/// planner-owned plan through collision-resistant sentinel values.
/// This stays in place for the existing numeric/text/grouped template surface
/// while `0.99` moves narrower shapes onto symbolic slot ownership.
///

#[derive(Clone, Debug)]
struct PreparedSqlLegacyExecutionTemplate {
    authority: EntityAuthority,
    projection: SqlProjectionContract,
    plan: AccessPlannedQuery,
}

///
/// PreparedSqlSymbolicScalarTemplate
///
/// First symbolic-slot prepared SQL template family for `0.99`.
/// This owns one scalar compare-family prepared route through symbolic slot
/// metadata so scalar prepared queries can stay on the fast lane without
/// sentinel literals in execution.
///

#[derive(Clone, Debug)]
struct PreparedSqlSymbolicScalarTemplate {
    authority: EntityAuthority,
    projection: SqlProjectionContract,
    plan: AccessPlannedQuery,
    predicate: Option<PreparedSqlScalarPredicateTemplate>,
    access: Option<PreparedSqlScalarAccessPathTemplate>,
}

///
/// PreparedSqlSymbolicGroupedHavingTemplate
///
/// Symbolic grouped prepared SQL template for the first grouped `0.99` slice.
/// This keeps the grouped route frozen through one exemplar plan while the
/// post-aggregate `HAVING` expression reads runtime values through slot-owned
/// literal leaves instead of sentinel replacement.
///

#[derive(Clone, Debug)]
struct PreparedSqlSymbolicGroupedHavingTemplate {
    authority: EntityAuthority,
    projection: SqlProjectionContract,
    plan: AccessPlannedQuery,
    having_expr: PreparedSqlGroupedExprTemplate,
}

///
/// PreparedSqlScalarCompareSlotTemplate
///
/// Frozen compare slot metadata for one symbolic scalar prepared template.
/// The template keeps the planner-chosen field/operator/coercion shape and
/// reads only the runtime value from the referenced binding slot.
///

#[derive(Clone, Debug)]
struct PreparedSqlScalarCompareSlotTemplate {
    field: String,
    op: crate::db::predicate::CompareOp,
    coercion: crate::db::predicate::CoercionId,
    slot_index: usize,
}

///
/// PreparedSqlScalarPredicateTemplate
///
/// Symbolic scalar prepared predicate template for the first `0.99` lane.
/// This keeps logical predicate ownership local to prepared SQL execution
/// while reusing planner-owned compare metadata gathered during lowering.
///

#[derive(Clone, Debug)]
enum PreparedSqlScalarPredicateTemplate {
    Compare(PreparedSqlScalarCompareSlotTemplate),
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
}

///
/// PreparedSqlScalarAccessValueTemplate
///
/// Frozen scalar access payload leaf for the first symbolic access slice.
/// Static leaves keep planner-owned access literals unchanged, while slot
/// leaves read their runtime value through the prepared binding vector.
///

#[derive(Clone, Debug)]
enum PreparedSqlScalarAccessValueTemplate {
    Static(Value),
    SlotLiteral(usize),
}

///
/// PreparedSqlScalarAccessPathTemplate
///
/// Symbolic scalar access payload template for the first `0.99` access slice.
/// This stays intentionally narrow on one single-path secondary prefix route
/// so prepared SQL can stop mutating access payloads by sentinel replacement.
///

#[derive(Clone, Debug)]
enum PreparedSqlScalarAccessPathTemplate {
    IndexPrefix {
        index: crate::model::index::IndexModel,
        values: Vec<PreparedSqlScalarAccessValueTemplate>,
    },
}

///
/// PreparedSqlGroupedExprTemplate
///
/// Symbolic grouped post-aggregate expression template for the first grouped
/// `0.99` slice. Static subtrees keep planner-owned `Expr` nodes directly,
/// while bound literal leaves read their runtime value through slot identity.
///

#[derive(Clone, Debug)]
enum PreparedSqlGroupedExprTemplate {
    Static(crate::db::query::plan::expr::Expr),
    SlotLiteral(usize),
    Unary {
        op: crate::db::query::plan::expr::UnaryOp,
        expr: Box<Self>,
    },
    Binary {
        op: crate::db::query::plan::expr::BinaryOp,
        left: Box<Self>,
        right: Box<Self>,
    },
}

///
/// PreparedSqlExecutionTemplateKind
///
/// Test-only classifier for the internal prepared SQL execution template lane.
/// This keeps `0.99` migration coverage precise without exposing the internal
/// template representation to production callers.
///

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PreparedSqlExecutionTemplateKind {
    SymbolicScalar,
    SymbolicGroupedHaving,
    Legacy,
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn template_kind_for_test(
        &self,
    ) -> Option<PreparedSqlExecutionTemplateKind> {
        match self.execution_template.as_ref() {
            Some(PreparedSqlExecutionTemplate::SymbolicScalar(_)) => {
                Some(PreparedSqlExecutionTemplateKind::SymbolicScalar)
            }
            Some(PreparedSqlExecutionTemplate::SymbolicGroupedHaving(_)) => {
                Some(PreparedSqlExecutionTemplateKind::SymbolicGroupedHaving)
            }
            Some(PreparedSqlExecutionTemplate::Legacy(_)) => {
                Some(PreparedSqlExecutionTemplateKind::Legacy)
            }
            None => None,
        }
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
            let bound_plan = match template {
                PreparedSqlExecutionTemplate::SymbolicScalar(template) => {
                    bind_symbolic_scalar_template_plan(
                        template.plan.clone(),
                        template.predicate.as_ref(),
                        template.access.as_ref(),
                        bindings,
                        template.authority,
                    )?
                }
                PreparedSqlExecutionTemplate::SymbolicGroupedHaving(template) => {
                    bind_symbolic_grouped_having_template_plan(
                        template.plan.clone(),
                        &template.having_expr,
                        bindings,
                        template.authority,
                    )?
                }
                PreparedSqlExecutionTemplate::Legacy(template) => bind_prepared_template_plan(
                    template.plan.clone(),
                    prepared.parameter_contracts(),
                    bindings,
                    template.authority,
                )?,
            };
            let prepared_plan =
                SharedPreparedExecutionPlan::from_plan(bound_plan.authority(), bound_plan.plan);

            let projection = match template {
                PreparedSqlExecutionTemplate::SymbolicScalar(template) => &template.projection,
                PreparedSqlExecutionTemplate::SymbolicGroupedHaving(template) => {
                    &template.projection
                }
                PreparedSqlExecutionTemplate::Legacy(template) => &template.projection,
            };

            return self.execute_prepared_template_plan(prepared_plan, projection);
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
        if let Some(template) = self.build_symbolic_scalar_prepared_sql_execution_template(
            prepared,
            parameter_contracts,
            authority,
        )? {
            return Ok(Some(template));
        }
        if let Some(template) = self.build_symbolic_grouped_having_prepared_sql_execution_template(
            prepared,
            parameter_contracts,
            authority,
        )? {
            return Ok(Some(template));
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

        Ok(Some(PreparedSqlExecutionTemplate::Legacy(
            PreparedSqlLegacyExecutionTemplate {
                authority,
                projection,
                plan,
            },
        )))
    }

    // Build the first symbolic-slot prepared SQL template family for `0.99`.
    // This slice stays disciplined on purpose: scalar compare-family routes
    // only, with access binding widened only for one single-path index-prefix
    // access payload shape.
    fn build_symbolic_scalar_prepared_sql_execution_template(
        &self,
        prepared: &PreparedSqlStatement,
        parameter_contracts: &[PreparedSqlParameterContract],
        authority: EntityAuthority,
    ) -> Result<Option<PreparedSqlExecutionTemplate>, QueryError> {
        if parameter_contracts.is_empty() {
            return Ok(None);
        }

        // Phase 1: admit only one scalar prepared SELECT with one compare-only
        // predicate shape owned entirely by the logical predicate tree.
        let SqlStatement::Select(select) = prepared.statement() else {
            return Ok(None);
        };
        let Some(sql_predicate) = select.predicate.as_ref() else {
            return Ok(None);
        };

        // Phase 2: compile one exemplar plan so the structural route stays
        // planner-owned while the scalar predicate moves onto a symbolic
        // template instantiated from slot metadata at execute time.
        let Some(exemplar_bindings) = parameter_contracts
            .iter()
            .map(prepared_sql_contract_exemplar_binding)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let bound_statement = prepared.bind_literals(exemplar_bindings.as_slice())?;
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

        // Phase 3: freeze the symbolic predicate and, when needed, one
        // symbolic access payload only when the lowered scalar plan still
        // matches the admitted compare-family shape exactly.
        let LogicalPlan::Scalar(scalar) = &plan.logical else {
            return Ok(None);
        };

        let predicate_template = match scalar.predicate.as_ref() {
            Some(predicate) => {
                let Some(predicate_template) =
                    build_symbolic_scalar_predicate_template(sql_predicate, predicate)
                else {
                    return Ok(None);
                };

                Some(predicate_template)
            }
            None => None,
        };
        let Ok(access_template) = build_symbolic_scalar_access_path_template(
            &plan.access,
            parameter_contracts,
            exemplar_bindings.as_slice(),
        ) else {
            return Ok(None);
        };
        if access_template.is_some()
            && prepared_statement_contains_template_literal_collision(
                prepared.statement(),
                exemplar_bindings.as_slice(),
            )
        {
            return Ok(None);
        }
        if predicate_template.is_none() && access_template.is_none() {
            return Ok(None);
        }

        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority, plan.clone());
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        Ok(Some(PreparedSqlExecutionTemplate::SymbolicScalar(
            PreparedSqlSymbolicScalarTemplate {
                authority,
                projection,
                plan,
                predicate: predicate_template,
                access: access_template,
            },
        )))
    }

    // Build the first grouped symbolic-slot prepared SQL template family for
    // `0.99`: compare-family HAVING expressions only, with grouped routing and
    // access ownership still frozen through one exemplar plan.
    fn build_symbolic_grouped_having_prepared_sql_execution_template(
        &self,
        prepared: &PreparedSqlStatement,
        parameter_contracts: &[PreparedSqlParameterContract],
        authority: EntityAuthority,
    ) -> Result<Option<PreparedSqlExecutionTemplate>, QueryError> {
        if parameter_contracts.is_empty() {
            return Ok(None);
        }

        // Phase 1: compile one exemplar grouped plan so route/projection stay
        // planner-owned while post-aggregate slot binding becomes symbolic.
        let Some(exemplar_bindings) = parameter_contracts
            .iter()
            .map(prepared_sql_contract_exemplar_binding)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        if prepared_statement_contains_template_literal_collision(
            prepared.statement(),
            exemplar_bindings.as_slice(),
        ) {
            return Ok(None);
        }
        let bound_statement = prepared.bind_literals(exemplar_bindings.as_slice())?;
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

        // Phase 2: freeze the symbolic grouped HAVING template only when the
        // lowered grouped plan still exposes one post-aggregate expression and
        // the scalar predicate stayed outside the parameterized grouped slice.
        let LogicalPlan::Grouped(grouped) = &plan.logical else {
            return Ok(None);
        };
        if grouped.scalar.predicate.is_some() {
            return Ok(None);
        }
        let Some(having_expr) = grouped.having_expr.as_ref() else {
            return Ok(None);
        };
        if !access_plan_is_full_scan(&plan.access) {
            return Ok(None);
        }
        let Some(having_template) = build_symbolic_grouped_expr_template(
            having_expr,
            parameter_contracts,
            exemplar_bindings.as_slice(),
        ) else {
            return Ok(None);
        };

        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority, plan.clone());
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        Ok(Some(PreparedSqlExecutionTemplate::SymbolicGroupedHaving(
            PreparedSqlSymbolicGroupedHavingTemplate {
                authority,
                projection,
                plan,
                having_expr: having_template,
            },
        )))
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

///
/// BoundPreparedTemplatePlan
///
/// One freshly instantiated prepared template plan plus its frozen authority.
/// This keeps the symbolic and legacy bind paths on one shared return shape
/// before they hand off to `SharedPreparedExecutionPlan`.
///

#[derive(Clone, Debug)]
struct BoundPreparedTemplatePlan {
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
}

impl BoundPreparedTemplatePlan {
    #[must_use]
    const fn new(authority: EntityAuthority, plan: AccessPlannedQuery) -> Self {
        Self { authority, plan }
    }

    #[must_use]
    const fn authority(&self) -> EntityAuthority {
        self.authority
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
) -> Result<BoundPreparedTemplatePlan, QueryError> {
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

    Ok(BoundPreparedTemplatePlan::new(authority, plan))
}

// Instantiate one symbolic scalar compare-family template back into the normal
// planner-owned query plan without relying on sentinel replacement.
fn bind_symbolic_scalar_template_plan(
    mut plan: AccessPlannedQuery,
    predicate_template: Option<&PreparedSqlScalarPredicateTemplate>,
    access_template: Option<&PreparedSqlScalarAccessPathTemplate>,
    bindings: &[Value],
    authority: EntityAuthority,
) -> Result<BoundPreparedTemplatePlan, QueryError> {
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        return Err(QueryError::unsupported_query(
            "symbolic scalar prepared template expected scalar logical plan",
        ));
    };
    if let Some(predicate_template) = predicate_template {
        // Phase 1: rebuild the slot-owned compare-family predicate with the
        // current runtime binding environment.
        let predicate =
            instantiate_symbolic_scalar_predicate_template(predicate_template, bindings)?;
        scalar.predicate = Some(predicate);
    }
    if let Some(access_template) = access_template {
        // Phase 2: rebuild the slot-owned access payload without reopening
        // route selection or relying on sentinel-value replacement.
        plan.access = instantiate_symbolic_scalar_access_path_template(access_template, bindings)?;
    }

    // Phase 3: rebuild planner-frozen executor residents from the rebound plan.
    plan.finalize_planner_route_profile_for_model(authority.model());
    plan.finalize_static_planning_shape_for_model(authority.model())
        .map_err(QueryError::execute)?;

    Ok(BoundPreparedTemplatePlan::new(authority, plan))
}

// Instantiate one symbolic grouped HAVING template back into the normal
// planner-owned grouped plan without relying on sentinel replacement.
fn bind_symbolic_grouped_having_template_plan(
    mut plan: AccessPlannedQuery,
    having_template: &PreparedSqlGroupedExprTemplate,
    bindings: &[Value],
    authority: EntityAuthority,
) -> Result<BoundPreparedTemplatePlan, QueryError> {
    // Phase 1: rebuild the slot-owned grouped HAVING expression.
    let having_expr = instantiate_symbolic_grouped_expr_template(having_template, bindings)?;
    let LogicalPlan::Grouped(grouped) = &mut plan.logical else {
        return Err(QueryError::unsupported_query(
            "symbolic grouped prepared template expected grouped logical plan",
        ));
    };
    grouped.having_expr = Some(having_expr);

    // Phase 2: rebuild planner-frozen executor residents from the rebound plan.
    plan.finalize_planner_route_profile_for_model(authority.model());
    plan.finalize_static_planning_shape_for_model(authority.model())
        .map_err(QueryError::execute)?;

    Ok(BoundPreparedTemplatePlan::new(authority, plan))
}

// Return one exemplar binding for internal symbolic-template plan compilation.
// `0.99` still compiles one concrete exemplar plan to freeze routing/projection,
// but slot identity comes from the symbolic predicate template rather than from
// these concrete values during execution.
fn prepared_sql_contract_exemplar_binding(
    contract: &PreparedSqlParameterContract,
) -> Option<Value> {
    match contract.type_family() {
        PreparedSqlParameterTypeFamily::Bool => Some(Value::Bool(false)),
        PreparedSqlParameterTypeFamily::Numeric | PreparedSqlParameterTypeFamily::Text => {
            contract.template_binding().cloned()
        }
    }
}

// Build one symbolic scalar access payload template when the selected access
// path carries slot-owned values and the current `0.99` slice knows how to
// rebuild that payload without sentinel replacement.
fn build_symbolic_scalar_access_path_template(
    access: &AccessPlan<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> Result<Option<PreparedSqlScalarAccessPathTemplate>, QueryError> {
    match access {
        AccessPlan::Path(path) => build_symbolic_scalar_access_path_template_from_path(
            path.as_ref(),
            parameter_contracts,
            exemplar_bindings,
        ),
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
            if access_plan_contains_symbolic_slot_value(
                access,
                parameter_contracts,
                exemplar_bindings,
            ) {
                return Err(QueryError::unsupported_query(
                    "symbolic scalar prepared template does not yet support composite access trees",
                ));
            }

            Ok(None)
        }
    }
}

// Build one symbolic scalar access payload template from one single selected
// access path when that path carries slot-owned values.
fn build_symbolic_scalar_access_path_template_from_path(
    path: &AccessPath<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> Result<Option<PreparedSqlScalarAccessPathTemplate>, QueryError> {
    match path {
        AccessPath::IndexPrefix { index, values } => {
            let mut had_slot = false;
            let mut templates = Vec::with_capacity(values.len());
            for value in values {
                let template = build_symbolic_scalar_access_value_template(
                    value,
                    parameter_contracts,
                    exemplar_bindings,
                );
                had_slot |= matches!(
                    template,
                    PreparedSqlScalarAccessValueTemplate::SlotLiteral(_)
                );
                templates.push(template);
            }
            if !had_slot {
                return Ok(None);
            }

            Ok(Some(PreparedSqlScalarAccessPathTemplate::IndexPrefix {
                index: *index,
                values: templates,
            }))
        }
        AccessPath::ByKey(_)
        | AccessPath::ByKeys(_)
        | AccessPath::KeyRange { .. }
        | AccessPath::IndexMultiLookup { .. }
        | AccessPath::IndexRange { .. }
        | AccessPath::FullScan => {
            if access_path_contains_symbolic_slot_value(
                path,
                parameter_contracts,
                exemplar_bindings,
            ) {
                return Err(QueryError::unsupported_query(
                    "symbolic scalar prepared template does not yet support this access payload shape",
                ));
            }

            Ok(None)
        }
    }
}

// Return one scalar access payload leaf template by resolving an exemplar
// access literal back to one unique prepared binding slot when possible.
fn build_symbolic_scalar_access_value_template(
    value: &Value,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> PreparedSqlScalarAccessValueTemplate {
    prepared_sql_slot_index_for_exemplar_value(value, parameter_contracts, exemplar_bindings)
        .map_or_else(
            || PreparedSqlScalarAccessValueTemplate::Static(value.clone()),
            PreparedSqlScalarAccessValueTemplate::SlotLiteral,
        )
}

// Return whether one access tree still carries exemplar values owned by one
// prepared binding slot.
fn access_plan_contains_symbolic_slot_value(
    access: &AccessPlan<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> bool {
    match access {
        AccessPlan::Path(path) => {
            access_path_contains_symbolic_slot_value(path, parameter_contracts, exemplar_bindings)
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().any(|child| {
                access_plan_contains_symbolic_slot_value(
                    child,
                    parameter_contracts,
                    exemplar_bindings,
                )
            })
        }
    }
}

// Return whether one single selected access path still carries exemplar values
// owned by one prepared binding slot.
fn access_path_contains_symbolic_slot_value(
    path: &AccessPath<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> bool {
    match path {
        AccessPath::ByKey(key) => {
            prepared_sql_slot_index_for_exemplar_value(key, parameter_contracts, exemplar_bindings)
                .is_some()
        }
        AccessPath::ByKeys(keys) => keys.iter().any(|key| {
            prepared_sql_slot_index_for_exemplar_value(key, parameter_contracts, exemplar_bindings)
                .is_some()
        }),
        AccessPath::KeyRange { start, end } => {
            prepared_sql_slot_index_for_exemplar_value(
                start,
                parameter_contracts,
                exemplar_bindings,
            )
            .is_some()
                || prepared_sql_slot_index_for_exemplar_value(
                    end,
                    parameter_contracts,
                    exemplar_bindings,
                )
                .is_some()
        }
        AccessPath::IndexPrefix { values, .. } | AccessPath::IndexMultiLookup { values, .. } => {
            values.iter().any(|value| {
                prepared_sql_slot_index_for_exemplar_value(
                    value,
                    parameter_contracts,
                    exemplar_bindings,
                )
                .is_some()
            })
        }
        AccessPath::IndexRange { spec } => {
            spec.prefix_values().iter().any(|value| {
                prepared_sql_slot_index_for_exemplar_value(
                    value,
                    parameter_contracts,
                    exemplar_bindings,
                )
                .is_some()
            }) || bound_contains_symbolic_slot_value(
                spec.lower(),
                parameter_contracts,
                exemplar_bindings,
            ) || bound_contains_symbolic_slot_value(
                spec.upper(),
                parameter_contracts,
                exemplar_bindings,
            )
        }
        AccessPath::FullScan => false,
    }
}

// Return whether one bound endpoint still carries an exemplar value owned by a
// prepared binding slot.
fn bound_contains_symbolic_slot_value(
    bound: &Bound<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> bool {
    match bound {
        Bound::Unbounded => false,
        Bound::Included(value) | Bound::Excluded(value) => {
            prepared_sql_slot_index_for_exemplar_value(
                value,
                parameter_contracts,
                exemplar_bindings,
            )
            .is_some()
        }
    }
}

// Return one unique prepared binding slot index for one exemplar literal value
// when that value comes from the admitted non-bool prepared compare families.
fn prepared_sql_slot_index_for_exemplar_value(
    value: &Value,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> Option<usize> {
    let mut matching_slot = None;
    for (contract, exemplar) in parameter_contracts.iter().zip(exemplar_bindings.iter()) {
        if contract.type_family() == PreparedSqlParameterTypeFamily::Bool || exemplar != value {
            continue;
        }
        if matching_slot.is_some() {
            return None;
        }
        matching_slot = Some(contract.index());
    }

    matching_slot
}

// Instantiate one symbolic scalar access payload template back into the
// planner-owned access tree without relying on sentinel replacement.
fn instantiate_symbolic_scalar_access_path_template(
    template: &PreparedSqlScalarAccessPathTemplate,
    bindings: &[Value],
) -> Result<AccessPlan<Value>, QueryError> {
    match template {
        PreparedSqlScalarAccessPathTemplate::IndexPrefix { index, values } => {
            let mut bound_values = Vec::with_capacity(values.len());
            for value in values {
                bound_values.push(instantiate_symbolic_scalar_access_value_template(
                    value, bindings,
                )?);
            }

            Ok(AccessPlan::index_prefix(*index, bound_values))
        }
    }
}

// Instantiate one symbolic scalar access leaf with the current runtime
// binding vector.
fn instantiate_symbolic_scalar_access_value_template(
    template: &PreparedSqlScalarAccessValueTemplate,
    bindings: &[Value],
) -> Result<Value, QueryError> {
    match template {
        PreparedSqlScalarAccessValueTemplate::Static(value) => Ok(value.clone()),
        PreparedSqlScalarAccessValueTemplate::SlotLiteral(slot_index) => {
            bindings.get(*slot_index).cloned().ok_or_else(|| {
                QueryError::unsupported_query(format!(
                    "missing prepared SQL binding at index={slot_index}",
                ))
            })
        }
    }
}

// Return whether one planned access tree carries no bound-value payload and
// therefore can be reused directly by the first symbolic scalar template slice.
fn access_plan_is_full_scan(plan: &AccessPlan<Value>) -> bool {
    matches!(plan, AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan))
}

// Build one symbolic scalar prepared predicate template by pairing the
// prepared SQL predicate structure with the lowered predicate compare metadata.
fn build_symbolic_scalar_predicate_template(
    sql_expr: &SqlExpr,
    predicate: &crate::db::predicate::Predicate,
) -> Option<PreparedSqlScalarPredicateTemplate> {
    match (sql_expr, predicate) {
        (
            SqlExpr::Binary {
                op: crate::db::sql::parser::SqlExprBinaryOp::And,
                left,
                right,
            },
            crate::db::predicate::Predicate::And(children),
        ) if children.len() == 2 => Some(PreparedSqlScalarPredicateTemplate::And(vec![
            build_symbolic_scalar_predicate_template(left, &children[0])?,
            build_symbolic_scalar_predicate_template(right, &children[1])?,
        ])),
        (
            SqlExpr::Binary {
                op: crate::db::sql::parser::SqlExprBinaryOp::Or,
                left,
                right,
            },
            crate::db::predicate::Predicate::Or(children),
        ) if children.len() == 2 => Some(PreparedSqlScalarPredicateTemplate::Or(vec![
            build_symbolic_scalar_predicate_template(left, &children[0])?,
            build_symbolic_scalar_predicate_template(right, &children[1])?,
        ])),
        (SqlExpr::Unary { expr, .. }, crate::db::predicate::Predicate::Not(child)) => {
            Some(PreparedSqlScalarPredicateTemplate::Not(Box::new(
                build_symbolic_scalar_predicate_template(expr, child)?,
            )))
        }
        (
            SqlExpr::Binary {
                op:
                    crate::db::sql::parser::SqlExprBinaryOp::Eq
                    | crate::db::sql::parser::SqlExprBinaryOp::Ne
                    | crate::db::sql::parser::SqlExprBinaryOp::Lt
                    | crate::db::sql::parser::SqlExprBinaryOp::Lte
                    | crate::db::sql::parser::SqlExprBinaryOp::Gt
                    | crate::db::sql::parser::SqlExprBinaryOp::Gte,
                left,
                right,
            },
            crate::db::predicate::Predicate::Compare(compare),
        ) => match (&**left, &**right) {
            (SqlExpr::Field(_), SqlExpr::Param { index }) => Some(
                PreparedSqlScalarPredicateTemplate::Compare(PreparedSqlScalarCompareSlotTemplate {
                    field: compare.field.clone(),
                    op: compare.op,
                    coercion: compare.coercion.id,
                    slot_index: *index,
                }),
            ),
            _ => None,
        },
        _ => None,
    }
}

// Instantiate one symbolic scalar predicate template with the current binding
// vector and rebuild a normal planner-owned predicate tree.
fn instantiate_symbolic_scalar_predicate_template(
    template: &PreparedSqlScalarPredicateTemplate,
    bindings: &[Value],
) -> Result<crate::db::predicate::Predicate, QueryError> {
    match template {
        PreparedSqlScalarPredicateTemplate::Compare(compare) => {
            let binding = bindings
                .get(compare.slot_index)
                .ok_or_else(|| {
                    QueryError::unsupported_query(format!(
                        "missing prepared SQL binding at index={}",
                        compare.slot_index,
                    ))
                })?
                .clone();

            Ok(crate::db::predicate::Predicate::Compare(
                crate::db::predicate::ComparePredicate::with_coercion(
                    compare.field.clone(),
                    compare.op,
                    binding,
                    compare.coercion,
                ),
            ))
        }
        PreparedSqlScalarPredicateTemplate::And(children) => {
            Ok(crate::db::predicate::Predicate::And(
                children
                    .iter()
                    .map(|child| instantiate_symbolic_scalar_predicate_template(child, bindings))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        PreparedSqlScalarPredicateTemplate::Or(children) => {
            Ok(crate::db::predicate::Predicate::Or(
                children
                    .iter()
                    .map(|child| instantiate_symbolic_scalar_predicate_template(child, bindings))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        PreparedSqlScalarPredicateTemplate::Not(child) => {
            Ok(crate::db::predicate::Predicate::Not(Box::new(
                instantiate_symbolic_scalar_predicate_template(child, bindings)?,
            )))
        }
    }
}

// Build one symbolic grouped post-aggregate expression template by walking the
// lowered planner-owned HAVING tree and turning exemplar-bound literal leaves
// back into slot references for the current prepared contracts.
fn build_symbolic_grouped_expr_template(
    expr: &crate::db::query::plan::expr::Expr,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> Option<PreparedSqlGroupedExprTemplate> {
    match expr {
        crate::db::query::plan::expr::Expr::Literal(value) => {
            let slot_index = parameter_contracts.iter().find_map(|contract| {
                exemplar_bindings
                    .get(contract.index())
                    .filter(|binding| *binding == value)
                    .map(|_| contract.index())
            });

            Some(slot_index.map_or_else(
                || PreparedSqlGroupedExprTemplate::Static(expr.clone()),
                PreparedSqlGroupedExprTemplate::SlotLiteral,
            ))
        }
        crate::db::query::plan::expr::Expr::Unary { op, expr } => {
            let child =
                build_symbolic_grouped_expr_template(expr, parameter_contracts, exemplar_bindings)?;
            if matches!(child, PreparedSqlGroupedExprTemplate::Static(_)) {
                Some(PreparedSqlGroupedExprTemplate::Static(
                    crate::db::query::plan::expr::Expr::Unary {
                        op: *op,
                        expr: Box::new(
                            instantiate_symbolic_grouped_expr_template(&child, exemplar_bindings)
                                .ok()?,
                        ),
                    },
                ))
            } else {
                Some(PreparedSqlGroupedExprTemplate::Unary {
                    op: *op,
                    expr: Box::new(child),
                })
            }
        }
        crate::db::query::plan::expr::Expr::Binary { op, left, right } => {
            let left_template =
                build_symbolic_grouped_expr_template(left, parameter_contracts, exemplar_bindings)?;
            let right_template = build_symbolic_grouped_expr_template(
                right,
                parameter_contracts,
                exemplar_bindings,
            )?;
            if matches!(left_template, PreparedSqlGroupedExprTemplate::Static(_))
                && matches!(right_template, PreparedSqlGroupedExprTemplate::Static(_))
            {
                Some(PreparedSqlGroupedExprTemplate::Static(expr.clone()))
            } else {
                Some(PreparedSqlGroupedExprTemplate::Binary {
                    op: *op,
                    left: Box::new(left_template),
                    right: Box::new(right_template),
                })
            }
        }
        _ => Some(PreparedSqlGroupedExprTemplate::Static(expr.clone())),
    }
}

// Instantiate one symbolic grouped expression template with the current
// binding vector and rebuild a normal planner-owned grouped HAVING tree.
fn instantiate_symbolic_grouped_expr_template(
    template: &PreparedSqlGroupedExprTemplate,
    bindings: &[Value],
) -> Result<crate::db::query::plan::expr::Expr, QueryError> {
    match template {
        PreparedSqlGroupedExprTemplate::Static(expr) => Ok(expr.clone()),
        PreparedSqlGroupedExprTemplate::SlotLiteral(index) => {
            Ok(crate::db::query::plan::expr::Expr::Literal(
                bindings
                    .get(*index)
                    .ok_or_else(|| {
                        QueryError::unsupported_query(format!(
                            "missing prepared SQL binding at index={index}",
                        ))
                    })?
                    .clone(),
            ))
        }
        PreparedSqlGroupedExprTemplate::Unary { op, expr } => {
            Ok(crate::db::query::plan::expr::Expr::Unary {
                op: *op,
                expr: Box::new(instantiate_symbolic_grouped_expr_template(expr, bindings)?),
            })
        }
        PreparedSqlGroupedExprTemplate::Binary { op, left, right } => {
            Ok(crate::db::query::plan::expr::Expr::Binary {
                op: *op,
                left: Box::new(instantiate_symbolic_grouped_expr_template(left, bindings)?),
                right: Box::new(instantiate_symbolic_grouped_expr_template(right, bindings)?),
            })
        }
    }
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
