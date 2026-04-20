#![allow(dead_code)]

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        access::AccessPlan,
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
    model::{entity::EntityModel, field::FieldKind},
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
/// Internal prepared SQL execution variants for supported fixed-route
/// parameter families.
/// `0.99` starts introducing symbolic slot ownership here without widening the
/// planner-wide IR, so legacy sentinel-backed templates and new symbolic
/// templates can coexist while the lane is migrated in bounded slices.
///

#[derive(Clone, Debug)]
enum PreparedSqlExecutionTemplate {
    SymbolicScalar(PreparedSqlSymbolicScalarTemplate),
    SymbolicGrouped(PreparedSqlSymbolicGroupedTemplate),
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
/// PreparedSqlSymbolicGroupedTemplate
///
/// Symbolic grouped prepared SQL template for the grouped `0.99` lane.
/// This template owns grouped scalar `WHERE`, grouped access payloads, and
/// optional `HAVING` rebinding on the same frozen grouped route.
///

#[derive(Clone, Debug)]
struct PreparedSqlSymbolicGroupedTemplate {
    authority: EntityAuthority,
    projection: SqlProjectionContract,
    plan: AccessPlannedQuery,
    predicate: Option<PreparedSqlScalarPredicateTemplate>,
    access: Option<PreparedSqlScalarAccessPathTemplate>,
    having_expr: Option<PreparedSqlGroupedExprTemplate>,
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
    ByKey {
        key: PreparedSqlScalarAccessValueTemplate,
    },
    KeyRange {
        start: PreparedSqlScalarAccessValueTemplate,
        end: PreparedSqlScalarAccessValueTemplate,
    },
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
    SymbolicGrouped,
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
            Some(PreparedSqlExecutionTemplate::SymbolicGrouped(_)) => {
                Some(PreparedSqlExecutionTemplateKind::SymbolicGrouped)
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
                PreparedSqlExecutionTemplate::SymbolicGrouped(template) => {
                    bind_symbolic_grouped_template_plan(
                        template.plan.clone(),
                        template.predicate.as_ref(),
                        template.access.as_ref(),
                        template.having_expr.as_ref(),
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
                PreparedSqlExecutionTemplate::SymbolicGrouped(template) => &template.projection,
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
        if let Some(template) = self.build_symbolic_grouped_prepared_sql_execution_template(
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
    #[expect(clippy::too_many_lines)]
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
        let mut exemplar_bindings = exemplar_bindings;
        apply_sql_range_exemplar_override(
            select.predicate.as_ref(),
            parameter_contracts,
            exemplar_bindings.as_mut_slice(),
            authority.model(),
        );
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
        let symbolic_value_candidates = parameter_contracts
            .iter()
            .zip(exemplar_bindings.iter())
            .filter(|(contract, _)| contract.type_family() != PreparedSqlParameterTypeFamily::Bool)
            .map(|(_, binding)| binding.clone())
            .collect::<Vec<_>>();
        if predicate_template.is_none()
            && scalar.predicate.as_ref().is_some_and(|predicate| {
                predicate_contains_any_runtime_values(
                    predicate,
                    symbolic_value_candidates.as_slice(),
                )
            })
        {
            return Ok(None);
        }
        let access_template = build_symbolic_scalar_access_path_template(
            &plan.access,
            parameter_contracts,
            exemplar_bindings.as_slice(),
        );
        if access_template.is_none()
            && !symbolic_value_candidates.is_empty()
            && plan
                .access
                .contains_any_runtime_values(symbolic_value_candidates.as_slice())
        {
            return Ok(None);
        }
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
    // `0.99`: admitted grouped scalar predicates, grouped access payloads, and
    // optional compare-family HAVING, with grouped routing and access
    // ownership still frozen through one exemplar plan.
    #[expect(clippy::too_many_lines)]
    fn build_symbolic_grouped_prepared_sql_execution_template(
        &self,
        prepared: &PreparedSqlStatement,
        parameter_contracts: &[PreparedSqlParameterContract],
        authority: EntityAuthority,
    ) -> Result<Option<PreparedSqlExecutionTemplate>, QueryError> {
        if parameter_contracts.is_empty() {
            return Ok(None);
        }

        let SqlStatement::Select(statement) = prepared.statement() else {
            return Ok(None);
        };

        // Phase 1: compile one exemplar grouped plan so route/projection stay
        // planner-owned while post-aggregate slot binding becomes symbolic.
        let Some(exemplar_bindings) = parameter_contracts
            .iter()
            .map(prepared_sql_contract_exemplar_binding)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let mut exemplar_bindings = exemplar_bindings;
        apply_sql_range_exemplar_override(
            statement.predicate.as_ref(),
            parameter_contracts,
            exemplar_bindings.as_mut_slice(),
            authority.model(),
        );
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

        // Phase 2: freeze the symbolic grouped template only when any grouped
        // scalar predicate or grouped post-aggregate expression still matches
        // the admitted symbolic slice exactly.
        let LogicalPlan::Grouped(grouped) = &plan.logical else {
            return Ok(None);
        };
        let symbolic_access_candidates = parameter_contracts
            .iter()
            .zip(exemplar_bindings.iter())
            .filter(|(contract, _)| contract.type_family() != PreparedSqlParameterTypeFamily::Bool)
            .map(|(_, binding)| binding.clone())
            .collect::<Vec<_>>();
        let access_template = build_symbolic_scalar_access_path_template(
            &plan.access,
            parameter_contracts,
            exemplar_bindings.as_slice(),
        );
        let predicate_template = match (
            statement.predicate.as_ref(),
            grouped.scalar.predicate.as_ref(),
        ) {
            (Some(sql_predicate), Some(predicate)) => {
                let Some(predicate_template) =
                    build_symbolic_scalar_predicate_template(sql_predicate, predicate)
                else {
                    return Ok(None);
                };

                Some(predicate_template)
            }
            // Grouped routes may push the whole admitted compare-family WHERE
            // into one symbolic access payload and leave no residual grouped
            // scalar predicate behind. That shape is still safe to keep on the
            // grouped symbolic lane as long as access rebinding owns the slot.
            (Some(_), None) if access_template.is_some() => None,
            (None, None) => None,
            _ => return Ok(None),
        };
        // Grouped `0.99` access ownership is still intentionally narrower than
        // the scalar lane. Exact/prefix grouped access can stay symbolic on
        // its own, grouped scalar predicates can stay symbolic on their own,
        // and simple same-field grouped key ranges can stay symbolic when the
        // whole WHERE predicate is already owned by the access payload.
        // Compound grouped `WHERE` shapes that mix symbolic access with extra
        // residual predicate work still fail closed for now.
        if access_template.is_some()
            && statement
                .predicate
                .as_ref()
                .is_some_and(sql_expr_is_compound_boolean)
            && !(predicate_template.is_none()
                && sql_simple_range_slots(
                    statement.predicate.as_ref(),
                    authority.model(),
                    parameter_contracts,
                )
                .is_some())
        {
            return Ok(None);
        }
        if access_template.is_none()
            && !symbolic_access_candidates.is_empty()
            && plan
                .access
                .contains_any_runtime_values(symbolic_access_candidates.as_slice())
        {
            return Ok(None);
        }
        let having_template = match grouped.having_expr.as_ref() {
            Some(having_expr) => {
                let Some(having_template) = build_symbolic_grouped_expr_template(
                    having_expr,
                    parameter_contracts,
                    exemplar_bindings.as_slice(),
                ) else {
                    return Ok(None);
                };

                Some(having_template)
            }
            None => None,
        };
        if access_template.is_some()
            && prepared_statement_contains_template_literal_collision(
                prepared.statement(),
                exemplar_bindings.as_slice(),
            )
        {
            return Ok(None);
        }
        if predicate_template.is_none() && access_template.is_none() && having_template.is_none() {
            return Ok(None);
        }

        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority, plan.clone());
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        Ok(Some(PreparedSqlExecutionTemplate::SymbolicGrouped(
            PreparedSqlSymbolicGroupedTemplate {
                authority,
                projection,
                plan,
                predicate: predicate_template,
                access: access_template,
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

// Return whether one parsed SQL boolean expression is compound enough that the
// grouped symbolic access lane should still fail closed instead of claiming a
// combined access+residual-predicate route in `0.99`.
const fn sql_expr_is_compound_boolean(expr: &SqlExpr) -> bool {
    matches!(
        expr,
        SqlExpr::Binary {
            op: crate::db::sql::parser::SqlExprBinaryOp::And
                | crate::db::sql::parser::SqlExprBinaryOp::Or,
            ..
        } | SqlExpr::Unary { .. }
    )
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

// Instantiate one symbolic grouped prepared template back into the normal
// planner-owned grouped plan without relying on sentinel replacement.
fn bind_symbolic_grouped_template_plan(
    mut plan: AccessPlannedQuery,
    predicate_template: Option<&PreparedSqlScalarPredicateTemplate>,
    access_template: Option<&PreparedSqlScalarAccessPathTemplate>,
    having_template: Option<&PreparedSqlGroupedExprTemplate>,
    bindings: &[Value],
    authority: EntityAuthority,
) -> Result<BoundPreparedTemplatePlan, QueryError> {
    // Phase 1: rebuild the slot-owned grouped scalar predicate when present.
    let LogicalPlan::Grouped(grouped) = &mut plan.logical else {
        return Err(QueryError::unsupported_query(
            "symbolic grouped prepared template expected grouped logical plan",
        ));
    };
    if let Some(predicate_template) = predicate_template {
        grouped.scalar.predicate = Some(instantiate_symbolic_scalar_predicate_template(
            predicate_template,
            bindings,
        )?);
    }
    if let Some(access_template) = access_template {
        plan.access = instantiate_symbolic_scalar_access_path_template(access_template, bindings)?;
    }

    // Phase 2: rebuild the slot-owned grouped HAVING expression when present.
    if let Some(having_template) = having_template {
        let having_expr = instantiate_symbolic_grouped_expr_template(having_template, bindings)?;
        grouped.having_expr = Some(having_expr);
    }

    // Phase 3: rebuild planner-frozen executor residents from the rebound plan.
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

// Rewrite one prepared exemplar binding pair when the parsed SQL predicate is a
// simple same-field lower/upper range. Numeric template sentinels descend by
// slot index, so range pairs would otherwise compile one empty exemplar route
// and never recover at execute time.
fn apply_sql_range_exemplar_override(
    predicate: Option<&SqlExpr>,
    contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &mut [Value],
    model: &'static EntityModel,
) {
    let Some((field_kind, lower_slot, upper_slot)) =
        sql_simple_range_slots(predicate, model, contracts)
    else {
        return;
    };
    let Some((lower_value, upper_value)) =
        ordered_exemplar_range_values_for_field_kind(field_kind, lower_slot, upper_slot)
    else {
        return;
    };
    let Some(lower_binding) = exemplar_bindings.get_mut(lower_slot) else {
        return;
    };
    *lower_binding = lower_value;
    let Some(upper_binding) = exemplar_bindings.get_mut(upper_slot) else {
        return;
    };
    *upper_binding = upper_value;
}

// Return one simple same-field lower/upper range pair when the parsed SQL
// predicate is exactly `field >= ? AND field < ?` (or the analogous strict/
// inclusive variants) over one admitted prepared compare-family field.
fn sql_simple_range_slots(
    predicate: Option<&SqlExpr>,
    model: &'static EntityModel,
    contracts: &[PreparedSqlParameterContract],
) -> Option<(FieldKind, usize, usize)> {
    let SqlExpr::Binary {
        op: crate::db::sql::parser::SqlExprBinaryOp::And,
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
        .map(crate::model::field::FieldModel::kind)?;

    Some((field_kind, lower_slot, upper_slot))
}

enum SqlRangeBoundKind {
    Lower,
    Upper,
}

struct SqlRangeCompareDescriptor<'a> {
    field: &'a str,
    slot_index: usize,
    bound: SqlRangeBoundKind,
}

fn sql_range_compare_descriptor(expr: &SqlExpr) -> Option<SqlRangeCompareDescriptor<'_>> {
    let SqlExpr::Binary { op, left, right } = expr else {
        return None;
    };
    let (SqlExpr::Field(field), SqlExpr::Param { index }) = (&**left, &**right) else {
        return None;
    };
    let bound = match op {
        crate::db::sql::parser::SqlExprBinaryOp::Gt
        | crate::db::sql::parser::SqlExprBinaryOp::Gte => SqlRangeBoundKind::Lower,
        crate::db::sql::parser::SqlExprBinaryOp::Lt
        | crate::db::sql::parser::SqlExprBinaryOp::Lte => SqlRangeBoundKind::Upper,
        _ => return None,
    };

    Some(SqlRangeCompareDescriptor {
        field,
        slot_index: *index,
        bound,
    })
}

fn ordered_exemplar_range_values_for_field_kind(
    field_kind: FieldKind,
    lower_slot: usize,
    upper_slot: usize,
) -> Option<(Value, Value)> {
    let lower_offset = u64::try_from(lower_slot).ok()?;
    let upper_offset = u64::try_from(upper_slot).ok()?;

    match field_kind {
        FieldKind::Int => {
            let lower = i64::try_from(lower_offset).ok()?;
            let upper = i64::try_from(upper_offset).ok()?.saturating_add(1);

            Some((Value::Int(lower), Value::Int(upper)))
        }
        FieldKind::Int128 => Some((
            Value::Int128((i128::from(lower_offset)).into()),
            Value::Int128((i128::from(upper_offset).saturating_add(1)).into()),
        )),
        FieldKind::IntBig => Some((
            Value::IntBig(crate::types::Int::from(i32::try_from(lower_offset).ok()?)),
            Value::IntBig(crate::types::Int::from(
                i32::try_from(upper_offset.saturating_add(1)).ok()?,
            )),
        )),
        FieldKind::Uint => Some((
            Value::Uint(lower_offset),
            Value::Uint(upper_offset.saturating_add(1)),
        )),
        FieldKind::Uint128 => Some((
            Value::Uint128((u128::from(lower_offset)).into()),
            Value::Uint128((u128::from(upper_offset).saturating_add(1)).into()),
        )),
        FieldKind::UintBig => Some((
            Value::UintBig(lower_offset.into()),
            Value::UintBig(upper_offset.saturating_add(1).into()),
        )),
        FieldKind::Decimal { scale } => Some((
            Value::Decimal(crate::types::Decimal::from_i128_with_scale(
                i128::from(lower_offset),
                scale,
            )),
            Value::Decimal(crate::types::Decimal::from_i128_with_scale(
                i128::from(upper_offset).saturating_add(1),
                scale,
            )),
        )),
        FieldKind::Duration => Some((
            Value::Duration(crate::types::Duration::from_millis(lower_offset)),
            Value::Duration(crate::types::Duration::from_millis(
                upper_offset.saturating_add(1),
            )),
        )),
        FieldKind::Timestamp => Some((
            Value::Timestamp(crate::types::Timestamp::from_millis(
                i64::try_from(lower_offset).ok()?,
            )),
            Value::Timestamp(crate::types::Timestamp::from_millis(
                i64::try_from(upper_offset).ok()?.saturating_add(1),
            )),
        )),
        FieldKind::Text => Some((
            Value::Text(format!("__icydb_prepared_range_lower_{lower_slot}__")),
            Value::Text(format!("__icydb_prepared_range_upper_{upper_slot}__")),
        )),
        _ => None,
    }
}

// Build one symbolic scalar access payload template when the selected access
// path carries slot-owned values and the current `0.99` slice knows how to
// rebuild that payload without sentinel replacement.
fn build_symbolic_scalar_access_path_template(
    access: &AccessPlan<Value>,
    parameter_contracts: &[PreparedSqlParameterContract],
    exemplar_bindings: &[Value],
) -> Option<PreparedSqlScalarAccessPathTemplate> {
    if let Some(key) = access.as_by_key_path() {
        return Some(PreparedSqlScalarAccessPathTemplate::ByKey {
            key: build_symbolic_scalar_access_value_template(
                key,
                parameter_contracts,
                exemplar_bindings,
            ),
        });
    }
    if let Some((start, end)) = access.as_primary_key_range_path() {
        let start = build_symbolic_scalar_access_value_template(
            start,
            parameter_contracts,
            exemplar_bindings,
        );
        let end = build_symbolic_scalar_access_value_template(
            end,
            parameter_contracts,
            exemplar_bindings,
        );
        if matches!(start, PreparedSqlScalarAccessValueTemplate::Static(_))
            && matches!(end, PreparedSqlScalarAccessValueTemplate::Static(_))
        {
            return None;
        }

        return Some(PreparedSqlScalarAccessPathTemplate::KeyRange { start, end });
    }
    let (index, values) = access.as_index_prefix_path()?;
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
        return None;
    }

    Some(PreparedSqlScalarAccessPathTemplate::IndexPrefix {
        index: *index,
        values: templates,
    })
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
        PreparedSqlScalarAccessPathTemplate::ByKey { key } => Ok(AccessPlan::by_key(
            instantiate_symbolic_scalar_access_value_template(key, bindings)?,
        )),
        PreparedSqlScalarAccessPathTemplate::KeyRange { start, end } => Ok(AccessPlan::key_range(
            instantiate_symbolic_scalar_access_value_template(start, bindings)?,
            instantiate_symbolic_scalar_access_value_template(end, bindings)?,
        )),
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

// Return whether one lowered scalar predicate still carries any exemplar
// runtime literal that the symbolic scalar template did not actually lift into
// slot ownership.
fn predicate_contains_any_runtime_values(
    predicate: &crate::db::predicate::Predicate,
    candidates: &[Value],
) -> bool {
    match predicate {
        crate::db::predicate::Predicate::True
        | crate::db::predicate::Predicate::False
        | crate::db::predicate::Predicate::CompareFields(_)
        | crate::db::predicate::Predicate::IsNull { .. }
        | crate::db::predicate::Predicate::IsNotNull { .. }
        | crate::db::predicate::Predicate::IsMissing { .. }
        | crate::db::predicate::Predicate::IsEmpty { .. }
        | crate::db::predicate::Predicate::IsNotEmpty { .. } => false,
        crate::db::predicate::Predicate::Compare(compare) => candidates.contains(compare.value()),
        crate::db::predicate::Predicate::And(children)
        | crate::db::predicate::Predicate::Or(children) => children
            .iter()
            .any(|child| predicate_contains_any_runtime_values(child, candidates)),
        crate::db::predicate::Predicate::Not(child) => {
            predicate_contains_any_runtime_values(child, candidates)
        }
        crate::db::predicate::Predicate::TextContains { value, .. }
        | crate::db::predicate::Predicate::TextContainsCi { value, .. } => {
            candidates.contains(value)
        }
    }
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
        ) if children.len() == 2 => build_symbolic_scalar_binary_children_template(
            left,
            right,
            &children[0],
            &children[1],
            PreparedSqlScalarPredicateTemplate::And,
        ),
        (
            SqlExpr::Binary {
                op: crate::db::sql::parser::SqlExprBinaryOp::Or,
                left,
                right,
            },
            crate::db::predicate::Predicate::Or(children),
        ) if children.len() == 2 => build_symbolic_scalar_binary_children_template(
            left,
            right,
            &children[0],
            &children[1],
            PreparedSqlScalarPredicateTemplate::Or,
        ),
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

// Pair one binary SQL predicate subtree with two lowered predicate children.
// Lowering may reorder compare-family `AND`/`OR` leaves during extraction, so
// symbolic template ownership must accept either stable child ordering as long
// as the rebuilt predicate semantics are unchanged.
fn build_symbolic_scalar_binary_children_template(
    left_sql: &SqlExpr,
    right_sql: &SqlExpr,
    first_child: &crate::db::predicate::Predicate,
    second_child: &crate::db::predicate::Predicate,
    ctor: fn(Vec<PreparedSqlScalarPredicateTemplate>) -> PreparedSqlScalarPredicateTemplate,
) -> Option<PreparedSqlScalarPredicateTemplate> {
    if let (Some(left), Some(right)) = (
        build_symbolic_scalar_predicate_template(left_sql, first_child),
        build_symbolic_scalar_predicate_template(right_sql, second_child),
    ) {
        return Some(ctor(vec![left, right]));
    }

    let (Some(left), Some(right)) = (
        build_symbolic_scalar_predicate_template(left_sql, second_child),
        build_symbolic_scalar_predicate_template(right_sql, first_child),
    ) else {
        return None;
    };

    Some(ctor(vec![left, right]))
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
