#![allow(dead_code)]

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        access::AccessPlan,
        executor::{
            EntityAuthority, SharedPreparedExecutionPlan,
            pipeline::execute_initial_grouped_rows_for_canister,
        },
        predicate::PreparedSqlScalarPredicateTemplate,
        query::plan::expr::PreparedSqlGroupedExprTemplate,
        query::plan::{AccessPlannedQuery, LogicalPlan},
        session::sql::{
            SqlProjectionContract,
            projection::{
                SqlProjectionPayload, execute_sql_projection_rows_for_canister,
                grouped_sql_statement_result_from_page,
            },
        },
        sql::lowering::{
            LoweredSqlQuery, PreparedSqlParameterContract, PreparedSqlParameterTypeFamily,
            PreparedSqlStatement, lower_sql_command_from_prepared_statement,
            prepared_sql_simple_range_slots, sql_expr_is_compound_boolean_shape,
            sql_expr_prepared_predicate_template_shape, sql_statement_contains_any_literal,
        },
        sql::parser::{SqlExpr, SqlStatement},
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
/// `0.103` keeps that boundary explicit: prepared templates are only for
/// predicate/access-owned parameter shapes. General expression-owned `WHERE`
/// semantics must stay on the normal bound-SQL fallback path instead of
/// growing template-local `filter_expr` ownership.
///

#[derive(Clone, Debug)]
enum PreparedSqlExecutionTemplate {
    SymbolicScalar(PreparedSqlSymbolicScalarTemplate),
    SymbolicGrouped(PreparedSqlSymbolicGroupedTemplate),
    Legacy(PreparedSqlLegacyExecutionTemplate),
}

impl PreparedSqlExecutionTemplate {
    // Bind one prepared execution template back onto a concrete planner-owned
    // plan while keeping the variant-specific rebinding policy inside the
    // template lane instead of duplicating that match at each call site.
    fn bind_plan(
        &self,
        contracts: &[PreparedSqlParameterContract],
        bindings: &[Value],
    ) -> Result<BoundPreparedTemplatePlan, QueryError> {
        match self {
            Self::SymbolicScalar(template) => bind_symbolic_scalar_template_plan(
                template.plan.clone(),
                template.predicate.as_ref(),
                template.access.as_ref(),
                bindings,
                template.authority,
            ),
            Self::SymbolicGrouped(template) => bind_symbolic_grouped_template_plan(
                template.plan.clone(),
                template.predicate.as_ref(),
                template.access.as_ref(),
                template.having_expr.as_ref(),
                bindings,
                template.authority,
            ),
            Self::Legacy(template) => bind_prepared_template_plan(
                template.plan.clone(),
                contracts,
                bindings,
                template.authority,
            ),
        }
    }

    // Return the frozen projection contract paired with this template plan so
    // execute-time callers do not need to reopen the enum shape themselves.
    const fn projection(&self) -> &SqlProjectionContract {
        match self {
            Self::SymbolicScalar(template) => &template.projection,
            Self::SymbolicGrouped(template) => &template.projection,
            Self::Legacy(template) => &template.projection,
        }
    }
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn statement_for_test(&self) -> &PreparedSqlStatement {
        &self.statement
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
            let bound_plan = template.bind_plan(prepared.parameter_contracts(), bindings)?;
            let prepared_plan =
                SharedPreparedExecutionPlan::from_plan(bound_plan.authority(), bound_plan.plan);

            return self.execute_prepared_template_plan(prepared_plan, template.projection());
        }

        let bound_statement = prepared.statement.bind_literals(bindings)?;
        let authority = EntityAuthority::for_type::<E>();

        self.execute_bound_prepared_sql_query_without_caches(&bound_statement, authority)
    }

    // Build one internal fixed-route prepared SQL execution shell when every
    // parameter slot exposes a collision-resistant non-null template binding.
    // This boundary stays intentionally narrower than general SQL admission:
    // prepared templates only own predicate/access-shaped parameters, while
    // general expression-owned `WHERE` semantics stay on prepared fallback.
    fn build_prepared_sql_execution_template(
        &self,
        prepared: &PreparedSqlStatement,
        parameter_contracts: &[PreparedSqlParameterContract],
        authority: EntityAuthority,
    ) -> Result<Option<PreparedSqlExecutionTemplate>, QueryError> {
        if parameter_contracts.is_empty() {
            return Ok(None);
        }
        if prepared.uses_general_template_expr_parameters() {
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
        if sql_statement_contains_any_literal(prepared.statement(), template_bindings.as_slice()) {
            return Ok(None);
        }
        let bound_statement = prepared.bind_literals(template_bindings.as_slice())?;
        let Some(plan) = self.build_access_planned_query_from_bound_prepared_statement(
            &bound_statement,
            authority,
        )?
        else {
            return Ok(None);
        };
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
        let mut exemplar_bindings = exemplar_bindings;
        apply_sql_range_exemplar_override(
            select.predicate.as_ref(),
            parameter_contracts,
            exemplar_bindings.as_mut_slice(),
            authority.model(),
        );
        let bound_statement = prepared.bind_literals(exemplar_bindings.as_slice())?;
        let Some(plan) = self.build_access_planned_query_from_bound_prepared_statement(
            &bound_statement,
            authority,
        )?
        else {
            return Ok(None);
        };

        // Phase 3: freeze the symbolic predicate and, when needed, one
        // symbolic access payload only when the lowered scalar plan still
        // matches the admitted compare-family shape exactly.
        let LogicalPlan::Scalar(scalar) = &plan.logical else {
            return Ok(None);
        };

        let predicate_template = match scalar.predicate.as_ref() {
            Some(predicate) => {
                let Some(predicate_shape) =
                    sql_expr_prepared_predicate_template_shape(sql_predicate)
                else {
                    return Ok(None);
                };
                let Some(predicate_template) = predicate.build_prepared_template(predicate_shape)
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
                predicate.contains_any_runtime_values(symbolic_value_candidates.as_slice())
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
            && sql_statement_contains_any_literal(
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
        if sql_statement_contains_any_literal(prepared.statement(), exemplar_bindings.as_slice()) {
            return Ok(None);
        }
        let bound_statement = prepared.bind_literals(exemplar_bindings.as_slice())?;
        let Some(plan) = self.build_access_planned_query_from_bound_prepared_statement(
            &bound_statement,
            authority,
        )?
        else {
            return Ok(None);
        };

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
                let Some(predicate_shape) =
                    sql_expr_prepared_predicate_template_shape(sql_predicate)
                else {
                    return Ok(None);
                };
                let Some(predicate_template) = predicate.build_prepared_template(predicate_shape)
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
                .is_some_and(sql_expr_is_compound_boolean_shape)
            && !(predicate_template.is_none()
                && prepared_sql_simple_range_slots(
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
                let Some(having_template) = having_expr.build_prepared_grouped_template(&|value| {
                    prepared_sql_slot_index_for_exemplar_value(
                        value,
                        parameter_contracts,
                        exemplar_bindings.as_slice(),
                    )
                }) else {
                    return Ok(None);
                };

                Some(having_template)
            }
            None => None,
        };
        if access_template.is_some()
            && sql_statement_contains_any_literal(
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

    // Execute one prepared fallback query directly from its bound SQL shape
    // without routing through either the raw SQL compiled-command cache or the
    // shared structural query-plan cache. Prepared fallback must rebind full
    // expression semantics per execution, so cache-key aliasing on source SQL
    // text or structural shape would be unsound here.
    fn execute_bound_prepared_sql_query_without_caches(
        &self,
        bound_statement: &SqlStatement,
        authority: EntityAuthority,
    ) -> Result<crate::db::session::sql::SqlStatementResult, QueryError> {
        let Some(plan) = self
            .build_access_planned_query_from_bound_prepared_statement(bound_statement, authority)?
        else {
            return Err(QueryError::invariant(
                "prepared SQL query fallback must lower to lowered SQL SELECT",
            ));
        };
        let prepared_plan = SharedPreparedExecutionPlan::from_plan(authority, plan);
        let projection = Self::sql_select_projection_contract_from_shared_prepared_plan(
            authority,
            &prepared_plan,
        );

        self.execute_prepared_template_plan(prepared_plan, &projection)
    }

    // Build one access-planned query from one already-bound prepared SQL
    // statement through the normal prepared lowering and structural planning
    // path. Legacy template compilation, symbolic exemplar compilation, and
    // prepared fallback all use the same owner path here instead of repeating
    // the normalize -> lower -> select -> structural -> plan ladder locally.
    fn build_access_planned_query_from_bound_prepared_statement(
        &self,
        bound_statement: &SqlStatement,
        authority: EntityAuthority,
    ) -> Result<Option<AccessPlannedQuery>, QueryError> {
        let normalized_prepared =
            Self::prepare_sql_statement_for_authority(bound_statement, authority)?;
        let lowered =
            lower_sql_command_from_prepared_statement(normalized_prepared, authority.model())
                .map_err(QueryError::from_sql_lowering_error)?;
        let Some(LoweredSqlQuery::Select(select)) = lowered.into_query() else {
            return Ok(None);
        };
        let structural = Self::structural_query_from_lowered_select(select, authority)?;
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(structural, authority)?;

        Ok(Some(plan))
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
            scalar.predicate = scalar
                .predicate
                .take()
                .map(|predicate| predicate.bind_template_values(replacements.as_slice()));
        }
        LogicalPlan::Grouped(grouped) => {
            grouped.scalar.predicate = grouped
                .scalar
                .predicate
                .take()
                .map(|predicate| predicate.bind_template_values(replacements.as_slice()));
            grouped.having_expr = grouped
                .having_expr
                .take()
                .map(|expr| expr.bind_template_values(replacements.as_slice()));
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
    let bound_components = instantiate_symbolic_scalar_template_components(
        predicate_template,
        access_template,
        bindings,
    )?;
    if let Some(predicate) = bound_components.predicate {
        scalar.predicate = Some(predicate);
        scalar.filter_expr = None;
    }
    if let Some(access) = bound_components.access {
        plan.access = access;
    }

    finalize_bound_symbolic_template_plan(plan, authority)
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
    let bound_components = instantiate_symbolic_scalar_template_components(
        predicate_template,
        access_template,
        bindings,
    )?;
    if let Some(predicate) = bound_components.predicate {
        grouped.scalar.predicate = Some(predicate);
        grouped.scalar.filter_expr = None;
    }
    if let Some(access) = bound_components.access {
        plan.access = access;
    }

    // Phase 2: rebuild the slot-owned grouped HAVING expression when present.
    if let Some(having_template) = having_template {
        let having_expr = having_template.instantiate(bindings)?;
        grouped.having_expr = Some(having_expr);
    }

    finalize_bound_symbolic_template_plan(plan, authority)
}

///
/// BoundPreparedSqlSymbolicScalarComponents
///
/// Shared instantiated symbolic scalar template payload before it is applied
/// back onto either a scalar or grouped prepared plan. This keeps slot-owned
/// predicate and access rebinding on one shared boundary instead of rebuilding
/// the same pair separately in both bind paths.
///

struct BoundPreparedSqlSymbolicScalarComponents {
    predicate: Option<crate::db::predicate::Predicate>,
    access: Option<AccessPlan<Value>>,
}

// Instantiate the shared scalar symbolic template components owned by both
// scalar and grouped prepared bind paths.
fn instantiate_symbolic_scalar_template_components(
    predicate_template: Option<&PreparedSqlScalarPredicateTemplate>,
    access_template: Option<&PreparedSqlScalarAccessPathTemplate>,
    bindings: &[Value],
) -> Result<BoundPreparedSqlSymbolicScalarComponents, QueryError> {
    let predicate = predicate_template
        .map(|template| template.instantiate(bindings))
        .transpose()?;
    let access = access_template
        .map(|template| instantiate_symbolic_scalar_access_path_template(template, bindings))
        .transpose()?;

    Ok(BoundPreparedSqlSymbolicScalarComponents { predicate, access })
}

// Finalize one rebound symbolic prepared plan through the same planner-frozen
// route/profile refresh used by both scalar and grouped bind paths.
fn finalize_bound_symbolic_template_plan(
    mut plan: AccessPlannedQuery,
    authority: EntityAuthority,
) -> Result<BoundPreparedTemplatePlan, QueryError> {
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
        prepared_sql_simple_range_slots(predicate, model, contracts)
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::plan::expr::Expr;

    #[test]
    fn grouped_expr_template_keeps_static_bool_literal_distinct_from_bool_exemplar_slot() {
        let expr = Expr::Literal(Value::Bool(false));
        let parameter_contracts = vec![PreparedSqlParameterContract::new(
            0,
            PreparedSqlParameterTypeFamily::Bool,
            true,
            None,
        )];
        let exemplar_bindings = vec![Value::Bool(false)];

        let template = expr
            .build_prepared_grouped_template(&|value| {
                prepared_sql_slot_index_for_exemplar_value(
                    value,
                    &parameter_contracts,
                    &exemplar_bindings,
                )
            })
            .expect("grouped literal template should build");

        assert!(
            matches!(
                template,
                PreparedSqlGroupedExprTemplate::Static(Expr::Literal(Value::Bool(false)))
            ),
            "grouped static FALSE should stay static instead of rebinding through one unrelated bool exemplar slot",
        );
    }
}
