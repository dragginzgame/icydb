//! Module: sql::lowering::select
//! Responsibility: lower SQL SELECT-like query tails into structural query contracts.
//! Does not own: SQL parsing, executor planning, or runtime execution.
//! Boundary: binds lowered SQL clauses to query intent using schema-owned validation.

mod aggregate;
mod binding;
mod order;
mod projection;

use crate::db::sql::lowering::{
    LoweredExprAnalysis, LoweredExprSourceRef, SqlLoweringError,
    aggregate::{SqlAggregateCallInterner, lower_grouped_aggregate_call},
    predicate::{
        derive_sql_where_expr_predicate_subset, lower_sql_scalar_where_bool_expr,
        lower_sql_where_bool_expr, lower_sql_where_expr,
    },
};
#[cfg(test)]
use crate::{db::query::intent::Query, entity::EntityKind};
use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate},
        query::{
            builder::AggregateExpr,
            intent::{QueryError, StructuralQuery},
            plan::expr::{Expr, FieldPath, ProjectionSelection},
        },
        schema::SchemaInfo,
        sql::parser::{
            SqlDeleteStatement, SqlExpr, SqlOrderDirection, SqlOrderTerm, SqlReturningProjection,
            SqlSelectStatement, SqlUpdateStatement,
        },
    },
    model::entity::EntityModel,
};
use icydb_diagnostic_code::{SqlFeatureCode, SqlWriteBoundaryCode};

use crate::db::sql::lowering::select::{
    aggregate::lower_having_clauses,
    order::{LoweredSqlOrderTerm, apply_order_terms_structural},
    projection::{
        LoweredSqlProjectionSelection, lower_grouped_projection, lower_scalar_projection_selection,
        validate_distinct_order_terms_against_projection,
    },
};

pub(in crate::db::sql::lowering) use aggregate::lower_global_aggregate_having_expr;
use binding::{canonicalize_sql_filter_expr_for_schema, canonicalize_sql_predicate_for_schema};
pub(in crate::db::sql::lowering) use projection::lower_analyzed_select_item_expr;

pub(in crate::db::sql::lowering) fn lower_order_terms(
    order_by: Vec<crate::db::sql::parser::SqlOrderTerm>,
) -> Result<Vec<LoweredSqlOrderTerm>, SqlLoweringError> {
    order::lower_order_terms(order_by)
}

///
/// LoweredSqlFilter
///
/// SQL-lowered filter wrapper that keeps SQL's visible expression and any
/// required predicate subset together until the final `StructuralQuery`
/// handoff. Ordinary SELECT filters keep expression authority and let query
/// intent own the shared pre-access filter contract after schema binding; when
/// the schema-bound expression is extractable, SQL lowering also passes a
/// canonicalized predicate mirror so numeric SQL literals retain strict
/// indexable semantics.
///
#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) struct LoweredSqlFilter {
    visible_expr: Option<Expr>,
    predicate_subset: Option<Predicate>,
}

impl LoweredSqlFilter {
    // Lower one scalar WHERE expression into the visible-expression contract
    // used by ordinary scalar SELECTs. Query intent derives the shared
    // predicate subset after schema binding.
    fn from_scalar_where_expr(expr: &SqlExpr) -> Result<Self, SqlLoweringError> {
        let filter_expr = lower_sql_scalar_where_bool_expr(expr)?;
        Ok(Self::from_visible_expr(filter_expr))
    }

    // Lower one grouped WHERE expression without scalar-only CASE
    // canonicalization, preserving the existing grouped/base-query behavior.
    fn from_grouped_where_expr(expr: &SqlExpr) -> Result<Self, SqlLoweringError> {
        let filter_expr = lower_sql_where_bool_expr(expr)?;
        Ok(Self::from_visible_expr(filter_expr))
    }

    // Lower one WHERE expression for lanes that must prove a predicate subset
    // before execution can use the shape.
    pub(in crate::db::sql::lowering) fn from_where_expr_requiring_predicate_subset(
        expr: &SqlExpr,
    ) -> Result<Self, SqlLoweringError> {
        let filter_expr = lower_sql_where_bool_expr(expr)?;
        let predicate_subset = derive_sql_where_expr_predicate_subset(&filter_expr)
            .ok_or_else(SqlLoweringError::unsupported_where_expression)?;

        Ok(Self::from_visible_expr_and_predicate_subset(
            filter_expr,
            predicate_subset,
        ))
    }

    // Lower DELETE filters through the same expression-backed authority as
    // scalar SELECTs. Query intent derives any supported predicate subset after
    // schema binding and leaves unsupported filters on the residual expression
    // lane.
    fn from_delete_where_expr(expr: &SqlExpr) -> Result<Self, SqlLoweringError> {
        let filter_expr = lower_sql_scalar_where_bool_expr(expr)?;

        Ok(Self::from_visible_expr(filter_expr))
    }

    // Preserve UPDATE's two-part filter policy: keep the scalar-visible filter
    // expression while requiring a strict SQL predicate subset for selector
    // admission.
    fn from_update_where_expr(expr: &SqlExpr) -> Result<Self, SqlLoweringError> {
        let filter_expr = lower_sql_scalar_where_bool_expr(expr)?;
        let predicate_subset = lower_sql_where_expr(expr)?;

        Ok(Self::from_visible_expr_and_predicate_subset(
            filter_expr,
            predicate_subset,
        ))
    }

    const fn from_visible_expr(expr: Expr) -> Self {
        Self {
            visible_expr: Some(expr),
            predicate_subset: None,
        }
    }

    const fn from_visible_expr_and_predicate_subset(
        expr: Expr,
        predicate_subset: Predicate,
    ) -> Self {
        Self {
            visible_expr: Some(expr),
            predicate_subset: Some(predicate_subset),
        }
    }

    #[must_use]
    fn apply_to_query(self, query: StructuralQuery, schema: &SchemaInfo) -> StructuralQuery {
        let visible_expr = self
            .visible_expr
            .map(|expr| canonicalize_sql_filter_expr_for_schema(schema, expr));

        match (visible_expr, self.predicate_subset) {
            (Some(filter_expr), Some(predicate)) => {
                let predicate = canonicalize_sql_predicate_for_schema(schema, predicate);

                query.filter_expr_with_normalized_predicate(filter_expr, predicate)
            }
            (Some(filter_expr), None) => {
                if let Some(predicate) = derive_sql_where_expr_predicate_subset(&filter_expr) {
                    let predicate = canonicalize_sql_predicate_for_schema(schema, predicate);
                    query.filter_expr_with_normalized_predicate(filter_expr, predicate)
                } else {
                    query.filter_expr(filter_expr)
                }
            }
            (None, Some(predicate)) => {
                let predicate = canonicalize_sql_predicate_for_schema(schema, predicate);
                query.filter_normalized_predicate(predicate)
            }
            (None, None) => query,
        }
    }
}

///
/// LoweredSelectShape
///
/// Entity-agnostic lowered SQL SELECT shape prepared for typed `Query<E>`
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredSelectShape {
    projection_selection: LoweredSqlProjectionSelection,
    grouped_aggregates: Vec<AggregateExpr>,
    group_by_fields: Vec<String>,
    distinct: bool,
    having: Vec<crate::db::query::plan::expr::Expr>,
    filter: Option<LoweredSqlFilter>,
    order_by: Vec<LoweredSqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[cfg(test)]
impl LoweredSelectShape {
    /// Borrow grouped key fields in declaration order for lowering tests.
    #[must_use]
    pub(crate) fn group_by_fields_for_test(&self) -> &[String] {
        self.group_by_fields.as_slice()
    }

    /// Render normalized ORDER BY terms back into stable plan labels for tests.
    #[must_use]
    pub(crate) fn order_labels_for_test(&self) -> Vec<String> {
        self.order_by
            .iter()
            .map(|term| {
                crate::db::query::builder::scalar_projection::render_scalar_projection_expr_plan_label(
                    &term.expr,
                )
            })
            .collect()
    }
}

///
/// LoweredBaseQueryShape
///
/// Generic-free filter/order/window query modifiers shared by delete and
/// global-aggregate SQL lowering.
/// This keeps common SQL query-shape lowering shared before typed query
/// binding.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredBaseQueryShape {
    pub(in crate::db::sql::lowering) filter: Option<LoweredSqlFilter>,
    pub(in crate::db::sql::lowering) order_by: Vec<LoweredSqlOrderTerm>,
    pub(in crate::db::sql::lowering) limit: Option<u32>,
    pub(in crate::db::sql::lowering) offset: Option<u32>,
}

///
/// LoweredDeleteShape
///
/// Prepared DELETE execution artifact after predicate, order, limit, and
/// offset lowering.
/// This keeps only the execution-ready query shape plus the SQL write-output
/// `RETURNING` contract, so session caches do not retain the full parsed
/// DELETE statement after lowering.
///
#[derive(Clone, Debug)]
pub(crate) struct LoweredDeleteShape {
    base_query: LoweredBaseQueryShape,
    returning: Option<SqlReturningProjection>,
}

impl LoweredDeleteShape {
    /// Consume this lowered DELETE artifact into its executable base query.
    #[must_use]
    pub(in crate::db) fn into_base_query(self) -> LoweredBaseQueryShape {
        self.base_query
    }

    /// Borrow the SQL write-output projection retained for DELETE RETURNING.
    #[must_use]
    pub(in crate::db) const fn returning(&self) -> Option<&SqlReturningProjection> {
        self.returning.as_ref()
    }
}

#[inline(never)]
#[cfg(test)]
pub(in crate::db::sql::lowering) fn lower_select_shape_for_model_only(
    statement: SqlSelectStatement,
    model: &'static EntityModel,
) -> Result<LoweredSelectShape, SqlLoweringError> {
    lower_select_shape_with_schema(
        statement,
        model,
        SchemaInfo::cached_for_generated_entity_model(model),
    )
}

#[inline(never)]
pub(in crate::db::sql::lowering) fn lower_select_shape_with_schema(
    statement: SqlSelectStatement,
    model: &'static EntityModel,
    schema: &SchemaInfo,
) -> Result<LoweredSelectShape, SqlLoweringError> {
    let SqlSelectStatement {
        projection,
        projection_aliases,
        predicate,
        distinct,
        group_by,
        having,
        order_by,
        limit,
        offset,
        entity: _,
        table_alias: _,
    } = statement;
    let projection_for_having = projection.clone();

    // Phase 1: resolve scalar/grouped projection shape.
    let is_grouped = !group_by.is_empty();
    if !is_grouped && !having.is_empty() {
        return Err(SqlLoweringError::having_requires_group_by());
    }

    let (projection_selection, grouped_aggregates, normalized_distinct) = if is_grouped {
        let grouped_projection = lower_grouped_projection(
            projection,
            projection_aliases.as_slice(),
            group_by.as_slice(),
            model,
        )?;
        let projection_aggregate_count = grouped_projection.aggregate_calls().len();
        let mut grouped_aggregates = grouped_projection.aggregate_calls().to_vec();
        let mut aggregate_call_interner =
            SqlAggregateCallInterner::from_existing(grouped_aggregates.as_slice());
        for expr in having.as_slice() {
            aggregate_call_interner.extend_expr(&mut grouped_aggregates, expr);
        }
        let projection_selection = grouped_projection.into_projection_selection(
            projection_aggregate_count == grouped_aggregates.len(),
            group_by.as_slice(),
        );
        let grouped_aggregates = lower_grouped_aggregate_calls(grouped_aggregates, model, schema)?;
        (projection_selection, grouped_aggregates, false)
    } else {
        let projection_selection =
            lower_scalar_projection_selection(projection, projection_aliases.as_slice())?;
        (projection_selection, Vec::new(), distinct)
    };

    // Phase 1b: keep SQL DISTINCT ordering fail-closed to the projected tuple.
    let order_by = lower_order_terms(order_by)?;
    if normalized_distinct {
        validate_distinct_order_terms_against_projection(
            projection_selection.selection(),
            order_by.as_slice(),
            model,
        )?;
    }

    // Phase 2: resolve HAVING symbols against grouped projection authority.
    let having = lower_having_clauses(
        having,
        &projection_for_having,
        group_by.as_slice(),
        grouped_aggregates.as_slice(),
        model,
        schema,
    )?;

    let filter = match predicate {
        Some(expr) if !is_grouped => Some(LoweredSqlFilter::from_scalar_where_expr(&expr)?),
        Some(expr) => Some(LoweredSqlFilter::from_grouped_where_expr(&expr)?),
        None => None,
    };

    Ok(LoweredSelectShape {
        projection_selection,
        grouped_aggregates,
        group_by_fields: group_by,
        distinct: normalized_distinct,
        having,
        filter,
        order_by,
        limit,
        offset,
    })
}

#[inline(never)]
#[cfg(all(test, feature = "sql-explain"))]
pub(in crate::db) fn apply_lowered_select_shape_for_model_only(
    query: StructuralQuery,
    lowered: LoweredSelectShape,
) -> Result<StructuralQuery, SqlLoweringError> {
    let schema = SchemaInfo::cached_for_generated_entity_model(query.model());

    apply_lowered_select_shape_with_schema(query, lowered, schema)
}

fn apply_lowered_select_shape_with_schema(
    mut query: StructuralQuery,
    lowered: LoweredSelectShape,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    let LoweredSelectShape {
        projection_selection,
        grouped_aggregates,
        group_by_fields,
        distinct,
        having,
        filter,
        order_by,
        limit,
        offset,
    } = lowered;
    validate_select_sql_capabilities(
        schema,
        &projection_selection,
        group_by_fields.as_slice(),
        order_by.as_slice(),
    )?;

    // Phase 1: apply grouped declaration semantics.
    for field in group_by_fields {
        query = query.group_by_with_schema(field, schema)?;
    }

    // Phase 2: apply scalar DISTINCT and projection contracts.
    if distinct {
        query = query.distinct();
    }
    query = query.projection_selection(projection_selection.into_selection());
    for aggregate in grouped_aggregates {
        query = query.aggregate(aggregate);
    }

    // Phase 3: bind resolved HAVING expressions against grouped terminals.
    for clause in having {
        query = query.having_expr_preserving_shape(clause)?;
    }

    // Phase 4: attach the shared filter/order/page tail through the base-query lane.
    Ok(apply_lowered_base_query_shape_with_schema(
        query,
        LoweredBaseQueryShape {
            filter,
            order_by,
            limit,
            offset,
        },
        schema,
    ))
}

fn lower_grouped_aggregate_calls(
    grouped_aggregates: Vec<crate::db::sql::parser::SqlAggregateCall>,
    model: &'static EntityModel,
    schema: &SchemaInfo,
) -> Result<Vec<AggregateExpr>, SqlLoweringError> {
    grouped_aggregates
        .into_iter()
        .map(|aggregate| lower_grouped_aggregate_call(model, schema, aggregate))
        .collect()
}

// Validate every clause against the revision-scoped accepted capability view
// before projection planning can admit it.
fn validate_select_sql_capabilities(
    schema: &SchemaInfo,
    projection_selection: &LoweredSqlProjectionSelection,
    group_by_fields: &[String],
    order_by: &[LoweredSqlOrderTerm],
) -> Result<(), SqlLoweringError> {
    validate_projection_sql_capabilities(schema, projection_selection)?;
    validate_group_by_sql_capabilities(schema, group_by_fields)?;
    validate_order_sql_capabilities(schema, order_by)?;

    Ok(())
}

// Check SELECT output admission against schema-owned SQL capabilities before
// unsupported field families can enter projection planning.
fn validate_projection_sql_capabilities(
    schema: &SchemaInfo,
    selection: &LoweredSqlProjectionSelection,
) -> Result<(), SqlLoweringError> {
    match selection.selection() {
        ProjectionSelection::All => {}
        ProjectionSelection::Fields(fields) => {
            for field in fields {
                ensure_sql_selectable_field(schema, field.as_str())?;
            }
        }
        ProjectionSelection::Exprs(fields) => {
            debug_assert_eq!(
                fields.len(),
                selection.expr_analyses().len(),
                "lowered SQL projection analyses must stay aligned for validation",
            );
            for analysis in selection.expr_analyses() {
                validate_projection_expr_sql_capabilities(schema, analysis)?;
            }
        }
    }

    Ok(())
}

// Walk projection expressions for source-field admission. The expression
// engine still owns type inference and coercion; this pass only rejects fields
// whose schema-owned SQL capabilities cannot support result projection.
fn validate_projection_expr_sql_capabilities(
    schema: &SchemaInfo,
    analysis: &LoweredExprAnalysis,
) -> Result<(), SqlLoweringError> {
    for source_ref in analysis.source_refs() {
        match source_ref {
            LoweredExprSourceRef::Direct(field) => {
                ensure_sql_selectable_field(schema, field.as_str())?;
            }
            LoweredExprSourceRef::Path(path) => {
                ensure_sql_selectable_field_path(schema, path)?;
            }
        }
    }

    Ok(())
}

// Apply one nested SELECT/source-field capability check and keep unknown-field
// reporting on the planner-owned unknown-field path.
fn ensure_sql_selectable_field_path(
    schema: &SchemaInfo,
    path: &FieldPath,
) -> Result<(), SqlLoweringError> {
    let Some(capabilities) = schema.nested_sql_capabilities(path.root().as_str(), path.segments())
    else {
        return Err(SqlLoweringError::unknown_field(render_field_path(path)));
    };
    if !capabilities.selectable() {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    Ok(())
}

// GROUP BY identity must use the schema-owned groupable capability instead of
// re-deriving comparable/identity behavior from generated field kinds.
fn validate_group_by_sql_capabilities(
    schema: &SchemaInfo,
    fields: &[String],
) -> Result<(), SqlLoweringError> {
    for field in fields {
        let Some(capabilities) = schema.sql_capabilities(field) else {
            return Err(SqlLoweringError::unknown_field(field.as_str()));
        };
        // Keep enum grouping closed until the canonical-ID runtime/key route
        // consumes the catalog capability recorded on accepted SchemaInfo.
        if capabilities.enum_equality().is_some() || !capabilities.groupable() {
            return Err(SqlLoweringError::unsupported_select_group_by());
        }
    }

    Ok(())
}

// ORDER BY direct fields use accepted top-level orderability. Computed ORDER BY
// expressions continue through expression planning because their result type,
// not each input field's type, owns the final orderability decision.
fn validate_order_sql_capabilities(
    schema: &SchemaInfo,
    terms: &[LoweredSqlOrderTerm],
) -> Result<(), SqlLoweringError> {
    for term in terms {
        let Expr::Field(field) = &term.expr else {
            continue;
        };
        let Some(capabilities) = schema.sql_capabilities(field.as_str()) else {
            continue;
        };
        if !capabilities.orderable() {
            return Err(QueryError::unsupported_sql_feature(
                SqlFeatureCode::OrderByFieldNotOrderable,
            )
            .into());
        }
    }

    Ok(())
}

// Apply one direct SELECT/source-field capability check and keep unknown-field
// reporting on the planner-owned unknown-field path.
fn ensure_sql_selectable_field(
    schema: &SchemaInfo,
    field_name: &str,
) -> Result<(), SqlLoweringError> {
    let Some(capabilities) = schema.sql_capabilities(field_name) else {
        return Err(SqlLoweringError::unknown_field(field_name));
    };
    if !capabilities.selectable() {
        return Err(SqlLoweringError::unsupported_select_projection());
    }

    Ok(())
}

fn render_field_path(path: &FieldPath) -> String {
    let mut rendered = path.root().as_str().to_string();
    for segment in path.segments() {
        rendered.push('.');
        rendered.push_str(segment);
    }
    rendered
}

/// Validate accepted-schema SQL capabilities for one lowered base-query tail.
///
/// This applies only to direct-field ORDER BY terms. Filters remain expression
/// planner-owned, and computed ORDER BY terms are validated by their inferred
/// result type rather than by each source field they reference.
pub(in crate::db::sql::lowering) fn validate_base_query_sql_capabilities(
    schema: &SchemaInfo,
    lowered: &LoweredBaseQueryShape,
) -> Result<(), SqlLoweringError> {
    validate_order_sql_capabilities(schema, lowered.order_by.as_slice())
}

#[cfg(test)]
pub(in crate::db::sql::lowering) fn apply_lowered_base_query_shape_for_model_only(
    query: StructuralQuery,
    lowered: LoweredBaseQueryShape,
) -> StructuralQuery {
    let schema = SchemaInfo::cached_for_generated_entity_model(query.model());

    apply_lowered_base_query_shape_with_schema(query, lowered, schema)
}

/// Apply one lowered base-query tail through an explicit schema projection.
///
/// SQL aggregate/session paths use this hook when they already hold the
/// accepted schema view and need filter canonicalization to respect it.
pub(in crate::db::sql::lowering) fn apply_lowered_base_query_shape_with_schema(
    mut query: StructuralQuery,
    lowered: LoweredBaseQueryShape,
    schema: &SchemaInfo,
) -> StructuralQuery {
    if let Some(filter) = lowered.filter {
        query = filter.apply_to_query(query, schema);
    }
    query = apply_order_terms_structural(query, lowered.order_by);
    if let Some(limit) = lowered.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = lowered.offset {
        query = query.offset(offset);
    }

    query
}

#[cfg(test)]
pub(in crate::db) fn bind_lowered_sql_query_structural_for_model_only(
    model: &'static EntityModel,
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<StructuralQuery, SqlLoweringError> {
    bind_lowered_sql_query_structural_with_schema(
        model,
        lowered,
        consistency,
        SchemaInfo::cached_for_generated_entity_model(model),
    )
}

/// Bind one lowered SQL query with an explicit schema projection.
#[cfg(any(test, feature = "sql-explain"))]
pub(in crate::db) fn bind_lowered_sql_query_structural_with_schema(
    model: &'static EntityModel,
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    match lowered {
        crate::db::sql::lowering::LoweredSqlQuery::Select(select) => {
            bind_lowered_sql_select_query_structural_with_schema(model, select, consistency, schema)
        }
        crate::db::sql::lowering::LoweredSqlQuery::Delete(delete) => {
            bind_lowered_sql_delete_query_structural_with_schema(model, delete, consistency, schema)
        }
    }
}

/// Bind one lowered SQL SELECT shape with an explicit schema projection.
///
/// Session SQL compile paths use this accepted-schema-aware boundary so
/// top-level predicate/filter literal canonicalization follows live schema
/// reconciliation, while direct lowering tests keep the generated fallback.
pub(in crate::db) fn bind_lowered_sql_select_query_structural_with_schema(
    model: &'static EntityModel,
    select: LoweredSelectShape,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    apply_lowered_select_shape_with_schema(StructuralQuery::new(model, consistency), select, schema)
}

/// Bind one lowered base-query selector with an explicit schema projection.
pub(in crate::db) fn bind_lowered_sql_base_query_structural_with_schema(
    model: &'static EntityModel,
    base_query: LoweredBaseQueryShape,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    validate_base_query_sql_capabilities(schema, &base_query)?;

    Ok(apply_lowered_base_query_shape_with_schema(
        StructuralQuery::new(model, consistency),
        base_query,
        schema,
    ))
}

/// Bind one lowered SQL DELETE shape with an explicit schema projection.
pub(in crate::db) fn bind_lowered_sql_delete_query_structural_with_schema(
    model: &'static EntityModel,
    delete: LoweredBaseQueryShape,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    validate_base_query_sql_capabilities(schema, &delete)?;

    Ok(apply_lowered_base_query_shape_with_schema(
        StructuralQuery::new(model, consistency).delete(),
        delete,
        schema,
    ))
}

/// Bind one parsed SQL DELETE statement with an explicit schema projection.
pub(in crate::db) fn bind_sql_delete_statement_structural_with_schema(
    model: &'static EntityModel,
    statement: SqlDeleteStatement,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    let base_query = lower_delete_shape(statement)?;

    bind_lowered_sql_delete_query_structural_with_schema(model, base_query, consistency, schema)
}

/// Bind one SQL UPDATE selector with an explicit schema projection.
///
/// This mirrors the base-query read boundary used by cached SELECT/DELETE
/// compilation so update target selection observes accepted top-level field
/// literal rules.
pub(in crate::db) fn bind_sql_update_selector_query_structural_with_schema(
    model: &'static EntityModel,
    statement: &SqlUpdateStatement,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<StructuralQuery, SqlLoweringError> {
    if schema.primary_key_names().is_empty() {
        return Err(QueryError::invariant().into());
    }

    let base_query = lower_update_selector_shape(statement, schema.primary_key_names())?;

    bind_lowered_sql_base_query_structural_with_schema(model, base_query, consistency, schema)
}

// Test-only typed SQL lowering still uses this adapter to compare the
// generic-free structural SQL lane with public typed query behavior.
#[cfg(test)]
pub(in crate::db) fn bind_lowered_sql_query_for_model_only<E: EntityKind>(
    lowered: crate::db::sql::lowering::LoweredSqlQuery,
    consistency: MissingRowPolicy,
) -> Result<Query<E>, SqlLoweringError> {
    let structural =
        bind_lowered_sql_query_structural_for_model_only(E::MODEL, lowered, consistency)?;

    Ok(Query::from_inner(structural))
}

pub(in crate::db::sql::lowering) fn lower_delete_shape(
    statement: SqlDeleteStatement,
) -> Result<LoweredBaseQueryShape, SqlLoweringError> {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        offset,
        entity: _,
        table_alias: _,
        returning: _,
    } = statement;

    lower_delete_query_modifiers(predicate, order_by, limit, offset)
}

/// Lower one full DELETE statement into the narrowed prepared execution shape.
pub(in crate::db::sql::lowering) fn lower_delete_statement_shape(
    statement: SqlDeleteStatement,
) -> Result<LoweredDeleteShape, SqlLoweringError> {
    let SqlDeleteStatement {
        predicate,
        order_by,
        limit,
        offset,
        returning,
        entity: _,
        table_alias: _,
    } = statement;
    let base_query = lower_delete_query_modifiers(predicate, order_by, limit, offset)?;

    Ok(LoweredDeleteShape {
        base_query,
        returning,
    })
}

// Lower the executable DELETE query modifiers once for both generic base-query
// callers and the narrowed prepared DELETE artifact.
fn lower_delete_query_modifiers(
    predicate: Option<SqlExpr>,
    order_by: Vec<SqlOrderTerm>,
    limit: Option<u32>,
    offset: Option<u32>,
) -> Result<LoweredBaseQueryShape, SqlLoweringError> {
    let filter = match predicate.as_ref() {
        Some(expr) => Some(LoweredSqlFilter::from_delete_where_expr(expr)?),
        None => None,
    };

    Ok(LoweredBaseQueryShape {
        filter,
        order_by: lower_order_terms(order_by)?,
        limit,
        offset,
    })
}

// Lower the executable UPDATE selector into the shared base-query shape while
// preserving the current UPDATE-only policy gates: a WHERE predicate is
// required, ORDER BY terms must be direct fields, and windowed updates without
// an explicit primary-key tie-breaker keep the historical primary-key fallback.
fn lower_update_selector_shape(
    statement: &SqlUpdateStatement,
    primary_key_names: &[String],
) -> Result<LoweredBaseQueryShape, SqlLoweringError> {
    let Some(predicate) = statement.predicate.clone() else {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::UpdateMissingWherePredicate,
        )
        .into());
    };
    let mut order_by = statement.order_by.clone();

    for term in &order_by {
        if term.direct_field_name().is_none() {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::WriteOrderByUnsupportedShape,
            )
            .into());
        }
    }

    append_primary_key_order_fallback(&mut order_by, primary_key_names);

    Ok(LoweredBaseQueryShape {
        filter: Some(LoweredSqlFilter::from_update_where_expr(&predicate)?),
        order_by: lower_order_terms(order_by)?,
        limit: statement.limit,
        offset: statement.offset,
    })
}

// Keep UPDATE target selection deterministic by preserving the previous
// session-write fallback: append every missing accepted primary-key component
// after caller-supplied terms.
fn append_primary_key_order_fallback(
    order_by: &mut Vec<SqlOrderTerm>,
    primary_key_names: &[String],
) {
    for primary_key_name in primary_key_names {
        if order_by
            .iter()
            .any(|term| term.direct_field_name() == Some(primary_key_name.as_str()))
        {
            continue;
        }

        order_by.push(SqlOrderTerm {
            field: SqlExpr::Field(primary_key_name.clone()),
            direction: SqlOrderDirection::Asc,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_selector_primary_key_order_fallback_appends_missing_composite_components() {
        let mut order_by = vec![SqlOrderTerm {
            field: SqlExpr::Field("tenant_id".to_string()),
            direction: SqlOrderDirection::Asc,
        }];
        let primary_key_names = vec!["tenant_id".to_string(), "local_id".to_string()];

        append_primary_key_order_fallback(&mut order_by, &primary_key_names);

        assert_eq!(order_by.len(), 2);
        assert_eq!(order_by[0].direct_field_name(), Some("tenant_id"));
        assert_eq!(order_by[1].direct_field_name(), Some("local_id"));
    }
}
