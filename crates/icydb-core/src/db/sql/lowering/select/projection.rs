use crate::{
    db::{
        query::plan::expr::{Alias, Expr, FieldId, ProjectionField, ProjectionSelection},
        sql::{
            identifier::split_qualified_identifier,
            lowering::{
                AnalyzedLoweredExpr, LoweredExprAnalysis, SqlLoweringError,
                aggregate::SqlAggregateCallInterner,
                expr::{SqlExprPhase, lower_sql_expr},
                select::order::LoweredSqlOrderTerm,
            },
            parser::{SqlAggregateCall, SqlProjection, SqlSelectItem},
        },
    },
    model::entity::EntityModel,
};

///
/// LoweredSqlProjectionSelection
///
/// SQL-local projection wrapper that keeps expression analyses beside lowered
/// projection fields until schema-bound SQL capability validation consumes
/// them.
///

#[derive(Clone, Debug)]
pub(super) struct LoweredSqlProjectionSelection {
    selection: ProjectionSelection,
    expr_analyses: Vec<LoweredExprAnalysis>,
}

impl LoweredSqlProjectionSelection {
    #[must_use]
    pub(super) const fn all() -> Self {
        Self {
            selection: ProjectionSelection::All,
            expr_analyses: Vec::new(),
        }
    }

    #[must_use]
    pub(super) const fn fields(fields: Vec<FieldId>) -> Self {
        Self {
            selection: ProjectionSelection::Fields(fields),
            expr_analyses: Vec::new(),
        }
    }

    #[must_use]
    pub(super) fn exprs(fields: Vec<ProjectionField>, analyses: Vec<LoweredExprAnalysis>) -> Self {
        debug_assert_eq!(
            fields.len(),
            analyses.len(),
            "lowered SQL projection analysis must stay aligned with projection fields",
        );
        Self {
            selection: ProjectionSelection::Exprs(fields),
            expr_analyses: analyses,
        }
    }

    #[must_use]
    pub(super) const fn selection(&self) -> &ProjectionSelection {
        &self.selection
    }

    #[must_use]
    pub(super) const fn expr_analyses(&self) -> &[LoweredExprAnalysis] {
        self.expr_analyses.as_slice()
    }

    #[must_use]
    pub(super) fn into_selection(self) -> ProjectionSelection {
        self.selection
    }

    fn is_grouped_canonical_identity(&self, group_by: &[String]) -> bool {
        matches!(
            &self.selection,
            ProjectionSelection::Exprs(fields)
                if grouped_projection_is_canonical_identity(fields.as_slice(), group_by)
        )
    }
}

///
/// LoweredGroupedProjection
///
/// SQL-local grouped projection artifact that keeps the projection and the
/// first-seen unique aggregate calls from the same analyzed expression pass.
///

#[derive(Clone, Debug)]
pub(super) struct LoweredGroupedProjection {
    selection: LoweredSqlProjectionSelection,
    aggregate_calls: Vec<SqlAggregateCall>,
}

impl LoweredGroupedProjection {
    #[must_use]
    pub(super) const fn aggregate_calls(&self) -> &[SqlAggregateCall] {
        self.aggregate_calls.as_slice()
    }

    #[must_use]
    pub(super) fn into_projection_selection(
        self,
        allow_identity_fast_path: bool,
        group_by: &[String],
    ) -> LoweredSqlProjectionSelection {
        if allow_identity_fast_path && self.selection.is_grouped_canonical_identity(group_by) {
            LoweredSqlProjectionSelection::all()
        } else {
            self.selection
        }
    }
}

pub(super) fn lower_scalar_projection_selection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
) -> Result<LoweredSqlProjectionSelection, SqlLoweringError> {
    let SqlProjection::Items(items) = projection else {
        return Ok(LoweredSqlProjectionSelection::all());
    };

    if let Some(field_ids) = direct_scalar_field_selection(items.as_slice(), projection_aliases) {
        return Ok(LoweredSqlProjectionSelection::fields(field_ids));
    }

    let mut fields = Vec::with_capacity(items.len());
    let mut projection_facts = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let analyzed = lower_analyzed_select_item_expr(&item, SqlExprPhase::Scalar, None)?;
        let (expr, expr_facts) = analyzed.into_parts();
        fields.push(ProjectionField::Scalar {
            expr,
            alias: projection_aliases
                .get(index)
                .and_then(Option::as_deref)
                .map(Alias::new),
        });
        projection_facts.push(expr_facts);
    }

    Ok(LoweredSqlProjectionSelection::exprs(
        fields,
        projection_facts,
    ))
}

pub(super) fn lower_grouped_projection(
    projection: SqlProjection,
    projection_aliases: &[Option<String>],
    group_by: &[String],
    model: &'static EntityModel,
) -> Result<LoweredGroupedProjection, SqlLoweringError> {
    if group_by.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::grouped_projection_requires_explicit_list());
    };
    let grouped_field_names = group_by.iter().map(String::as_str).collect::<Vec<_>>();

    let mut seen_aggregate = false;
    let mut fields = Vec::with_capacity(items.len());
    let mut projection_facts = Vec::with_capacity(items.len());
    let mut aggregate_calls = Vec::new();
    let mut aggregate_call_interner = SqlAggregateCallInterner::new();

    for (index, item) in items.into_iter().enumerate() {
        let analyzed =
            lower_analyzed_select_item_expr(&item, SqlExprPhase::PostAggregate, Some(model))?;
        let expr_facts = analyzed.analysis();
        let contains_aggregate = expr_facts.contains_aggregate();
        if seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::grouped_projection_scalar_after_aggregate(
                index,
            ));
        }
        validate_grouped_projection_expr(index, grouped_field_names.as_slice(), expr_facts)?;
        seen_aggregate |= contains_aggregate;
        if contains_aggregate {
            aggregate_call_interner.extend_select_item(&mut aggregate_calls, &item);
        }

        let (expr, expr_facts) = analyzed.into_parts();
        fields.push(ProjectionField::Scalar {
            expr,
            alias: projection_aliases
                .get(index)
                .and_then(Option::as_deref)
                .map(Alias::new),
        });
        projection_facts.push(expr_facts);
    }

    if !seen_aggregate {
        return Err(SqlLoweringError::grouped_projection_requires_aggregate());
    }

    Ok(LoweredGroupedProjection {
        selection: LoweredSqlProjectionSelection::exprs(fields, projection_facts),
        aggregate_calls,
    })
}

// Validate one grouped projection expression against grouped-key authority
// while preserving specific unknown-field diagnostics.
fn validate_grouped_projection_expr(
    index: usize,
    grouped_field_names: &[&str],
    analysis: &LoweredExprAnalysis,
) -> Result<(), SqlLoweringError> {
    if let Some(field) = analysis.first_unknown_field() {
        return Err(SqlLoweringError::unknown_field(field));
    }
    if !analysis.references_only_direct_fields(grouped_field_names) {
        return Err(SqlLoweringError::grouped_projection_references_non_group_field(index));
    }

    Ok(())
}

// Preserve the older grouped `ProjectionSelection::All` fast path only for
// the canonical identity shape where projected grouped fields match `GROUP BY`
// exactly and aggregate terminals follow in declaration order.
fn grouped_projection_is_canonical_identity(
    fields: &[ProjectionField],
    group_by: &[String],
) -> bool {
    if fields.len() < group_by.len() {
        return false;
    }

    let Some((group_fields, aggregate_fields)) = fields.split_at_checked(group_by.len()) else {
        return false;
    };

    group_fields
        .iter()
        .zip(group_by.iter())
        .all(|(field, group_by)| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Field(field_id),
                    alias: None,
                } if field_id.as_str() == group_by
            )
        })
        && aggregate_fields.iter().all(|field| {
            matches!(
                field,
                ProjectionField::Scalar {
                    expr: Expr::Aggregate(_),
                    alias: None,
                }
            )
        })
}

// Enforce the SQL DISTINCT rule at lowering time: every ORDER BY term must be
// fully expressible as a function of the outward projected distinct tuple
// rather than from hidden base-row fields.
pub(super) fn validate_distinct_order_terms_against_projection(
    projection: &ProjectionSelection,
    order_by: &[LoweredSqlOrderTerm],
    model: &'static EntityModel,
) -> Result<(), SqlLoweringError> {
    if order_by
        .iter()
        .all(|term| distinct_order_term_is_derivable_from_projection(projection, term, model))
    {
        return Ok(());
    }

    Err(SqlLoweringError::distinct_order_by_requires_projected_tuple())
}

// Keep DISTINCT ORDER BY derivation intentionally narrow:
// - exact projected expressions are always admissible
// - otherwise the term may reference only direct projected fields
// - `SELECT DISTINCT *` exposes the full entity field set
// This is a field-level proof only; it does not admit hidden payloads or a
// broader symbolic derivation model.
fn distinct_order_term_is_derivable_from_projection(
    projection: &ProjectionSelection,
    order_term: &LoweredSqlOrderTerm,
    model: &'static EntityModel,
) -> bool {
    match projection {
        ProjectionSelection::All => {
            let projected_fields = model
                .fields()
                .iter()
                .map(crate::model::field::FieldModel::name)
                .collect::<Vec<_>>();

            order_term
                .analysis
                .references_only_direct_fields(projected_fields.as_slice())
        }
        ProjectionSelection::Fields(field_ids) => {
            let projected_fields = field_ids.iter().map(FieldId::as_str).collect::<Vec<_>>();

            order_term
                .analysis
                .references_only_direct_fields(projected_fields.as_slice())
        }
        ProjectionSelection::Exprs(fields) => {
            if fields.iter().any(|field| match field {
                ProjectionField::Scalar { expr, .. } => expr == &order_term.expr,
            }) {
                return true;
            }

            let projected_fields = fields
                .iter()
                .filter_map(ProjectionField::direct_field_name)
                .collect::<Vec<_>>();

            order_term
                .analysis
                .references_only_direct_fields(projected_fields.as_slice())
        }
    }
}

pub(super) fn direct_scalar_field_selection(
    items: &[SqlSelectItem],
    projection_aliases: &[Option<String>],
) -> Option<Vec<FieldId>> {
    if !projection_aliases.iter().all(Option::is_none) {
        return None;
    }

    items
        .iter()
        .map(|item| match item {
            SqlSelectItem::Field(field) if split_qualified_identifier(field).is_none() => {
                Some(FieldId::new(field.clone()))
            }
            SqlSelectItem::Field(_) | SqlSelectItem::Aggregate(_) | SqlSelectItem::Expr(_) => None,
        })
        .collect()
}

pub(in crate::db::sql::lowering) fn lower_select_item_expr(
    item: &SqlSelectItem,
    phase: SqlExprPhase,
) -> Result<Expr, SqlLoweringError> {
    lower_sql_expr(
        &crate::db::sql::parser::SqlExpr::from_select_item(item),
        phase,
    )
}

pub(in crate::db::sql::lowering) fn lower_analyzed_select_item_expr(
    item: &SqlSelectItem,
    phase: SqlExprPhase,
    model: Option<&EntityModel>,
) -> Result<AnalyzedLoweredExpr, SqlLoweringError> {
    Ok(AnalyzedLoweredExpr::new(
        lower_select_item_expr(item, phase)?,
        model,
    ))
}
