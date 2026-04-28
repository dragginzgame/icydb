use crate::{
    db::sql::{
        lowering::{
            SqlLoweringError, aggregate::grouped::extend_unique_sql_select_item_aggregate_calls,
            analyze_lowered_expr, expr::SqlExprPhase, select::lower_select_item_expr,
        },
        parser::{SqlAggregateCall, SqlProjection, SqlSelectItem},
    },
    model::entity::EntityModel,
};

pub(in crate::db::sql::lowering) fn grouped_projection_aggregate_calls(
    projection: &SqlProjection,
    group_by_fields: &[String],
    model: &'static EntityModel,
) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
    if group_by_fields.is_empty() {
        return Err(SqlLoweringError::unsupported_select_group_by());
    }

    let SqlProjection::Items(items) = projection else {
        return Err(SqlLoweringError::grouped_projection_requires_explicit_list());
    };

    GroupedProjectionAggregateCollector::new(group_by_fields, model)?.collect_from_items(items)
}

///
/// GroupedProjectionAggregateCollector
///
/// Local grouped-projection aggregate extraction owner. It validates grouped
/// field authority, preserves the first aggregate ordering rule, and keeps one
/// stable unique aggregate list so grouped reducer slots are derived once.
///

struct GroupedProjectionAggregateCollector<'a> {
    grouped_field_names: Vec<&'a str>,
    model: &'static EntityModel,
    aggregate_calls: Vec<SqlAggregateCall>,
    seen_aggregate: bool,
}

impl<'a> GroupedProjectionAggregateCollector<'a> {
    // Build the grouped projection collector once so field-authority and
    // aggregate-ordering policy stay on one local owner.
    fn new(
        group_by_fields: &'a [String],
        model: &'static EntityModel,
    ) -> Result<Self, SqlLoweringError> {
        if group_by_fields.is_empty() {
            return Err(SqlLoweringError::unsupported_select_group_by());
        }

        Ok(Self {
            grouped_field_names: group_by_fields.iter().map(String::as_str).collect(),
            model,
            aggregate_calls: Vec::new(),
            seen_aggregate: false,
        })
    }

    // Walk grouped projection items in SQL order so first-seen aggregate leaves
    // map onto one stable grouped reducer slot ordering.
    fn collect_from_items(
        mut self,
        items: &[SqlSelectItem],
    ) -> Result<Vec<SqlAggregateCall>, SqlLoweringError> {
        for (index, item) in items.iter().enumerate() {
            self.collect_item(index, item)?;
        }

        if self.aggregate_calls.is_empty() {
            return Err(SqlLoweringError::grouped_projection_requires_aggregate());
        }

        Ok(self.aggregate_calls)
    }

    // Validate one grouped projection item before collecting any aggregate
    // leaves so field-resolution and grouped-key diagnostics stay precise.
    fn collect_item(&mut self, index: usize, item: &SqlSelectItem) -> Result<(), SqlLoweringError> {
        let expr = lower_select_item_expr(item, SqlExprPhase::PostAggregate)?;
        let analysis = analyze_lowered_expr(&expr, Some(self.model));
        let contains_aggregate = analysis.contains_aggregate();
        if self.seen_aggregate && !contains_aggregate {
            return Err(SqlLoweringError::grouped_projection_scalar_after_aggregate(
                index,
            ));
        }
        if let Some(field) = analysis.first_unknown_field() {
            return Err(SqlLoweringError::unknown_field(field));
        }
        if !expr.references_only_fields(self.grouped_field_names.as_slice()) {
            return Err(SqlLoweringError::grouped_projection_references_non_group_field(index));
        }
        if contains_aggregate {
            self.seen_aggregate = true;
            extend_unique_sql_select_item_aggregate_calls(&mut self.aggregate_calls, item);
        }

        Ok(())
    }
}
