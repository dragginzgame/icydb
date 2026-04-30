//! Module: db::executor::projection::grouped
//! Defines grouped-row projection evaluation over finalized group keys and
//! aggregate outputs.

use crate::{
    db::{
        executor::projection::eval::ProjectionEvalError,
        query::plan::{
            FieldSlot, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
            expr::{CompiledExpr, CompiledExprValueReader, ProjectionSpec},
        },
    },
    error::InternalError,
    value::Value,
};

pub(in crate::db::executor) use crate::db::query::plan::expr::compile_grouped_projection_plan;
pub(in crate::db) use crate::db::query::plan::expr::{
    compile_grouped_projection_expr, evaluate_grouped_having_expr,
};

///
/// GroupedRowView
///
/// Read-only grouped-row adapter for expression evaluation over finalized
/// grouped-key and aggregate outputs.
///

pub(in crate::db) struct GroupedRowView<'a> {
    pub(in crate::db::executor::projection) key_values: &'a [Value],
    pub(in crate::db::executor::projection) aggregate_values: &'a [Value],
    #[cfg(test)]
    group_fields: &'a [FieldSlot],
    #[cfg(test)]
    pub(in crate::db::executor::projection) aggregate_execution_specs:
        &'a [GroupedAggregateExecutionSpec],
}

impl<'a> GroupedRowView<'a> {
    /// Build one grouped-row adapter from grouped finalization payloads.
    #[must_use]
    pub(in crate::db) const fn new(
        key_values: &'a [Value],
        aggregate_values: &'a [Value],
        group_fields: &'a [FieldSlot],
        aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
    ) -> Self {
        #[cfg(not(test))]
        let _ = (group_fields, aggregate_execution_specs);

        Self {
            key_values,
            aggregate_values,
            #[cfg(test)]
            group_fields,
            #[cfg(test)]
            aggregate_execution_specs,
        }
    }

    /// Borrow grouped key values in grouped-field declaration order.
    #[must_use]
    pub(in crate::db) const fn key_values(&self) -> &'a [Value] {
        self.key_values
    }

    /// Borrow finalized grouped aggregate values in execution-spec order.
    #[must_use]
    pub(in crate::db) const fn aggregate_values(&self) -> &'a [Value] {
        self.aggregate_values
    }

    /// Borrow grouped field slots used to interpret grouped key offsets.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }
}

impl CompiledExprValueReader for GroupedRowView<'_> {
    fn read_slot(&self, _slot: usize) -> Option<&Value> {
        None
    }

    fn read_group_key(&self, offset: usize) -> Option<&Value> {
        self.key_values().get(offset)
    }

    fn read_aggregate(&self, index: usize) -> Option<&Value> {
        self.aggregate_values().get(index)
    }
}

///
/// CompiledGroupedProjectionPlan
///
/// Executor-owned grouped projection compilation contract.
/// This keeps the grouped identity short-circuit and compiled projection
/// carriage under the projection boundary so grouped runtime lanes consume one
/// shared compiled evaluator contract instead of open-coding it.
///

#[derive(Clone)]
pub(in crate::db) struct CompiledGroupedProjectionPlan<'a> {
    compiled_projection: Vec<CompiledExpr>,
    projection_layout: &'a PlannedProjectionLayout,
    group_fields: &'a [FieldSlot],
    aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
}

impl<'a> CompiledGroupedProjectionPlan<'a> {
    /// Build one compiled grouped projection contract from already-compiled expressions.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn from_parts_for_test(
        compiled_projection: Vec<CompiledExpr>,
        projection_layout: &'a PlannedProjectionLayout,
        group_fields: &'a [FieldSlot],
        aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
    ) -> Self {
        Self {
            compiled_projection,
            projection_layout,
            group_fields,
            aggregate_execution_specs,
        }
    }

    /// Borrow the compiled grouped projection expression slice.
    #[must_use]
    pub(in crate::db) const fn compiled_projection(&self) -> &[CompiledExpr] {
        self.compiled_projection.as_slice()
    }

    /// Borrow the planner-owned grouped projection layout.
    #[must_use]
    pub(in crate::db) const fn projection_layout(&self) -> &'a PlannedProjectionLayout {
        self.projection_layout
    }

    /// Borrow grouped key field slots used by grouped projection evaluation.
    #[must_use]
    pub(in crate::db) const fn group_fields(&self) -> &'a [FieldSlot] {
        self.group_fields
    }

    /// Borrow grouped aggregate execution specs used by grouped projection evaluation.
    #[must_use]
    pub(in crate::db) const fn aggregate_execution_specs(
        &self,
    ) -> &'a [GroupedAggregateExecutionSpec] {
        self.aggregate_execution_specs
    }
}

/// Compile one grouped projection contract only when the planner has not
/// already proved the grouped output projection is row-identical.
pub(in crate::db) fn compile_grouped_projection_plan_if_needed<'a>(
    projection: &ProjectionSpec,
    projection_is_identity: bool,
    projection_layout: &'a PlannedProjectionLayout,
    group_fields: &'a [FieldSlot],
    aggregate_execution_specs: &'a [GroupedAggregateExecutionSpec],
) -> Result<Option<CompiledGroupedProjectionPlan<'a>>, InternalError> {
    if projection_is_identity {
        return Ok(None);
    }

    let compiled_projection =
        compile_grouped_projection_plan(projection, group_fields, aggregate_execution_specs)
            .map_err(ProjectionEvalError::into_grouped_projection_internal_error)?;

    Ok(Some(CompiledGroupedProjectionPlan {
        compiled_projection,
        projection_layout,
        group_fields,
        aggregate_execution_specs,
    }))
}

/// Evaluate one compiled grouped projection plan into ordered projected values.
#[cfg(test)]
pub(in crate::db::executor) fn evaluate_grouped_projection_values(
    compiled_projection: &[CompiledExpr],
    grouped_row: &GroupedRowView<'_>,
) -> Result<Vec<Value>, ProjectionEvalError> {
    let mut projected_values = Vec::with_capacity(compiled_projection.len());

    for expr in compiled_projection {
        projected_values.push(expr.evaluate(grouped_row)?.into_owned());
    }

    Ok(projected_values)
}
