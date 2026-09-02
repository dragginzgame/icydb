//! Module: db::executor::projection::grouped
//! Defines grouped-row projection evaluation over finalized group keys and
//! aggregate outputs.

mod contracts;

use crate::{
    db::executor::projection::eval::ProjectionEvalError, error::InternalError, value::Value,
};
use std::borrow::Cow;

use self::contracts::{
    CompiledExpr, CompiledExprValueReader, GroupedAggregateExecutionSpec, PlannedProjectionLayout,
    ProjectionSpec,
};

pub(in crate::db::executor) use contracts::{
    compile_grouped_projection_expr, compile_grouped_projection_plan, evaluate_grouped_having_expr,
};

///
/// GroupedRowView
///
/// Read-only grouped-row adapter for expression evaluation over finalized
/// grouped-key and aggregate outputs.
///

pub(in crate::db::executor) struct GroupedRowView<'a> {
    pub(in crate::db::executor::projection) key_values: &'a [Value],
    pub(in crate::db::executor::projection) aggregate_values: &'a [Value],
}

impl<'a> GroupedRowView<'a> {
    /// Build one grouped-row adapter from grouped finalization payloads.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        key_values: &'a [Value],
        aggregate_values: &'a [Value],
    ) -> Self {
        Self {
            key_values,
            aggregate_values,
        }
    }

    /// Borrow grouped key values in grouped-field declaration order.
    #[must_use]
    pub(in crate::db::executor) const fn key_values(&self) -> &'a [Value] {
        self.key_values
    }

    /// Borrow finalized grouped aggregate values in execution-spec order.
    #[must_use]
    pub(in crate::db::executor) const fn aggregate_values(&self) -> &'a [Value] {
        self.aggregate_values
    }
}

impl CompiledExprValueReader for GroupedRowView<'_> {
    fn read_slot(&self, _slot: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_group_key(&self, offset: usize) -> Option<Cow<'_, Value>> {
        self.key_values().get(offset).map(Cow::Borrowed)
    }

    fn read_aggregate(&self, index: usize) -> Option<Cow<'_, Value>> {
        self.aggregate_values().get(index).map(Cow::Borrowed)
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
pub(in crate::db::executor) struct CompiledGroupedProjectionPlan<'a> {
    compiled_projection: Vec<CompiledExpr>,
    projection_layout: &'a PlannedProjectionLayout,
}

impl<'a> CompiledGroupedProjectionPlan<'a> {
    /// Build one compiled grouped projection contract from test inputs.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db::executor) const fn from_test_inputs(
        compiled_projection: Vec<CompiledExpr>,
        projection_layout: &'a PlannedProjectionLayout,
    ) -> Self {
        Self {
            compiled_projection,
            projection_layout,
        }
    }

    /// Borrow the compiled grouped projection expression slice.
    #[must_use]
    pub(in crate::db::executor) const fn compiled_projection(&self) -> &[CompiledExpr] {
        self.compiled_projection.as_slice()
    }

    /// Borrow the planner-owned grouped projection layout.
    #[must_use]
    pub(in crate::db::executor) const fn projection_layout(&self) -> &'a PlannedProjectionLayout {
        self.projection_layout
    }
}

/// Compile one grouped projection contract only when the planner has not
/// already proved the grouped output projection is row-identical.
pub(in crate::db::executor) fn compile_grouped_projection_plan_if_needed<'a>(
    projection: &ProjectionSpec,
    projection_is_identity: bool,
    projection_layout: &'a PlannedProjectionLayout,
    group_fields: &'a crate::db::query::plan::GroupFieldSet,
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
    }))
}
