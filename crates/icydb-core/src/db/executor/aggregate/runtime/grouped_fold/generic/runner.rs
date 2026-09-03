//! Module: executor::aggregate::runtime::grouped_fold::generic::runner
//! Responsibility: canonical grouped reducer path.
//! Boundary: owns generic grouped ingest and page finalization wiring.

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext, GroupError, ProjectionSpec,
            contracts::GroupedDistinctExecutionMode,
            runtime::grouped_fold::{
                bundle::{
                    GroupedAggregateBundle, GroupedAggregateBundleSpec, OrderedGroupedAggregateFold,
                },
                dispatch::group_fields_support_borrowed_group_probe,
                generic::{OrderedGroupedPageSelection, page_finalize::finalize_grouped_page},
            },
        },
        pipeline::{
            contracts::{GroupedCursorPage, GroupedRouteStage},
            runtime::{GroupedFoldStage, GroupedStreamStage},
        },
    },
    error::InternalError,
};

///
/// GenericGroupedFoldRunner
///
/// GenericGroupedFoldRunner keeps the canonical grouped reducer path under one
/// route-owned execution contract.
/// It owns row ingest plus grouped finalization for grouped routes that do not
/// take the dedicated DISTINCT or `COUNT(*)` fast paths.
///

struct GenericGroupedFoldRunner<'a> {
    route: &'a GroupedRouteStage,
    grouped_projection_spec: &'a ProjectionSpec,
    group_fields: &'a crate::db::query::plan::GroupFieldSet,
    group_probe_kind: GroupProbeKind,
}

#[derive(Clone, Copy)]
enum GroupProbeKind {
    DirectBorrowed,
    DirectOwned,
    PathAware,
}

impl<'a> GenericGroupedFoldRunner<'a> {
    // Build one generic grouped fold runner from route-owned grouped policy.
    fn new(route: &'a GroupedRouteStage, grouped_projection_spec: &'a ProjectionSpec) -> Self {
        Self {
            route,
            grouped_projection_spec,
            group_fields: route.group_fields(),
            group_probe_kind: match route.group_fields().as_direct() {
                Some(fields) if group_fields_support_borrowed_group_probe(fields) => {
                    GroupProbeKind::DirectBorrowed
                }
                Some(_) => GroupProbeKind::DirectOwned,
                None => GroupProbeKind::PathAware,
            },
        }
    }

    // Execute the generic grouped reducer path from grouped stream ingest
    // through grouped page finalization under one route-owned runner.
    fn execute(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        mut grouped_bundle: GroupedAggregateBundle,
    ) -> Result<GroupedFoldStage, InternalError> {
        let filtered_rows =
            self.fold_rows_into_bundle(stream, grouped_execution_context, &mut grouped_bundle)?;
        let (page_rows, next_cursor) = finalize_grouped_page(
            self.route,
            self.grouped_projection_spec,
            grouped_bundle,
            self.route.grouped_pagination_window(),
        )?;

        Ok(GroupedFoldStage::new(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            filtered_rows,
            true,
        ))
    }

    // Execute the canonical ordered grouped reducer while retaining exactly
    // one active aggregate group plus the response page.
    fn execute_ordered(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        mut grouped_fold: OrderedGroupedAggregateFold,
    ) -> Result<GroupedFoldStage, InternalError> {
        let mut selection = OrderedGroupedPageSelection::new(
            self.route,
            self.grouped_projection_spec,
            grouped_fold.aggregate_count(),
        )?;
        let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();
        let mut early_scan_stop = false;

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                && !row_view.eval_filter_program(effective_runtime_filter_program)?
            {
                continue;
            }

            early_scan_stop = if let Some(direct_group_fields) = self.group_fields.as_direct() {
                grouped_fold.ingest_row(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    direct_group_fields,
                    self.route.direction(),
                    |closed| selection.push_closed_group(closed),
                )
            } else if let Some(path_group_fields) = self.group_fields.as_path_aware() {
                grouped_fold.ingest_path_row(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    path_group_fields,
                    self.route.direction(),
                    |closed| selection.push_closed_group(closed),
                )
            } else {
                return Err(InternalError::query_executor_invariant());
            }
            .map_err(GroupError::into_internal_error)?;
            if early_scan_stop {
                break;
            }
            filtered_rows = filtered_rows.saturating_add(1);
        }

        if !early_scan_stop {
            grouped_fold
                .finish(grouped_execution_context, |closed| {
                    selection.push_closed_group(closed)
                })
                .map_err(GroupError::into_internal_error)?;
        }
        let (page_rows, next_cursor) = selection.finish(self.route)?;

        Ok(GroupedFoldStage::new(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            filtered_rows,
            true,
        ))
    }

    // Ingest grouped source rows into the shared grouped bundle while
    // preserving grouped budget contracts and borrowed grouped-key fast paths.
    fn fold_rows_into_bundle(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<usize, InternalError> {
        match self.group_probe_kind {
            GroupProbeKind::DirectBorrowed => self.fold_rows_into_bundle_borrowed(
                stream,
                grouped_execution_context,
                grouped_bundle,
            ),
            GroupProbeKind::DirectOwned => {
                self.fold_rows_into_bundle_owned(stream, grouped_execution_context, grouped_bundle)
            }
            GroupProbeKind::PathAware => self.fold_rows_into_bundle_path_aware(
                stream,
                grouped_execution_context,
                grouped_bundle,
            ),
        }
    }

    // Ingest grouped source rows with the borrowed existing-group probe path
    // selected once before the row loop.
    fn fold_rows_into_bundle_borrowed(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<usize, InternalError> {
        let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            // Phase 1: read and filter the source row before it reaches the
            // grouped aggregate states.
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                && !row_view.eval_filter_program(effective_runtime_filter_program)?
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            // Phase 2: update through the allocation-free existing-group
            // probe path selected outside the row loop.
            let Some(direct_group_fields) = self.group_fields.as_direct() else {
                return Err(InternalError::query_executor_invariant());
            };
            grouped_bundle
                .ingest_row_with_borrowed_group_probe(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    direct_group_fields,
                )
                .map_err(GroupError::into_internal_error)?;
        }

        Ok(filtered_rows)
    }

    // Ingest grouped source rows with the owned group-key path selected once
    // before the row loop.
    fn fold_rows_into_bundle_owned(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<usize, InternalError> {
        let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            // Phase 1: read and filter the source row before it reaches the
            // grouped aggregate states.
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                && !row_view.eval_filter_program(effective_runtime_filter_program)?
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            // Phase 2: update through the owned canonical key path selected
            // outside the row loop.
            let Some(direct_group_fields) = self.group_fields.as_direct() else {
                return Err(InternalError::query_executor_invariant());
            };
            grouped_bundle
                .ingest_row_with_owned_group_key(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    direct_group_fields,
                )
                .map_err(GroupError::into_internal_error)?;
        }

        Ok(filtered_rows)
    }

    fn fold_rows_into_bundle_path_aware(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<usize, InternalError> {
        let Some(group_fields) = self.group_fields.as_path_aware() else {
            return Err(InternalError::query_executor_invariant());
        };
        let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            if let Some(filter) = effective_runtime_filter_program
                && !row_view.eval_filter_program(filter)?
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);
            grouped_bundle
                .ingest_row_with_path_group_probe(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    group_fields,
                )
                .map_err(GroupError::into_internal_error)?;
        }

        Ok(filtered_rows)
    }
}

// Build the shared grouped aggregate bundle for canonical grouped terminal
// projection layout.
fn build_grouped_specs(
    route: &GroupedRouteStage,
    grouped_execution_context: &ExecutionContext,
) -> Result<Vec<GroupedAggregateBundleSpec>, InternalError> {
    route
        .grouped_aggregate_execution_specs()
        .iter()
        .map(|aggregate_spec| {
            GroupedAggregateBundleSpec::new(
                aggregate_spec.kind(),
                aggregate_spec.kind().materialized_fold_direction(),
                GroupedDistinctExecutionMode::new(
                    aggregate_spec.distinct(),
                    aggregate_spec.uses_grouped_distinct_value_dedup(),
                ),
                aggregate_spec.target_slot().cloned(),
                aggregate_spec.compiled_input_expr().cloned(),
                aggregate_spec.compiled_filter_expr().cloned(),
                grouped_execution_context
                    .config()
                    .max_distinct_values_per_group(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
}

// Execute the canonical grouped reducer/finalize path for every grouped shape
// that does not use a dedicated grouped fast path.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn execute_generic_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let grouped_specs = build_grouped_specs(route, grouped_execution_context)?;
    if matches!(
        route.grouped_execution_mode()?,
        crate::db::executor::route::GroupedExecutionMode::OrderedStreaming
    ) {
        return GenericGroupedFoldRunner::new(route, grouped_projection_spec).execute_ordered(
            stream,
            grouped_execution_context,
            OrderedGroupedAggregateFold::new(grouped_specs),
        );
    }

    let grouped_bundle = GroupedAggregateBundle::new(grouped_specs);

    GenericGroupedFoldRunner::new(route, grouped_projection_spec).execute(
        stream,
        grouped_execution_context,
        grouped_bundle,
    )
}
