//! Module: executor::aggregate::runtime::grouped_fold::generic::runner
//! Responsibility: canonical grouped reducer path.
//! Boundary: owns generic grouped ingest and page finalization wiring.

use crate::{
    db::{
        executor::{
            aggregate::{
                ExecutionContext, GroupError, aggregate_materialized_fold_direction,
                runtime::grouped_fold::{
                    bundle::{GroupedAggregateBundle, GroupedAggregateBundleSpec},
                    dispatch::group_fields_support_borrowed_group_probe,
                    page_finalize::finalize_grouped_page,
                },
            },
            pipeline::{
                contracts::{GroupedCursorPage, GroupedRouteStage},
                runtime::{GroupedFoldStage, GroupedStreamStage},
            },
        },
        query::plan::{FieldSlot, expr::ProjectionSpec},
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
    group_fields: &'a [FieldSlot],
    borrowed_group_probe_supported: bool,
}

impl<'a> GenericGroupedFoldRunner<'a> {
    // Build one generic grouped fold runner from route-owned grouped policy.
    fn new(route: &'a GroupedRouteStage, grouped_projection_spec: &'a ProjectionSpec) -> Self {
        Self {
            route,
            grouped_projection_spec,
            group_fields: route.group_fields(),
            borrowed_group_probe_supported: group_fields_support_borrowed_group_probe(
                route.group_fields(),
            ),
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
        let (scanned_rows, filtered_rows) =
            self.fold_rows_into_bundle(stream, grouped_execution_context, &mut grouped_bundle)?;
        let (page_rows, next_cursor) = finalize_grouped_page(
            self.route,
            self.grouped_projection_spec,
            grouped_bundle,
            self.route.grouped_pagination_window(),
        )?;

        Ok(GroupedFoldStage::from_grouped_stream(
            GroupedCursorPage {
                rows: page_rows,
                next_cursor,
            },
            filtered_rows,
            true,
            stream,
            scanned_rows,
        ))
    }

    // Ingest grouped source rows into the shared grouped bundle while
    // preserving grouped budget contracts and borrowed grouped-key fast paths.
    fn fold_rows_into_bundle(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<(usize, usize), InternalError> {
        if self.borrowed_group_probe_supported {
            return self.fold_rows_into_bundle_borrowed(
                stream,
                grouped_execution_context,
                grouped_bundle,
            );
        }

        self.fold_rows_into_bundle_owned(stream, grouped_execution_context, grouped_bundle)
    }

    // Ingest grouped source rows with the borrowed existing-group probe path
    // selected once before the row loop.
    fn fold_rows_into_bundle_borrowed(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<(usize, usize), InternalError> {
        let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            // Phase 1: read and filter the source row before it reaches the
            // grouped aggregate states.
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            scanned_rows = scanned_rows.saturating_add(1);
            if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                && !row_view.eval_filter_program(effective_runtime_filter_program)?
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            // Phase 2: update through the allocation-free existing-group
            // probe path selected outside the row loop.
            grouped_bundle
                .ingest_row_with_borrowed_group_probe(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    self.group_fields,
                )
                .map_err(GroupError::into_internal_error)?;
        }

        Ok((scanned_rows, filtered_rows))
    }

    // Ingest grouped source rows with the owned group-key path selected once
    // before the row loop.
    fn fold_rows_into_bundle_owned(
        &self,
        stream: &mut GroupedStreamStage,
        grouped_execution_context: &mut ExecutionContext,
        grouped_bundle: &mut GroupedAggregateBundle,
    ) -> Result<(usize, usize), InternalError> {
        let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
        let effective_runtime_filter_program =
            execution_preparation.effective_runtime_filter_program();
        let mut scanned_rows = 0usize;
        let mut filtered_rows = 0usize;
        let consistency = self.route.consistency();

        while let Some(data_key) = resolved.key_stream_mut().next_key()? {
            // Phase 1: read and filter the source row before it reaches the
            // grouped aggregate states.
            let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
                continue;
            };
            scanned_rows = scanned_rows.saturating_add(1);
            if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
                && !row_view.eval_filter_program(effective_runtime_filter_program)?
            {
                continue;
            }
            filtered_rows = filtered_rows.saturating_add(1);

            // Phase 2: update through the owned canonical key path selected
            // outside the row loop.
            grouped_bundle
                .ingest_row_with_owned_group_key(
                    grouped_execution_context,
                    &data_key,
                    &row_view,
                    self.group_fields,
                )
                .map_err(GroupError::into_internal_error)?;
        }

        Ok((scanned_rows, filtered_rows))
    }
}

// Build the shared grouped aggregate bundle for canonical grouped terminal
// projection layout.
fn build_grouped_bundle(
    route: &GroupedRouteStage,
    grouped_execution_context: &ExecutionContext,
) -> Result<GroupedAggregateBundle, InternalError> {
    let grouped_specs = route
        .grouped_aggregate_execution_specs()
        .iter()
        .map(|aggregate_spec| {
            GroupedAggregateBundleSpec::new(
                aggregate_spec.kind(),
                aggregate_materialized_fold_direction(aggregate_spec.kind()),
                aggregate_spec.distinct(),
                aggregate_spec.target_slot().cloned(),
                aggregate_spec.compiled_input_expr().cloned(),
                aggregate_spec.compiled_filter_expr().cloned(),
                grouped_execution_context
                    .config()
                    .max_distinct_values_per_group(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(GroupedAggregateBundle::new(grouped_specs))
}

// Execute the canonical grouped reducer/finalize path for every grouped shape
// that does not use a dedicated grouped fast path.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn execute_generic_grouped_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    let grouped_bundle = build_grouped_bundle(route, grouped_execution_context)?;

    GenericGroupedFoldRunner::new(route, grouped_projection_spec).execute(
        stream,
        grouped_execution_context,
        grouped_bundle,
    )
}
