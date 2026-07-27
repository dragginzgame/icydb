//! Module: executor::aggregate::runtime::grouped_fold::count::finalize
//! Responsibility: grouped `COUNT(*)` page finalization entrypoint.
//! Boundary: adapts count state rows into selected projected grouped rows.

use crate::{
    db::cursor::GroupedContinuationToken,
    db::executor::{
        RuntimeGroupedRow,
        aggregate::{
            ProjectionSpec,
            runtime::grouped_fold::{count::window::GroupedCountWindowSelection, metrics},
        },
        group::GroupKey,
        pipeline::contracts::GroupedRouteStage,
    },
    error::InternalError,
};

// Finalize grouped count buckets into grouped rows plus optional next cursor
// without routing the dedicated count path back through the generic candidate
// row envelope.
pub(super) fn finalize_grouped_count_page(
    route: &GroupedRouteStage,
    grouped_projection_spec: &ProjectionSpec,
    grouped_counts: Vec<(GroupKey, u32)>,
) -> Result<(Vec<RuntimeGroupedRow>, Option<GroupedContinuationToken>), InternalError> {
    metrics::record_finalize_stage(grouped_counts.len());
    let selection = GroupedCountWindowSelection::new(route)?;
    selection
        .select_page_rows(grouped_counts)?
        .project_and_build_cursor(route, grouped_projection_spec)
}
