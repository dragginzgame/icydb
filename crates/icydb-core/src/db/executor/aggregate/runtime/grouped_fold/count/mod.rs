//! Module: executor::aggregate::runtime::grouped_fold::count
//! Responsibility: dedicated grouped `COUNT(*)` fold execution.
//! Boundary: owns count-state ingestion, windowing, and finalization.

mod finalize;
mod ingest;
mod state;
mod window;

use crate::{
    db::executor::{
        aggregate::{
            ExecutionContext,
            runtime::grouped_fold::{
                count::{finalize::finalize_grouped_count_page, state::GroupedCountState},
                dispatch::{GroupedCountKeyPath, GroupedCountProbeKind},
                metrics,
            },
        },
        pipeline::{
            contracts::GroupedRouteStage,
            contracts::ResolvedExecutionKeyStream,
            runtime::{GroupedFoldStage, GroupedStreamStage, RowView, StructuralGroupedRowRuntime},
        },
    },
    error::InternalError,
};

pub(in crate::db::executor::aggregate::runtime::grouped_fold) use ingest::materialize_group_key_from_row_view;
pub(super) use state::GroupedCountBucket;
#[cfg(test)]
use window::GroupedCountWindowSelection;

// Execute grouped `COUNT(*)` through a dedicated fold path that keeps only one
// canonical grouped-count map instead of the generic grouped reducer stack.
pub(super) fn execute_single_grouped_count_fold_stage(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage,
    grouped_execution_context: &mut ExecutionContext,
    grouped_projection_spec: &crate::db::query::plan::expr::ProjectionSpec,
) -> Result<GroupedFoldStage, InternalError> {
    metrics::record_fold_stage_run();
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let consistency = route.consistency();
    let key_path = GroupedCountKeyPath::for_route(route, effective_runtime_filter_program);
    let mut grouped_counts = GroupedCountState::new();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;

    // Phase 1: fold grouped source rows directly into one canonical count map.
    match key_path {
        GroupedCountKeyPath::DirectSingleField { group_field_index } => {
            while let Some(data_key) = resolved.key_stream_mut().next_key()? {
                let (row_materialization_local_instructions, group_value) =
                    metrics::measure(|| {
                        row_runtime.read_single_group_value(
                            consistency,
                            &data_key,
                            group_field_index,
                        )
                    });
                metrics::record_row_materialization(row_materialization_local_instructions);
                let Some(group_value) = group_value? else {
                    continue;
                };
                scanned_rows = scanned_rows.saturating_add(1);
                filtered_rows = filtered_rows.saturating_add(1);
                grouped_counts
                    .increment_single_group_value(group_value, grouped_execution_context)?;
            }
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Borrowed,
        } => {
            fold_row_view_count_rows(
                route,
                row_runtime,
                resolved,
                effective_runtime_filter_program,
                grouped_execution_context,
                &mut grouped_counts,
                (&mut scanned_rows, &mut filtered_rows),
                GroupedCountState::increment_row_borrowed_group_probe,
            )?;
        }
        GroupedCountKeyPath::RowView {
            probe_kind: GroupedCountProbeKind::Owned,
        } => {
            fold_row_view_count_rows(
                route,
                row_runtime,
                resolved,
                effective_runtime_filter_program,
                grouped_execution_context,
                &mut grouped_counts,
                (&mut scanned_rows, &mut filtered_rows),
                GroupedCountState::increment_row_owned_group_key,
            )?;
        }
    }

    // Phase 2: page and project the finalized grouped-count rows directly so
    // this dedicated path does not round-trip through the generic candidate
    // row envelope only to rebuild grouped rows immediately afterwards.
    let (page_rows, next_cursor) =
        finalize_grouped_count_page(route, grouped_projection_spec, grouped_counts.into_groups())?;

    Ok(GroupedFoldStage::from_grouped_stream(
        crate::db::executor::pipeline::contracts::GroupedCursorPage {
            rows: page_rows,
            next_cursor,
        },
        filtered_rows,
        true,
        stream,
        scanned_rows,
    ))
}

// Fold row-view grouped-count input through one statically selected ingest
// function so borrowed and owned paths are resolved before the source-row loop.
#[expect(
    clippy::too_many_arguments,
    reason = "the helper preserves the pre-existing hot-loop data flow while avoiding dynamic dispatch"
)]
fn fold_row_view_count_rows(
    route: &GroupedRouteStage,
    row_runtime: &StructuralGroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    effective_runtime_filter_program: Option<
        &crate::db::query::plan::EffectiveRuntimeFilterProgram,
    >,
    grouped_execution_context: &mut ExecutionContext,
    grouped_counts: &mut GroupedCountState,
    counters: (&mut usize, &mut usize),
    mut increment_row: impl FnMut(
        &mut GroupedCountState,
        &RowView,
        &[crate::db::query::plan::FieldSlot],
        &mut ExecutionContext,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    let consistency = route.consistency();
    let (scanned_rows, filtered_rows) = counters;

    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        let (row_materialization_local_instructions, row_view) =
            metrics::measure(|| row_runtime.read_row_view(consistency, &data_key));
        metrics::record_row_materialization(row_materialization_local_instructions);
        let Some(row_view) = row_view? else {
            continue;
        };
        *scanned_rows = scanned_rows.saturating_add(1);
        if let Some(effective_runtime_filter_program) = effective_runtime_filter_program
            && !row_view.eval_filter_program(effective_runtime_filter_program)?
        {
            continue;
        }
        *filtered_rows = filtered_rows.saturating_add(1);
        increment_row(
            grouped_counts,
            &row_view,
            route.group_fields(),
            grouped_execution_context,
        )?;
    }

    Ok(())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::{
                aggregate::{
                    ExecutionConfig, ExecutionContext,
                    runtime::grouped_fold::{
                        count::{GroupedCountState, GroupedCountWindowSelection},
                        utils::stable_hash_group_values_from_row_view,
                    },
                },
                pipeline::runtime::RowView,
            },
            query::plan::FieldSlot,
        },
        error::InternalError,
        types::Decimal,
        value::{Value, with_test_hash_override},
    };

    fn group_fields(indices: &[usize]) -> Vec<FieldSlot> {
        indices
            .iter()
            .map(|index| FieldSlot::from_parts_for_test(*index, format!("field_{index}")))
            .collect()
    }

    #[test]
    fn grouped_count_fast_path_hash_matches_owned_group_key_hash() {
        fn supports_group_probe(
            row_view: &RowView,
            group_fields: &[FieldSlot],
        ) -> Result<bool, InternalError> {
            fn group_value_supports_group_probe(value: &Value) -> bool {
                match value {
                    Value::List(_) | Value::Map(_) | Value::Unit => false,
                    Value::Enum(value_enum) => value_enum
                        .payload()
                        .is_none_or(group_value_supports_group_probe),
                    _ => true,
                }
            }

            for field in group_fields {
                let supports = row_view.with_required_slot(field.index(), |value| {
                    Ok(group_value_supports_group_probe(value))
                })?;
                if !supports {
                    return Ok(false);
                }
            }

            Ok(true)
        }

        let row_view = RowView::new(vec![
            Some(Value::Decimal(Decimal::new(100, 2))),
            Some(Value::Text("alpha".to_string())),
        ]);
        let group_fields = group_fields(&[0, 1]);

        assert!(
            supports_group_probe(&row_view, &group_fields).expect("borrowed probe"),
            "scalar grouped values should stay on the borrowed grouped-count fast path",
        );

        let borrowed_hash =
            stable_hash_group_values_from_row_view(&row_view, &group_fields).expect("hash");
        let owned_group_key = crate::db::executor::group::GroupKey::from_group_values(
            row_view.group_values(&group_fields).expect("group values"),
        )
        .expect("owned group key");

        assert_eq!(
            borrowed_hash,
            owned_group_key.hash(),
            "borrowed grouped-count hashing must stay aligned with owned canonical group-key hashing",
        );
    }

    #[test]
    fn grouped_count_fast_path_rejects_structured_group_values() {
        fn supports_group_probe(
            row_view: &RowView,
            group_fields: &[FieldSlot],
        ) -> Result<bool, InternalError> {
            fn group_value_supports_group_probe(value: &Value) -> bool {
                match value {
                    Value::List(_) | Value::Map(_) | Value::Unit => false,
                    Value::Enum(value_enum) => value_enum
                        .payload()
                        .is_none_or(group_value_supports_group_probe),
                    _ => true,
                }
            }

            for field in group_fields {
                let supports = row_view.with_required_slot(field.index(), |value| {
                    Ok(group_value_supports_group_probe(value))
                })?;
                if !supports {
                    return Ok(false);
                }
            }

            Ok(true)
        }

        let row_view = RowView::new(vec![Some(Value::List(vec![Value::Uint(7)]))]);
        let group_fields = group_fields(&[0]);

        assert!(
            !supports_group_probe(&row_view, &group_fields).expect("borrowed probe"),
            "structured grouped values must fall back to owned canonical key materialization",
        );
    }

    #[test]
    fn grouped_count_fast_path_handles_hash_collisions_without_merging_groups() {
        with_test_hash_override([0xAB; 16], || {
            let mut grouped_execution_context = ExecutionContext::new(ExecutionConfig::unbounded());
            let group_fields = group_fields(&[0]);
            let alpha = RowView::new(vec![Some(Value::Text("alpha".to_string()))]);
            let beta = RowView::new(vec![Some(Value::Text("beta".to_string()))]);
            let mut grouped_counts = GroupedCountState::new();

            grouped_counts
                .increment_row_borrowed_group_probe(
                    &alpha,
                    &group_fields,
                    &mut grouped_execution_context,
                )
                .expect("alpha insert");
            grouped_counts
                .increment_row_borrowed_group_probe(
                    &beta,
                    &group_fields,
                    &mut grouped_execution_context,
                )
                .expect("beta insert");
            grouped_counts
                .increment_row_borrowed_group_probe(
                    &alpha,
                    &group_fields,
                    &mut grouped_execution_context,
                )
                .expect("alpha increment");

            let mut rows = grouped_counts.into_groups();
            rows.sort_by(|(left_key, _), (right_key, _)| {
                crate::db::numeric::canonical_value_compare(
                    left_key.canonical_value(),
                    right_key.canonical_value(),
                )
            });
            assert_eq!(
                rows,
                vec![
                    (
                        crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                            "alpha".to_string(),
                        )])
                        .expect("alpha key"),
                        2,
                    ),
                    (
                        crate::db::executor::group::GroupKey::from_group_values(vec![Value::Text(
                            "beta".to_string(),
                        )])
                        .expect("beta key"),
                        1,
                    ),
                ],
                "same-hash grouped count rows must remain distinct under canonical grouped equality",
            );
        });
    }

    #[test]
    fn grouped_count_bounded_candidate_selection_keeps_smallest_canonical_window() {
        let rows = vec![
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(9)])
                    .expect("group key"),
                9,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(2)])
                    .expect("group key"),
                2,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(5)])
                    .expect("group key"),
                5,
            ),
            (
                crate::db::executor::group::GroupKey::from_group_values(vec![Value::Uint(1)])
                    .expect("group key"),
                1,
            ),
        ];

        let route = crate::db::executor::pipeline::contracts::GroupedRouteStage::new_for_test(
            crate::db::direction::Direction::Asc,
            Some(3),
        );
        let selected = GroupedCountWindowSelection::new(&route)
            .expect("grouped count window selection should compile")
            .retain_smallest_candidates(rows, 3);

        assert_eq!(
            selected
                .into_iter()
                .map(|(group_key, count)| (group_key.into_canonical_value(), count))
                .collect::<Vec<_>>(),
            vec![
                (Value::List(vec![Value::Uint(1)]), 1),
                (Value::List(vec![Value::Uint(2)]), 2),
                (Value::List(vec![Value::Uint(5)]), 5),
            ],
            "bounded grouped count selection should retain the smallest canonical grouped-key window only",
        );
    }
}
