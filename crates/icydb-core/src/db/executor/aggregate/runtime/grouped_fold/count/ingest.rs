//! Module: executor::aggregate::runtime::grouped_fold::count::ingest
//! Responsibility: grouped `COUNT(*)` row ingestion.
//! Boundary: preserves borrowed and owned ingest paths without route dispatch.

use crate::{
    db::executor::{
        aggregate::{
            EffectiveRuntimeFilterProgram, ExecutionContext, FieldSlot,
            capability::accepted_field_kind_has_identity_group_canonical_form,
            runtime::grouped_fold::{
                count::GroupedCountState,
                metrics,
                utils::{
                    GroupIndexBucket, find_matching_group_index,
                    find_matching_single_group_value_index, stable_hash_group_values_from_row_view,
                    stable_hash_single_group_value,
                },
            },
        },
        group::{GroupKey, StableHash, StableHashMap},
        pipeline::contracts::{GroupedRouteStage, ResolvedExecutionKeyStream},
        pipeline::runtime::{RowView, StructuralGroupedRowRuntime},
    },
    error::InternalError,
    value::Value,
};

// Fold row-view grouped-count input through one statically selected ingest
// function so borrowed and owned paths are resolved before the source-row loop.
#[expect(
    clippy::too_many_arguments,
    reason = "the helper preserves the pre-existing hot-loop data flow while avoiding dynamic dispatch"
)]
pub(super) fn fold_row_view_count_rows(
    route: &GroupedRouteStage,
    row_runtime: &StructuralGroupedRowRuntime,
    resolved: &mut ResolvedExecutionKeyStream,
    effective_runtime_filter_program: Option<&EffectiveRuntimeFilterProgram>,
    grouped_execution_context: &mut ExecutionContext,
    grouped_counts: &mut GroupedCountState,
    counters: (&mut usize, &mut usize),
    mut increment_row: impl FnMut(
        &mut GroupedCountState,
        &RowView,
        &[FieldSlot],
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

impl GroupedCountState {
    // Increment one grouped count row through the borrowed row-view probe path.
    pub(super) fn increment_row_borrowed_group_probe(
        &mut self,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        metrics::record_rows_folded();

        self.increment_borrowed_group_probe(
            "bucket-indexed",
            grouped_execution_context,
            |grouped_counts, bucket_index| {
                let group_hash = stable_hash_group_values_from_row_view(row_view, group_fields)?;
                let existing_index = find_matching_group_index(
                    grouped_counts,
                    bucket_index.get(&group_hash),
                    row_view,
                    group_fields,
                )?;

                Ok((group_hash, existing_index))
            },
            |group_hash| {
                let group_key =
                    materialize_group_key_from_row_view(row_view, group_fields, Some(group_hash))?;
                debug_assert_eq!(
                    group_key.hash(),
                    group_hash,
                    "borrowed grouped key hash must match owned canonical group key hash",
                );

                Ok(group_key)
            },
        )
    }

    // Increment one grouped count row through the owned row-view fallback path.
    pub(super) fn increment_row_owned_group_key(
        &mut self,
        row_view: &RowView,
        group_fields: &[FieldSlot],
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        metrics::record_rows_folded();
        let group_key = materialize_group_key_from_row_view(row_view, group_fields, None)?;

        self.increment_owned_group_key(group_key, grouped_execution_context)
    }

    // Increment one grouped count row from one direct single grouped value
    // when the grouped route already proves the single-field identity-canonical
    // fast path is valid.
    pub(super) fn increment_single_group_value(
        &mut self,
        group_value: Value,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        metrics::record_rows_folded();
        metrics::record_borrowed_probe_row();

        // Phase 1: hash and probe the owned value by reference before the
        // insertion path consumes it. This keeps the existing-group path clone
        // free while preserving the same bucketed lookup accounting.
        let (lookup_local_instructions, lookup): (
            u64,
            Result<(StableHash, Option<usize>), InternalError>,
        ) = metrics::measure(|| {
            metrics::record_borrowed_hash_computation();
            let group_hash = stable_hash_single_group_value(&group_value)?;
            let existing_index = find_matching_single_group_value_index(
                self.groups.as_slice(),
                self.bucket_index.get(&group_hash),
                &group_value,
            )?;

            Ok((group_hash, existing_index))
        });
        metrics::record_lookup(lookup_local_instructions);

        // Phase 2: only move the owned grouped value into a canonical key when
        // the borrowed probe proved this row opens a new group.
        self.complete_group_lookup(
            "bucket-indexed direct",
            grouped_execution_context,
            lookup?,
            |group_hash| {
                metrics::record_owned_key_materialization();
                Ok(GroupKey::from_single_canonical_group_value_with_hash(
                    group_value,
                    group_hash,
                ))
            },
        )
    }

    // Resolve one borrowed grouped-count probe through the shared
    // hash/lookup/update-or-insert contract used by both row-view and direct
    // single-value grouped key ingestion.
    fn increment_borrowed_group_probe(
        &mut self,
        existing_group_source: &'static str,
        grouped_execution_context: &mut ExecutionContext,
        lookup_existing_group: impl FnOnce(
            &[(GroupKey, u32)],
            &StableHashMap<GroupIndexBucket>,
        )
            -> Result<(StableHash, Option<usize>), InternalError>,
        materialize_new_group: impl FnOnce(StableHash) -> Result<GroupKey, InternalError>,
    ) -> Result<(), InternalError> {
        metrics::record_borrowed_probe_row();

        // Phase 1: keep the existing-group path on borrowed hashing and
        // bucket probing only, regardless of which grouped key surface
        // supplied the equality contract.
        let (lookup_local_instructions, lookup) = metrics::measure(|| {
            metrics::record_borrowed_hash_computation();
            lookup_existing_group(self.groups.as_slice(), &self.bucket_index)
        });
        metrics::record_lookup(lookup_local_instructions);
        self.complete_group_lookup(
            existing_group_source,
            grouped_execution_context,
            lookup?,
            materialize_new_group,
        )
    }

    // Complete one grouped-count lookup result under the shared
    // existing-group hit vs new-group insert contract used after both
    // borrowed probes and owned-key fallback lookups.
    fn complete_group_lookup(
        &mut self,
        existing_group_source: &'static str,
        grouped_execution_context: &mut ExecutionContext,
        lookup: (StableHash, Option<usize>),
        materialize_new_group: impl FnOnce(StableHash) -> Result<GroupKey, InternalError>,
    ) -> Result<(), InternalError> {
        let (group_hash, existing_index) = lookup;

        if let Some(existing_index) = existing_index {
            self.measure_existing_group_increment(existing_index, existing_group_source)?;

            return Ok(());
        }

        // Only materialize or forward one owned grouped key after lookup has
        // proven this row opens a genuinely new canonical group.
        let group_key = materialize_new_group(group_hash)?;

        self.measure_new_group_insert(group_hash, group_key, grouped_execution_context)
    }

    // Increment one grouped count row from an already-owned canonical key.
    fn increment_owned_group_key(
        &mut self,
        group_key: GroupKey,
        grouped_execution_context: &mut ExecutionContext,
    ) -> Result<(), InternalError> {
        metrics::record_owned_group_fallback_row();

        // Phase 1: reuse the stable-hash side index so owned-key fallback rows
        // still avoid a full scan across every grouped count entry.
        let group_hash = group_key.hash();
        let (lookup_local_instructions, existing_index) = metrics::measure(|| {
            if let Some(bucket) = self.bucket_index.get(&group_hash) {
                for existing_index in bucket.as_slice().iter().copied() {
                    metrics::record_bucket_candidate_check();
                    if self
                        .groups
                        .get(existing_index)
                        .is_some_and(|(existing, _)| existing == &group_key)
                    {
                        return Some(existing_index);
                    }
                }
            }

            None
        });
        metrics::record_lookup(lookup_local_instructions);
        self.complete_group_lookup(
            "owned-key bucket-indexed",
            grouped_execution_context,
            (group_hash, existing_index),
            |_| Ok(group_key),
        )
    }
}

// Materialize one canonical grouped key from row slots when borrowed probing
// cannot satisfy the ingest path or the row opens a genuinely new group.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn materialize_group_key_from_row_view(
    row_view: &RowView,
    group_fields: &[FieldSlot],
    precomputed_hash: Option<StableHash>,
) -> Result<GroupKey, InternalError> {
    metrics::record_owned_key_materialization();

    if let [field] = group_fields {
        let group_value = row_view.require_slot_owned(field.index())?;
        let identity_canonical_form = field
            .accepted_kind()
            .is_some_and(accepted_field_kind_has_identity_group_canonical_form);

        return match (identity_canonical_form, precomputed_hash) {
            (true, Some(hash)) => Ok(GroupKey::from_single_canonical_group_value_with_hash(
                group_value,
                hash,
            )),
            (true, None) => GroupKey::from_single_canonical_group_value(group_value),
            (false, Some(hash)) => GroupKey::from_single_group_value_with_hash(group_value, hash),
            (false, None) => GroupKey::from_single_group_value(group_value),
        }
        .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error);
    }

    let group_values = row_view.group_values(group_fields)?;
    match precomputed_hash {
        Some(hash) => GroupKey::from_group_values_with_hash(group_values, hash),
        None => GroupKey::from_group_values(group_values),
    }
    .map_err(crate::db::executor::group::KeyCanonicalError::into_internal_error)
}
