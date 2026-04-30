//! Module: executor::aggregate::runtime::grouped_fold::utils::equality
//! Responsibility: grouped bucket equality probes.
//! Boundary: centralizes canonical grouped-key comparison without duplicating lookup logic.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            aggregate::runtime::grouped_fold::{metrics, utils::GroupIndexBucket},
            group::GroupKey,
            pipeline::runtime::RowView,
        },
        numeric::canonical_value_compare,
        query::plan::FieldSlot,
    },
    error::InternalError,
    value::Value,
};

// Return true when one canonical grouped key matches one direct single grouped
// value under the grouped-count single-field identity-canonical fast path.
fn single_group_key_matches_value(
    group_key: &GroupKey,
    group_value: &Value,
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = group_key.canonical_value() else {
        return Err(InternalError::query_executor_invariant(
            "grouped count key must remain a canonical Value::List".to_string(),
        ));
    };
    let [canonical_group_value] = canonical_group_values.as_slice() else {
        return Err(InternalError::query_executor_invariant(format!(
            "single-field grouped count key must retain exactly one canonical value: len={}",
            canonical_group_values.len(),
        )));
    };

    Ok(canonical_value_compare(group_value, canonical_group_value) == Ordering::Equal)
}

// Return true when one canonical grouped key value matches this row's grouped
// slot values under the borrowed grouped-key equality contract.
fn canonical_group_value_matches_row_view_with_context(
    canonical_group_value: &Value,
    row_view: &RowView,
    group_fields: &[FieldSlot],
    context: &'static str,
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = canonical_group_value else {
        return Err(InternalError::query_executor_invariant(format!(
            "{context} key must remain a canonical Value::List"
        )));
    };
    if canonical_group_values.len() != group_fields.len() {
        return Err(InternalError::query_executor_invariant(format!(
            "{context} key field count drifted from route group fields: key_len={} group_fields_len={}",
            canonical_group_values.len(),
            group_fields.len(),
        )));
    }

    for (field, canonical_group_value) in group_fields.iter().zip(canonical_group_values) {
        let matches = row_view.with_required_slot(field.index(), |value| {
            Ok(canonical_value_compare(value, canonical_group_value) == Ordering::Equal)
        })?;
        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

// Return true when one canonical grouped aggregate key matches this row's
// grouped slot values.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn group_key_matches_row_view(
    group_key: &GroupKey,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    canonical_group_value_matches_row_view_with_context(
        group_key.canonical_value(),
        row_view,
        group_fields,
        "grouped aggregate",
    )
}

// Search one stable-hash bucket slice for a matching group key without owning
// the caller's bucket storage. The caller supplies group-key access and metric
// hooks so COUNT and generic grouped bundles keep their local state shapes.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_group_index_in_bucket<
    'a,
>(
    bucket_indexes: &[usize],
    group_count: usize,
    mut group_key_at: impl FnMut(usize) -> Option<&'a GroupKey>,
    mut matches_group: impl FnMut(&GroupKey) -> Result<bool, InternalError>,
    mut record_candidate: impl FnMut(),
    missing_group_error: impl Fn(usize, usize) -> InternalError,
) -> Result<Option<usize>, InternalError> {
    for group_index in bucket_indexes.iter().copied() {
        record_candidate();
        let Some(group_key) = group_key_at(group_index) else {
            return Err(missing_group_error(group_index, group_count));
        };
        if matches_group(group_key)? {
            return Ok(Some(group_index));
        }
    }

    Ok(None)
}

// Search one stable-hash bucket for an existing grouped count entry using one
// caller-supplied grouped-key equality probe.
fn find_matching_group_in_bucket(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupIndexBucket>,
    matches_group: impl FnMut(&GroupKey) -> Result<bool, InternalError>,
) -> Result<Option<usize>, InternalError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    find_matching_group_index_in_bucket(
        bucket.as_slice(),
        grouped_counts.len(),
        |group_index| {
            grouped_counts
                .get(group_index)
                .map(|(group_key, _)| group_key)
        },
        matches_group,
        metrics::record_bucket_candidate_check,
        |group_index, group_count| {
            InternalError::query_executor_invariant(format!(
                "grouped count bucket index out of bounds: index={group_index} len={group_count}",
            ))
        },
    )
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches the current borrowed grouped slot values.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_group_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupIndexBucket>,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<Option<usize>, InternalError> {
    find_matching_group_in_bucket(grouped_counts, bucket, |group_key| {
        canonical_group_value_matches_row_view_with_context(
            group_key.canonical_value(),
            row_view,
            group_fields,
            "grouped count",
        )
    })
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches one direct single grouped value.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_single_group_value_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupIndexBucket>,
    group_value: &Value,
) -> Result<Option<usize>, InternalError> {
    find_matching_group_in_bucket(grouped_counts, bucket, |group_key| {
        single_group_key_matches_value(group_key, group_value)
    })
}
