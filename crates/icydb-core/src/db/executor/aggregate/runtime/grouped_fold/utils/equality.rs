//! Module: executor::aggregate::runtime::grouped_fold::utils::equality
//! Responsibility: grouped-count bucket equality probes.
//! Boundary: centralizes canonical grouped-key comparison without duplicating lookup logic.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            aggregate::runtime::grouped_fold::{count::GroupedCountBucket, metrics},
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

// Return true when one canonical grouped key matches this row's grouped slot
// values under the borrowed grouped-count fast-path equality contract.
fn canonical_group_value_matches_row_view(
    canonical_group_value: &Value,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = canonical_group_value else {
        return Err(InternalError::query_executor_invariant(
            "grouped count key must remain a canonical Value::List".to_string(),
        ));
    };
    if canonical_group_values.len() != group_fields.len() {
        return Err(InternalError::query_executor_invariant(format!(
            "grouped count key field count drifted from route group fields: key_len={} group_fields_len={}",
            canonical_group_values.len(),
            group_fields.len(),
        )));
    }

    for (field, canonical_group_value) in group_fields.iter().zip(canonical_group_values) {
        if canonical_value_compare(
            row_view.require_slot_ref(field.index())?,
            canonical_group_value,
        ) != Ordering::Equal
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn group_key_matches_row_view(
    group_key: &GroupKey,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    canonical_group_value_matches_row_view(group_key.canonical_value(), row_view, group_fields)
}

// Search one stable-hash bucket for an existing grouped count entry using one
// caller-supplied grouped-key equality probe.
fn find_matching_group_in_bucket(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupedCountBucket>,
    mut matches_group: impl FnMut(&GroupKey) -> Result<bool, InternalError>,
) -> Result<Option<usize>, InternalError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    for group_index in bucket.as_slice() {
        metrics::record_bucket_candidate_check();
        let Some((group_key, _)) = grouped_counts.get(*group_index) else {
            return Err(InternalError::query_executor_invariant(format!(
                "grouped count bucket index out of bounds: index={} len={}",
                group_index,
                grouped_counts.len(),
            )));
        };
        if matches_group(group_key)? {
            return Ok(Some(*group_index));
        }
    }

    Ok(None)
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches the current borrowed grouped slot values.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_group_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupedCountBucket>,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<Option<usize>, InternalError> {
    find_matching_group_in_bucket(grouped_counts, bucket, |group_key| {
        group_key_matches_row_view(group_key, row_view, group_fields)
    })
}

// Search one stable-hash bucket for an existing grouped count entry that
// matches one direct single grouped value.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_single_group_value_index(
    grouped_counts: &[(GroupKey, u32)],
    bucket: Option<&GroupedCountBucket>,
    group_value: &Value,
) -> Result<Option<usize>, InternalError> {
    find_matching_group_in_bucket(grouped_counts, bucket, |group_key| {
        single_group_key_matches_value(group_key, group_value)
    })
}
