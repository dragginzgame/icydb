//! Module: executor::aggregate::runtime::grouped_fold::utils::equality
//! Responsibility: grouped bucket equality probes.
//! Boundary: centralizes canonical grouped-key comparison without duplicating lookup logic.

use std::cmp::Ordering;

use crate::{
    db::{
        executor::{
            aggregate::{FieldSlot, runtime::grouped_fold::utils::GroupIndexBucket},
            group::GroupKey,
            pipeline::runtime::RowView,
        },
        numeric::canonical_value_compare,
    },
    error::InternalError,
    value::Value,
};

// Return true when one canonical grouped key matches one direct single grouped
// value under the grouped-count single-field identity-canonical fast path.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn group_key_matches_single_group_value(
    group_key: &GroupKey,
    group_value: &Value,
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = group_key.canonical_value() else {
        return Err(InternalError::query_executor_invariant());
    };
    let [canonical_group_value] = canonical_group_values.as_slice() else {
        return Err(InternalError::query_executor_invariant());
    };

    Ok(canonical_value_compare(group_value, canonical_group_value) == Ordering::Equal)
}

// Return true when one canonical grouped key value matches this row's grouped
// slot values under the borrowed grouped-key equality contract.
fn canonical_group_value_matches_row_view(
    canonical_group_value: &Value,
    row_view: &RowView,
    group_fields: &[FieldSlot],
) -> Result<bool, InternalError> {
    let Value::List(canonical_group_values) = canonical_group_value else {
        return Err(InternalError::query_executor_invariant());
    };
    if canonical_group_values.len() != group_fields.len() {
        return Err(InternalError::query_executor_invariant());
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
    canonical_group_value_matches_row_view(group_key.canonical_value(), row_view, group_fields)
}

// Search one stable-hash bucket slice for a matching group key without owning
// the caller's bucket storage.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn find_matching_group_index_in_bucket<
    'a,
>(
    bucket_indexes: &[usize],
    mut group_key_at: impl FnMut(usize) -> Option<&'a GroupKey>,
    mut matches_group: impl FnMut(&GroupKey) -> Result<bool, InternalError>,
) -> Result<Option<usize>, InternalError> {
    for group_index in bucket_indexes.iter().copied() {
        let Some(group_key) = group_key_at(group_index) else {
            return Err(InternalError::query_executor_invariant());
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
        |group_index| {
            grouped_counts
                .get(group_index)
                .map(|(group_key, _)| group_key)
        },
        matches_group,
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
        canonical_group_value_matches_row_view(group_key.canonical_value(), row_view, group_fields)
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
        group_key_matches_single_group_value(group_key, group_value)
    })
}

#[cfg(test)]
mod tests {
    use super::{find_matching_group_index_in_bucket, group_key_matches_single_group_value};
    use crate::{db::executor::group::GroupKey, error::InternalError, value::Value};

    #[test]
    fn bucket_lookup_preserves_matching_absence_and_typed_failure() {
        let keys = [1, 2]
            .map(|value| GroupKey::from_single_canonical_group_value(Value::Nat64(value)).unwrap());
        for (bucket, target, expected) in [
            (&[0, 1][..], 2, Some(1)),
            (&[1, 0][..], 1, Some(0)),
            (&[0, 1][..], 3, None),
            (&[][..], 1, None),
        ] {
            let found = find_matching_group_index_in_bucket(
                bucket,
                |index| keys.get(index),
                |key| group_key_matches_single_group_value(key, &Value::Nat64(target)),
            )
            .unwrap();
            assert_eq!(found, expected);
        }

        let missing = find_matching_group_index_in_bucket(
            &[2],
            |index| keys.get(index),
            |_| unreachable!("missing keys must reject before comparison"),
        )
        .unwrap_err();
        assert_eq!(
            missing.diagnostic(),
            InternalError::query_executor_invariant().diagnostic()
        );
        let comparison_error = InternalError::planner_executor_invariant();
        let expected = comparison_error.diagnostic();
        let error = find_matching_group_index_in_bucket(
            &[0],
            |index| keys.get(index),
            |_| Err(InternalError::planner_executor_invariant()),
        )
        .unwrap_err();
        assert_eq!(error.diagnostic(), expected);
    }
}
