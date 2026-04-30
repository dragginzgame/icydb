//! Module: executor::aggregate::runtime::grouped_fold::utils::sizing
//! Responsibility: grouped fold table sizing helpers.
//! Boundary: converts stream and budget metadata into conservative capacities.

// Return a conservative group-table capacity from a source-row candidate hint.
// A grouped fold cannot create more groups than candidate rows or `max_groups`,
// so this avoids repeated growth for exact materialized streams without
// preallocating when the stream has no cheap count.
pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn group_capacity_hint(
    candidate_count_hint: Option<usize>,
    max_groups: u64,
) -> usize {
    let Some(candidate_count_hint) = candidate_count_hint else {
        return 0;
    };
    let max_groups = usize::try_from(max_groups).unwrap_or(usize::MAX);

    candidate_count_hint.min(max_groups)
}
