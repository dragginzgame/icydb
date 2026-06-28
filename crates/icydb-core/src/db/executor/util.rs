//! Module: executor::util
//! Responsibility: tiny helpers shared by executor runtime and executor-local tests.
//! Does not own: execution semantics, routing, or plan validation.

use crate::db::{data::DecodedDataStoreKey, direction::Direction};

/// Apply one offset/limit window to an already ordered in-memory row set.
///
/// This helper owns only the vector slicing mechanics. Callers remain
/// responsible for deciding whether paging, projection, or delete semantics
/// allow this window to run at their phase boundary.
pub(in crate::db::executor) fn apply_offset_limit_window<T>(
    rows: &mut Vec<T>,
    offset: u32,
    limit: Option<u32>,
) {
    let offset = usize::min(rows.len(), usize::try_from(offset).unwrap_or(usize::MAX));
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::min(rows.len(), usize::try_from(limit).unwrap_or(usize::MAX));
        rows.truncate(limit);
    }
}

/// Sort rows by decoded data-store key, suppress duplicate keys, then apply a
/// direction and limit window.
///
/// This helper owns only the materialized row mechanics. Callers remain
/// responsible for proving that full materialization is the correct execution
/// boundary before using it.
pub(in crate::db::executor) fn apply_data_key_ordered_dedup_window<T>(
    rows: &mut Vec<T>,
    direction: Direction,
    limit: usize,
    key: impl Fn(&T) -> &DecodedDataStoreKey,
) {
    rows.sort_by(|left, right| key(left).cmp(key(right)));
    rows.dedup_by(|left, right| key(left) == key(right));
    if matches!(direction, Direction::Desc) {
        rows.reverse();
    }
    if limit != usize::MAX {
        rows.truncate(limit);
    }
}

/// Convert one row-count length into `u32` using saturating semantics.
#[must_use]
pub(in crate::db::executor) fn saturating_u32_len(row_len: usize) -> u32 {
    u32::try_from(row_len).unwrap_or(u32::MAX)
}

/// Convert one byte-length value into `u64` using saturating semantics.
///
/// This helper exists to keep numeric-clamp behavior consistent between runtime
/// terminal folds and executor-owned expected-value helpers in tests.
#[must_use]
pub(in crate::db::executor) fn saturating_row_len(row_len: usize) -> u64 {
    u64::try_from(row_len).unwrap_or(u64::MAX)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::data::PrimaryKeyComponent, types::EntityTag};

    fn data_key(value: u64) -> DecodedDataStoreKey {
        let raw = DecodedDataStoreKey::new(
            EntityTag::new(41),
            &PrimaryKeyComponent::Nat64(value).into(),
        )
        .to_raw()
        .expect("test key encoding should succeed");

        DecodedDataStoreKey::try_from_raw(&raw).expect("test key decode should succeed")
    }

    #[test]
    fn saturating_row_len_returns_exact_value_within_u64_range() {
        assert_eq!(saturating_row_len(42), 42);
    }

    #[test]
    fn saturating_row_len_saturates_at_u64_max() {
        assert_eq!(
            saturating_row_len(usize::MAX),
            u64::try_from(usize::MAX).unwrap_or(u64::MAX)
        );
    }

    #[test]
    fn apply_data_key_ordered_dedup_window_orders_dedups_and_limits_rows() {
        let mut rows = vec![
            (data_key(4), "four"),
            (data_key(2), "two-first"),
            (data_key(3), "three"),
            (data_key(2), "two-duplicate"),
            (data_key(1), "one"),
        ];

        apply_data_key_ordered_dedup_window(&mut rows, Direction::Asc, 3, |row| &row.0);

        assert_eq!(
            rows.into_iter().map(|(_, label)| label).collect::<Vec<_>>(),
            vec!["one", "two-first", "three"],
            "helper should sort ascending, keep one row per key, and apply limit",
        );
    }

    #[test]
    fn apply_data_key_ordered_dedup_window_reverses_after_dedup_for_desc() {
        let mut rows = vec![
            (data_key(1), "one"),
            (data_key(2), "two-first"),
            (data_key(2), "two-duplicate"),
            (data_key(3), "three"),
        ];

        apply_data_key_ordered_dedup_window(&mut rows, Direction::Desc, usize::MAX, |row| &row.0);

        assert_eq!(
            rows.into_iter().map(|(_, label)| label).collect::<Vec<_>>(),
            vec!["three", "two-first", "one"],
            "helper should dedup in canonical key order before applying descending output order",
        );
    }
}
