//! Module: executor::util
//! Responsibility: tiny numeric helpers shared by executor runtime and executor-local tests.
//! Does not own: execution semantics, routing, or plan validation.

/// Convert one byte-length value into `u64` using saturating semantics.
///
/// This helper exists to keep numeric-clamp behavior consistent between runtime
/// terminal folds and executor-owned expected-value helpers in tests.
#[must_use]
#[expect(clippy::cast_possible_truncation)]
pub(in crate::db::executor) const fn saturating_row_len(row_len: usize) -> u64 {
    if row_len > u64::MAX as usize {
        u64::MAX
    } else {
        row_len as u64
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturating_row_len_returns_exact_value_within_u64_range() {
        assert_eq!(saturating_row_len(42), 42);
    }

    #[test]
    fn saturating_row_len_saturates_at_u64_max() {
        assert_eq!(saturating_row_len(usize::MAX), u64::MAX);
    }
}
