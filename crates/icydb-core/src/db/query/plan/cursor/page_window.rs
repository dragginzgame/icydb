///
/// PageWindow
///
/// Canonical pagination window sizing in usize-domain.
/// `keep_count` is `offset + limit`, and `fetch_count` adds one extra row when requested.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageWindow {
    pub(crate) fetch_count: usize,
    pub(crate) keep_count: usize,
}

/// Compute canonical page window counts from logical pagination inputs.
#[must_use]
pub(crate) fn compute_page_window(offset: u32, limit: u32, needs_extra: bool) -> PageWindow {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let keep_count = offset.saturating_add(limit);
    let fetch_count = keep_count.saturating_add(usize::from(needs_extra));

    PageWindow {
        fetch_count,
        keep_count,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PageWindow, compute_page_window};

    #[test]
    fn compute_page_window_zero_offset_zero_limit_without_extra() {
        let window = compute_page_window(0, 0, false);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 0,
                keep_count: 0,
            }
        );
    }

    #[test]
    fn compute_page_window_zero_offset_zero_limit_with_extra() {
        let window = compute_page_window(0, 0, true);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 1,
                keep_count: 0,
            }
        );
    }

    #[test]
    fn compute_page_window_zero_offset_limit_one() {
        let without_extra = compute_page_window(0, 1, false);
        let with_extra = compute_page_window(0, 1, true);

        assert_eq!(
            without_extra,
            PageWindow {
                fetch_count: 1,
                keep_count: 1,
            }
        );
        assert_eq!(
            with_extra,
            PageWindow {
                fetch_count: 2,
                keep_count: 1,
            }
        );
    }

    #[test]
    fn compute_page_window_offset_n_limit_one() {
        let window = compute_page_window(37, 1, true);
        assert_eq!(
            window,
            PageWindow {
                fetch_count: 39,
                keep_count: 38,
            }
        );
    }

    #[test]
    fn compute_page_window_high_bounds_and_needs_extra_toggle() {
        let base = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
        let expected_keep = base.saturating_add(base);

        let without_extra = compute_page_window(u32::MAX, u32::MAX, false);
        let with_extra = compute_page_window(u32::MAX, u32::MAX, true);

        assert_eq!(
            without_extra,
            PageWindow {
                fetch_count: expected_keep,
                keep_count: expected_keep,
            }
        );
        assert_eq!(with_extra.keep_count, without_extra.keep_count);
        assert_eq!(
            with_extra.fetch_count,
            without_extra.fetch_count.saturating_add(1)
        );
    }
}
