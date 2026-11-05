use super::apply_compact_page_window;
use crate::{db::executor::ProjectionDataRows, error::InternalError};

#[test]
fn compact_page_windows_preserve_order_and_zero_offset_needs_no_swaps() {
    for total in 0..=6 {
        for offset in [0, 1, 3, 6, usize::MAX] {
            for limit in [None, Some(0), Some(1), Some(4), Some(u32::MAX)] {
                let mut rows = (0..total).collect::<Vec<_>>();
                let expected = rows
                    .iter()
                    .copied()
                    .skip(offset)
                    .take(limit.map_or(usize::MAX, |limit| usize::try_from(limit).unwrap()))
                    .collect::<Vec<_>>();
                let mut swaps = 0;
                apply_compact_page_window(
                    &mut rows,
                    total,
                    offset,
                    limit,
                    |rows, left, right| {
                        swaps += 1;
                        rows.swap(left, right);
                        Ok(())
                    },
                    Vec::truncate,
                )
                .unwrap();
                assert_eq!(rows, expected);
                if offset == 0 {
                    assert_eq!(swaps, 0);
                }
            }
        }
    }
}

#[test]
fn compact_page_window_propagates_data_row_swap_refusal_without_truncation() {
    let mut rows = ProjectionDataRows::read(Vec::new());
    let error = apply_compact_page_window(
        &mut rows,
        2,
        1,
        Some(1),
        ProjectionDataRows::swap,
        |_, _| panic!("failed compaction must not truncate"),
    )
    .unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::query_executor_invariant().diagnostic_code()
    );
}
