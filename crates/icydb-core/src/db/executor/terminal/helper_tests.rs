//! Test-only helpers for the parent module.

pub(in crate::db::executor::terminal) const fn bytes_window_limit_exhausted(
    limit_remaining: Option<usize>,
) -> bool {
    matches!(limit_remaining, Some(0))
}

pub(in crate::db::executor::terminal) const fn bytes_window_accept_row(
    offset_remaining: &mut usize,
    limit_remaining: &mut Option<usize>,
) -> bool {
    if *offset_remaining > 0 {
        *offset_remaining = offset_remaining.saturating_sub(1);
        return false;
    }

    if let Some(remaining) = limit_remaining.as_mut() {
        if *remaining == 0 {
            return false;
        }
        *remaining = remaining.saturating_sub(1);
    }

    true
}
