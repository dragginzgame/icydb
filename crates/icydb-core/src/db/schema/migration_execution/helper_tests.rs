//! Test-only helpers for the parent module.

use super::*;

pub(in crate::db::schema) fn interrupt_next_migration_rewrite_at(
    interruption: MigrationRewriteInterruption,
) {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| next.set(Some(interruption)));
}

pub(super) fn take_migration_rewrite_interruption(
    interruption: MigrationRewriteInterruption,
) -> bool {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| {
        if next.get() == Some(interruption) {
            next.set(None);
            true
        } else {
            false
        }
    })
}

pub(super) fn migration_rewrite_row_limit() -> usize {
    MIGRATION_REWRITE_INTERRUPTION.with(|next| {
        if next.get().is_some() {
            1
        } else {
            MAX_MIGRATION_REWRITE_ROWS_PER_PAGE
        }
    })
}

thread_local! {
    pub(super) static MIGRATION_REWRITE_INTERRUPTION: std::cell::Cell<Option<MigrationRewriteInterruption>> =
        const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::db::schema) enum MigrationRewriteInterruption {
    MarkerPersisted,
    JournalPublished,
    PhysicalApplied,
}
