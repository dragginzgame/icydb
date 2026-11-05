//! Test-only helpers for the parent module.

pub(in crate::db) fn interrupt_next_mutation_commit_for_tests(
    interruption: MutationCommitInterruption,
) {
    NEXT_MUTATION_COMMIT_INTERRUPTION.with(|next| next.set(Some(interruption)));
}

pub(super) fn take_mutation_commit_interruption(interruption: MutationCommitInterruption) -> bool {
    NEXT_MUTATION_COMMIT_INTERRUPTION.with(|next| {
        if next.get() == Some(interruption) {
            next.set(None);
            true
        } else {
            false
        }
    })
}

/// Test-only durable interruption boundaries around mutation publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum MutationCommitInterruption {
    MarkerPersisted,
    JournalPublished,
    RowPrefixPublished,
    RowsPublished,
    StateMaterialized,
    ProgressReplaced,
}

thread_local! {
    pub(super) static NEXT_MUTATION_COMMIT_INTERRUPTION: std::cell::Cell<Option<MutationCommitInterruption>> =
        const { std::cell::Cell::new(None) };
}
