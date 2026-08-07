//! Module: executor::stream::key::seek
//! Responsibility: monotonic held-head positioning and its repeated-pull reference adapter.
//! Does not own: physical range jumps, page cursor encoding, or planner eligibility.
//! Boundary: defines the protocol physical ordered streams must implement before seek use.

#[cfg(test)]
use crate::db::executor::stream::key::{KeyOrderComparator, OrderedKeyStream};
use crate::{db::data::DecodedDataStoreKey, error::InternalError};

/// Result of ensuring or seeking one ordered stream head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum HeldHeadSeekOutcome<'a> {
    /// The first qualifying physical occurrence is held and remains unconsumed.
    Held(&'a DecodedDataStoreKey),
    /// The stream proved that no physical occurrences remain.
    Exhausted,
    /// The page envelope stopped before the next indivisible pull unit.
    PageStop,
}

/// Page-local logical work performed by held-head positioning.
///
/// Pull attempts include the final exhaustion probe. Physical streams add
/// their own path, storage, decode, and hard-budget charges in Patch 3; this
/// reference authority must never claim those operations were skipped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct HeldHeadSeekWork {
    pull_attempt_limit: u64,
    pull_attempts: u64,
    comparisons: u64,
    skipped_occurrences: u64,
    consumed_occurrences: u64,
    physical_seeks: u64,
    reposition_bound_bytes: u64,
}

impl HeldHeadSeekWork {
    /// Construct work with a finite admitted pull-attempt envelope.
    #[must_use]
    pub(in crate::db::executor) const fn with_pull_attempt_limit(pull_attempt_limit: u64) -> Self {
        Self {
            pull_attempt_limit,
            pull_attempts: 0,
            comparisons: 0,
            skipped_occurrences: 0,
            consumed_occurrences: 0,
            physical_seeks: 0,
            reposition_bound_bytes: 0,
        }
    }

    /// Construct work whose reference-adapter pull count is effectively unbounded.
    #[must_use]
    pub(in crate::db::executor) const fn unbounded() -> Self {
        Self::with_pull_attempt_limit(u64::MAX)
    }

    #[cfg(test)]
    pub(in crate::db::executor) const fn with_observed_for_tests(
        pull_attempt_limit: u64,
        pull_attempts: u64,
        comparisons: u64,
        skipped_occurrences: u64,
        consumed_occurrences: u64,
    ) -> Self {
        Self {
            pull_attempt_limit,
            pull_attempts,
            comparisons,
            skipped_occurrences,
            consumed_occurrences,
            physical_seeks: 0,
            reposition_bound_bytes: 0,
        }
    }

    /// Return attempted upstream pulls, including an exhaustion probe or failed pull.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn pull_attempts(self) -> u64 {
        self.pull_attempts
    }

    /// Return comparator invocations owned by the held-head protocol.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn comparisons(self) -> u64 {
        self.comparisons
    }

    /// Return physical occurrences discarded strictly before seek targets.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn skipped_occurrences(self) -> u64 {
        self.skipped_occurrences
    }

    /// Return physical occurrences removed from held state, including skips.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn consumed_occurrences(self) -> u64 {
        self.consumed_occurrences
    }

    /// Return physical range reposition operations performed by capable leaves.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn physical_seeks(self) -> u64 {
        self.physical_seeks
    }

    /// Return encoded target-bound bytes constructed for physical repositioning.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db::executor) const fn reposition_bound_bytes(self) -> u64 {
        self.reposition_bound_bytes
    }

    pub(in crate::db::executor) const fn admits_pull(self) -> bool {
        self.pull_attempts < self.pull_attempt_limit
    }

    pub(in crate::db::executor) fn record_pull_attempt(&mut self) -> Result<(), InternalError> {
        self.pull_attempts = self
            .pull_attempts
            .checked_add(1)
            .ok_or_else(InternalError::executor_invariant)?;
        Ok(())
    }

    pub(in crate::db::executor) fn record_comparison(&mut self) -> Result<(), InternalError> {
        self.comparisons = self
            .comparisons
            .checked_add(1)
            .ok_or_else(InternalError::executor_invariant)?;
        Ok(())
    }

    pub(in crate::db::executor) fn record_consumed(&mut self) -> Result<(), InternalError> {
        self.consumed_occurrences = self
            .consumed_occurrences
            .checked_add(1)
            .ok_or_else(InternalError::executor_invariant)?;
        Ok(())
    }

    pub(in crate::db::executor) fn record_skipped_consumptions(
        &mut self,
        count: u64,
    ) -> Result<(), InternalError> {
        let skipped_occurrences = self
            .skipped_occurrences
            .checked_add(count)
            .ok_or_else(InternalError::executor_invariant)?;
        let consumed_occurrences = self
            .consumed_occurrences
            .checked_add(count)
            .ok_or_else(InternalError::executor_invariant)?;
        self.skipped_occurrences = skipped_occurrences;
        self.consumed_occurrences = consumed_occurrences;
        Ok(())
    }

    pub(in crate::db::executor) fn record_physical_seek(
        &mut self,
        bound_bytes: u64,
    ) -> Result<(), InternalError> {
        let physical_seeks = self
            .physical_seeks
            .checked_add(1)
            .ok_or_else(InternalError::executor_invariant)?;
        let reposition_bound_bytes = self
            .reposition_bound_bytes
            .checked_add(bound_bytes)
            .ok_or_else(InternalError::executor_invariant)?;
        self.physical_seeks = physical_seeks;
        self.reposition_bound_bytes = reposition_bound_bytes;
        Ok(())
    }
}

/// Monotonic ordered-stream protocol that separates positioning from consumption.
pub(in crate::db::executor) trait HeldHeadKeyStream {
    /// Ensure one unconsumed head without moving an existing held occurrence.
    // Patch 3's physical adapter enters through `seek_head_at_or_after`; Patch 4
    // consumes this zero-target positioning operation while aligning children.
    #[allow(dead_code)]
    fn ensure_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError>;

    /// Hold the first occurrence not before `target` in traversal order.
    fn seek_head_at_or_after(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError>;

    /// Remove and return exactly one held physical occurrence.
    fn consume_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(test)]
enum EnsureHeadState {
    Held,
    Exhausted,
    PageStop,
}

/// Repeated-pull reference implementation for the held-head protocol.
///
/// This adapter is the semantic oracle for later physical seek implementations.
/// It intentionally performs no range jump and therefore claims no storage-work
/// reduction.
#[cfg(test)]
pub(in crate::db::executor) struct RepeatedPullHeldHeadKeyStream<S> {
    inner: S,
    comparator: KeyOrderComparator,
    held: Option<DecodedDataStoreKey>,
    exhausted: bool,
    last_pulled: Option<DecodedDataStoreKey>,
}

#[cfg(test)]
impl<S> RepeatedPullHeldHeadKeyStream<S>
where
    S: OrderedKeyStream,
{
    /// Construct a reference adapter over one stream with fixed traversal order.
    #[must_use]
    pub(in crate::db::executor) const fn new(inner: S, comparator: KeyOrderComparator) -> Self {
        Self {
            inner,
            comparator,
            held: None,
            exhausted: false,
            last_pulled: None,
        }
    }

    fn ensure_head_state(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<EnsureHeadState, InternalError> {
        if self.held.is_some() {
            return Ok(EnsureHeadState::Held);
        }
        if self.exhausted {
            return Ok(EnsureHeadState::Exhausted);
        }

        if !work.admits_pull() {
            return Ok(EnsureHeadState::PageStop);
        }
        work.record_pull_attempt()?;

        let Some(next) = self.inner.next_key()? else {
            self.exhausted = true;
            return Ok(EnsureHeadState::Exhausted);
        };

        if let Some(previous) = self.last_pulled.as_ref() {
            if previous.entity_tag() != next.entity_tag() {
                return Err(InternalError::executor_invariant());
            }
            work.record_comparison()?;
            if self.comparator.compare_data_keys(previous, &next).is_gt() {
                return Err(InternalError::executor_invariant());
            }
        }

        self.last_pulled = Some(next.clone());
        self.held = Some(next);
        Ok(EnsureHeadState::Held)
    }

    fn outcome(&self, state: EnsureHeadState) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        match state {
            EnsureHeadState::Held => self
                .held
                .as_ref()
                .map(HeldHeadSeekOutcome::Held)
                .ok_or_else(InternalError::executor_invariant),
            EnsureHeadState::Exhausted => Ok(HeldHeadSeekOutcome::Exhausted),
            EnsureHeadState::PageStop => Ok(HeldHeadSeekOutcome::PageStop),
        }
    }

    fn discard_head_for_seek(&mut self, work: &mut HeldHeadSeekWork) -> Result<(), InternalError> {
        if self.held.is_none() {
            return Err(InternalError::executor_invariant());
        }
        work.record_skipped_consumptions(1)?;
        self.held = None;
        Ok(())
    }
}

#[cfg(test)]
impl<S> HeldHeadKeyStream for RepeatedPullHeldHeadKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn ensure_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        let state = self.ensure_head_state(work)?;
        self.outcome(state)
    }

    fn seek_head_at_or_after(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        loop {
            let state = self.ensure_head_state(work)?;
            if state != EnsureHeadState::Held {
                return self.outcome(state);
            }

            let key = self
                .held
                .as_ref()
                .ok_or_else(InternalError::executor_invariant)?;
            work.record_comparison()?;
            let held_is_before_target = self.comparator.compare_data_keys(key, target).is_lt();
            if !held_is_before_target {
                return self.outcome(EnsureHeadState::Held);
            }
            self.discard_head_for_seek(work)?;
        }
    }

    fn consume_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        if self.held.is_none() {
            return Ok(None);
        }
        work.record_consumed()?;
        self.held
            .take()
            .map(Some)
            .ok_or_else(InternalError::executor_invariant)
    }
}
