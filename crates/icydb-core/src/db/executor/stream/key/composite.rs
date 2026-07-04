//! Module: executor::stream::key::composite
//! Responsibility: merge/intersection combinators over ordered key streams.
//! Does not own: physical key production or access-path traversal.
//! Boundary: comparator-driven stream composition with monotonicity guards.

use crate::{
    db::{
        data::DecodedDataStoreKey,
        executor::stream::{
            FlatMergeOrderedChild, FlatMergeStream,
            key::{KeyOrderComparator, OrderedKeyStream},
        },
        key_taxonomy::PrimaryKeyValue,
    },
    error::InternalError,
    types::EntityTag,
};

type RowKeyWitness = (EntityTag, PrimaryKeyValue);

const fn row_key_witness(key: &DecodedDataStoreKey) -> RowKeyWitness {
    (key.entity_tag(), key.primary_key_value())
}

fn row_witness_matches_key(witness: &RowKeyWitness, key: &DecodedDataStoreKey) -> bool {
    witness.0 == key.entity_tag() && witness.1 == key.primary_key_value()
}

///
/// ConcatOrderedKeyStream
///
/// Pull-based concatenation over already branch-ordered child streams.
/// This is for secondary-prefix families where the branch prefix itself
/// supplies the leading ORDER BY term, so merging by decoded primary key would
/// destroy the requested index order.
///

pub(in crate::db::executor) struct ConcatOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    streams: Vec<S>,
    current: usize,
}

impl<S> ConcatOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    /// Construct one concatenated ordered stream from branch-ordered children.
    #[must_use]
    pub(in crate::db::executor) const fn new(streams: Vec<S>) -> Self {
        Self {
            streams,
            current: 0,
        }
    }
}

impl<S> OrderedKeyStream for ConcatOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        while let Some(stream) = self.streams.get_mut(self.current) {
            if let Some(key) = stream.next_key()? {
                return Ok(Some(key));
            }
            self.current = self.current.saturating_add(1);
        }

        Ok(None)
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        let mut total = 0usize;
        for stream in &self.streams {
            total = total.checked_add(stream.exact_key_count_hint()?)?;
        }

        Some(total)
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        let mut total = 0usize;
        for stream in &self.streams {
            total = total.checked_add(stream.cheap_access_candidate_count_hint()?)?;
        }

        Some(total)
    }

    #[cfg(test)]
    fn exact_diagnostic_access_candidate_count(&self) -> Option<usize> {
        let mut total = 0usize;
        for stream in &self.streams {
            total = total.checked_add(stream.exact_diagnostic_access_candidate_count()?)?;
        }

        Some(total)
    }
}

///
/// StreamSideState
///
/// Per-side lookahead state for one ordered child stream.
/// Tracks pending key, exhaustion status, and monotonicity witness.
///

struct StreamSideState {
    item: Option<DecodedDataStoreKey>,
    done: bool,
    last_key: Option<RowKeyWitness>,
    comparator: KeyOrderComparator,
    strict_monotonicity: bool,
}

impl StreamSideState {
    /// Construct one stream-side lookahead state.
    const fn new(comparator: KeyOrderComparator) -> Self {
        Self {
            item: None,
            done: false,
            last_key: None,
            comparator,
            strict_monotonicity: true,
        }
    }

    // Ensure one lookahead item is available for this stream side.
    fn ensure_item<S>(&mut self, stream: &mut S) -> Result<(), InternalError>
    where
        S: OrderedKeyStream,
    {
        if self.done || self.item.is_some() {
            return Ok(());
        }

        match stream.next_key()? {
            Some(key) => self.push_key(key)?,
            None => self.done = true,
        }

        Ok(())
    }

    // Push one polled key into this stream-side lookahead slot with direction checks.
    fn push_key(&mut self, key: DecodedDataStoreKey) -> Result<(), InternalError> {
        self.validate_monotonicity(&key)?;
        self.item = Some(key);

        Ok(())
    }

    // Build the canonical invariant for entity drift within one ordered child stream.
    fn entity_monotonicity_required() -> InternalError {
        InternalError::query_executor_invariant()
    }

    // Build the canonical invariant for out-of-order keys within one ordered child stream.
    fn key_monotonicity_required() -> InternalError {
        InternalError::query_executor_invariant()
    }

    // Validate this stream-side monotonicity according to configured direction.
    fn validate_monotonicity(&self, current: &DecodedDataStoreKey) -> Result<(), InternalError> {
        if !self.strict_monotonicity {
            return Ok(());
        }
        let Some((previous_entity, previous_key)) = self.last_key.as_ref() else {
            return Ok(());
        };
        let (current_entity, current_key) = row_key_witness(current);

        if *previous_entity != current_entity {
            return Err(Self::entity_monotonicity_required());
        }

        if !self
            .comparator
            .violates_monotonicity(previous_key, &current_key)
        {
            return Ok(());
        }

        Err(Self::key_monotonicity_required())
    }

    fn take_item(&mut self) -> Option<DecodedDataStoreKey> {
        let key = self.item.take()?;
        self.last_key = Some(row_key_witness(&key));

        Some(key)
    }

    fn clear_item(&mut self) {
        if let Some(key) = self.item.take() {
            self.last_key = Some(row_key_witness(&key));
        }
    }
}

///
/// OrderedPairState
///
/// Shared lookahead state for left/right ordered stream polling.
/// Keeps one pending key per side for merge/intersection stream combinators.
///

struct OrderedPairState {
    left: StreamSideState,
    right: StreamSideState,
}

impl OrderedPairState {
    /// Construct one ordered-pair lookahead envelope.
    const fn new(comparator: KeyOrderComparator) -> Self {
        Self {
            left: StreamSideState::new(comparator),
            right: StreamSideState::new(comparator),
        }
    }
}

///
/// MergeOrderedKeyStream
///
/// Pull-based merger over two ordered key streams.
/// Produces one canonical ordered stream while suppressing duplicate keys.
///

pub(in crate::db::executor) struct MergeOrderedKeyStream<A, B> {
    left: A,
    right: B,
    pair: OrderedPairState,
    comparator: KeyOrderComparator,
    last_emitted: Option<RowKeyWitness>,
}

impl<A, B> MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    /// Construct one merge stream using explicit key comparator policy.
    #[must_use]
    pub(in crate::db::executor) const fn new_with_comparator(
        left: A,
        right: B,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self {
            left,
            right,
            pair: OrderedPairState::new(comparator),
            comparator,
            last_emitted: None,
        }
    }

    fn ensure_left_item(&mut self) -> Result<(), InternalError> {
        self.pair.left.ensure_item(&mut self.left)
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        self.pair.right.ensure_item(&mut self.right)
    }
}

impl<A, B> OrderedKeyStream for MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        loop {
            // Maintain one lookahead key on each side.
            self.ensure_left_item()?;
            self.ensure_right_item()?;

            if self.pair.left.item.is_none() && self.pair.right.item.is_none() {
                return Ok(None);
            }

            let next = match (self.pair.left.item.as_ref(), self.pair.right.item.as_ref()) {
                (Some(left_key), Some(right_key)) => {
                    if left_key == right_key {
                        self.pair.right.clear_item();
                        self.pair.left.take_item()
                    } else {
                        let choose_left = self
                            .comparator
                            .compare_data_keys(left_key, right_key)
                            .is_lt();
                        if choose_left {
                            self.pair.left.take_item()
                        } else {
                            self.pair.right.take_item()
                        }
                    }
                }
                (Some(_), None) => self.pair.left.take_item(),
                (None, Some(_)) => self.pair.right.take_item(),
                (None, None) => None,
            };

            let Some(next) = next else {
                return Ok(None);
            };

            // Suppress duplicate output keys from overlapping streams.
            if self
                .last_emitted
                .as_ref()
                .is_some_and(|last| row_witness_matches_key(last, &next))
            {
                continue;
            }

            self.last_emitted = Some(row_key_witness(&next));
            return Ok(Some(next));
        }
    }
}

struct KeyFlatMergeChild<S> {
    stream: S,
    state: StreamSideState,
}

impl<S> KeyFlatMergeChild<S> {
    const fn new(stream: S, comparator: KeyOrderComparator) -> Self {
        Self {
            stream,
            state: StreamSideState::new(comparator),
        }
    }
}

impl<S> FlatMergeOrderedChild for KeyFlatMergeChild<S>
where
    S: OrderedKeyStream,
{
    type Item = DecodedDataStoreKey;
    type KeyWitness = RowKeyWitness;

    fn ensure_item(&mut self) -> Result<(), InternalError> {
        self.state.ensure_item(&mut self.stream)
    }

    fn head_key(&self) -> Option<&DecodedDataStoreKey> {
        self.state.item.as_ref()
    }

    fn take_item(&mut self) -> Option<Self::Item> {
        self.state.take_item()
    }

    fn clear_item(&mut self) {
        self.state.clear_item();
    }

    fn item_key(item: &Self::Item) -> &DecodedDataStoreKey {
        item
    }

    fn key_witness(key: &DecodedDataStoreKey) -> Self::KeyWitness {
        row_key_witness(key)
    }

    fn witness_matches_key(witness: &Self::KeyWitness, key: &DecodedDataStoreKey) -> bool {
        row_witness_matches_key(witness, key)
    }
}

///
/// FlatMergeOrderedKeyStream
///
/// Pull-based merger over a sibling set of ordered key streams.
/// This is the `merge_all` runtime shape for branch-like access routes where a
/// balanced binary merge tree would add nested stream polling without adding
/// ordering information.
///

pub(in crate::db::executor) struct FlatMergeOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    inner: FlatMergeStream<KeyFlatMergeChild<S>>,
}

impl<S> FlatMergeOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    /// Construct one flat merge stream using explicit key comparator policy.
    #[must_use]
    pub(in crate::db::executor) fn new_with_comparator(
        streams: Vec<S>,
        comparator: KeyOrderComparator,
    ) -> Self {
        let children = streams
            .into_iter()
            .map(|stream| KeyFlatMergeChild::new(stream, comparator))
            .collect();

        Self {
            inner: FlatMergeStream::new(children, comparator),
        }
    }
}

impl<S> OrderedKeyStream for FlatMergeOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        self.inner.next_item()
    }
}

///
/// IntersectOrderedKeyStream
///
/// Pull-based intersection over two ordered key streams.
/// Produces one canonical ordered stream containing keys present in both inputs.
///

pub(in crate::db::executor) struct IntersectOrderedKeyStream<A, B> {
    left: A,
    right: B,
    pair: OrderedPairState,
    comparator: KeyOrderComparator,
    last_emitted: Option<RowKeyWitness>,
}

impl<A, B> IntersectOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    /// Construct one intersection stream using explicit key comparator policy.
    #[must_use]
    pub(in crate::db::executor) const fn new_with_comparator(
        left: A,
        right: B,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self {
            left,
            right,
            pair: OrderedPairState::new(comparator),
            comparator,
            last_emitted: None,
        }
    }

    fn ensure_left_item(&mut self) -> Result<(), InternalError> {
        self.pair.left.ensure_item(&mut self.left)
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        self.pair.right.ensure_item(&mut self.right)
    }
}

impl<A, B> OrderedKeyStream for IntersectOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        loop {
            // Once either child is exhausted, no further intersection output is possible.
            if self.pair.left.done || self.pair.right.done {
                return Ok(None);
            }

            // Maintain one lookahead key on each side.
            self.ensure_left_item()?;
            self.ensure_right_item()?;

            let (Some(left_key), Some(right_key)) =
                (self.pair.left.item.as_ref(), self.pair.right.item.as_ref())
            else {
                return Ok(None);
            };

            if left_key == right_key {
                let Some(next) = self.pair.left.take_item() else {
                    return Ok(None);
                };
                self.pair.right.clear_item();

                // Defensively suppress duplicate outputs.
                if self
                    .last_emitted
                    .as_ref()
                    .is_some_and(|last| row_witness_matches_key(last, &next))
                {
                    continue;
                }

                self.last_emitted = Some(row_key_witness(&next));
                return Ok(Some(next));
            }

            let advance_left = self
                .comparator
                .compare_data_keys(left_key, right_key)
                .is_lt();
            if advance_left {
                self.pair.left.clear_item();
            } else {
                self.pair.right.clear_item();
            }
        }
    }
}
