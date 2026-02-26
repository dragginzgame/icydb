#[cfg(test)]
use crate::db::direction::Direction;
use crate::{
    db::{
        data::{DataKey, StorageKey},
        executor::stream::key::{KeyOrderComparator, OrderedKeyStream},
        identity::EntityName,
    },
    error::InternalError,
};

type DataKeyWitness = (EntityName, StorageKey);

const fn data_key_witness(key: &DataKey) -> DataKeyWitness {
    (*key.entity_name(), key.storage_key())
}

fn witness_matches_key(witness: &DataKeyWitness, key: &DataKey) -> bool {
    witness.0 == *key.entity_name() && witness.1 == key.storage_key()
}

///
/// StreamSideState
///
/// StreamSideState
///
/// Per-side lookahead state for one ordered child stream.
/// Tracks pending key, exhaustion status, and monotonicity witness.
///

struct StreamSideState {
    item: Option<DataKey>,
    done: bool,
    last_key: Option<DataKeyWitness>,
    comparator: KeyOrderComparator,
    strict_monotonicity: bool,
    name: &'static str,
}

impl StreamSideState {
    const fn new(name: &'static str, comparator: KeyOrderComparator) -> Self {
        Self {
            item: None,
            done: false,
            last_key: None,
            comparator,
            strict_monotonicity: true,
            name,
        }
    }

    // Ensure one lookahead item is available for this stream side.
    fn ensure_item<S>(
        &mut self,
        stream: &mut S,
        stream_kind: &'static str,
        direction_context: &'static str,
    ) -> Result<(), InternalError>
    where
        S: OrderedKeyStream,
    {
        if self.done || self.item.is_some() {
            return Ok(());
        }

        match stream.next_key()? {
            Some(key) => self.push_key(key, stream_kind, direction_context)?,
            None => self.done = true,
        }

        Ok(())
    }

    // Push one polled key into this stream-side lookahead slot with direction checks.
    fn push_key(
        &mut self,
        key: DataKey,
        stream_kind: &'static str,
        direction_context: &'static str,
    ) -> Result<(), InternalError> {
        self.validate_monotonicity(&key, stream_kind, direction_context)?;
        self.item = Some(key);

        Ok(())
    }

    // Validate this stream-side monotonicity according to configured direction.
    fn validate_monotonicity(
        &self,
        current: &DataKey,
        stream_kind: &'static str,
        direction_context: &'static str,
    ) -> Result<(), InternalError> {
        if !self.strict_monotonicity {
            return Ok(());
        }
        let Some((previous_entity, previous_key)) = self.last_key.as_ref() else {
            return Ok(());
        };
        let (current_entity, current_key) = data_key_witness(current);

        if *previous_entity != current_entity {
            return Err(InternalError::query_invariant(format!(
                "executor invariant violated: {stream_kind} stream {} changed entity while enforcing {} {direction_context} monotonicity (previous entity: {:?}, current entity: {:?})",
                self.name,
                self.comparator.order_label(),
                previous_entity,
                current_entity,
            )));
        }

        if !self
            .comparator
            .violates_monotonicity(previous_key, &current_key)
        {
            return Ok(());
        }

        Err(InternalError::query_invariant(format!(
            "executor invariant violated: {stream_kind} stream {} emitted out-of-order key for {} {direction_context} (entity: {:?}, previous key: {:?}, current key: {:?})",
            self.name,
            self.comparator.order_label(),
            current_entity,
            previous_key,
            current_key,
        )))
    }

    fn take_item(&mut self) -> Option<DataKey> {
        let key = self.item.take()?;
        self.last_key = Some(data_key_witness(&key));

        Some(key)
    }

    const fn clear_item(&mut self) {
        if let Some(key) = self.item.take() {
            self.last_key = Some(data_key_witness(&key));
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
    const fn new(comparator: KeyOrderComparator) -> Self {
        Self {
            left: StreamSideState::new("left", comparator),
            right: StreamSideState::new("right", comparator),
        }
    }
}

///
/// MergeOrderedKeyStream
///
/// Pull-based merger over two ordered key streams.
/// Produces one canonical ordered stream while suppressing duplicate keys.
///

pub(crate) struct MergeOrderedKeyStream<A, B> {
    left: A,
    right: B,
    pair: OrderedPairState,
    comparator: KeyOrderComparator,
    last_emitted: Option<DataKeyWitness>,
}

impl<A, B> MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(left: A, right: B, direction: Direction) -> Self {
        Self::new_with_comparator(left, right, KeyOrderComparator::from_direction(direction))
    }

    #[must_use]
    pub(crate) const fn new_with_comparator(
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
        self.pair.left.ensure_item(&mut self.left, "merge", "merge")
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        self.pair
            .right
            .ensure_item(&mut self.right, "merge", "merge")
    }
}

impl<A, B> OrderedKeyStream for MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
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
                        let choose_left = self.comparator.compare(left_key, right_key).is_lt();
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
                .is_some_and(|last| witness_matches_key(last, &next))
            {
                continue;
            }

            self.last_emitted = Some(data_key_witness(&next));
            return Ok(Some(next));
        }
    }
}

///
/// IntersectOrderedKeyStream
///
/// Pull-based intersection over two ordered key streams.
/// Produces one canonical ordered stream containing keys present in both inputs.
///

pub(crate) struct IntersectOrderedKeyStream<A, B> {
    left: A,
    right: B,
    pair: OrderedPairState,
    comparator: KeyOrderComparator,
    last_emitted: Option<DataKeyWitness>,
}

impl<A, B> IntersectOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(left: A, right: B, direction: Direction) -> Self {
        Self::new_with_comparator(left, right, KeyOrderComparator::from_direction(direction))
    }

    #[must_use]
    pub(crate) const fn new_with_comparator(
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
        self.pair
            .left
            .ensure_item(&mut self.left, "intersect", "intersection")
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        self.pair
            .right
            .ensure_item(&mut self.right, "intersect", "intersection")
    }
}

impl<A, B> OrderedKeyStream for IntersectOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
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
                    .is_some_and(|last| witness_matches_key(last, &next))
                {
                    continue;
                }

                self.last_emitted = Some(data_key_witness(&next));
                return Ok(Some(next));
            }

            let advance_left = self.comparator.compare(left_key, right_key).is_lt();
            if advance_left {
                self.pair.left.clear_item();
            } else {
                self.pair.right.clear_item();
            }
        }
    }
}
