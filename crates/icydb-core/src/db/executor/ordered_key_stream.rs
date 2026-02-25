use crate::{
    db::{
        data::{DataKey, StorageKey},
        identity::EntityName,
        query::plan::Direction,
    },
    error::InternalError,
};
use std::{cell::Cell, cmp::Ordering, rc::Rc};

type DataKeyWitness = (EntityName, StorageKey);

const fn data_key_witness(key: &DataKey) -> DataKeyWitness {
    (*key.entity_name(), key.storage_key())
}

fn compare_key_witnesses(
    comparator: KeyOrderComparator,
    left: &DataKeyWitness,
    right: &DataKeyWitness,
) -> Option<Ordering> {
    if left.0 != right.0 {
        return None;
    }

    Some(comparator.compare_storage_keys(&left.1, &right.1))
}

fn witness_matches_key(witness: &DataKeyWitness, key: &DataKey) -> bool {
    witness.0 == *key.entity_name() && witness.1 == key.storage_key()
}

///
/// OrderedKeyStream
///
/// Internal pull-based stream contract for deterministic ordered `DataKey`
/// production during load execution.
///

pub(crate) trait OrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError>;

    // Return the exact total number of keys this stream can emit.
    // Implementations should keep this stable across stream consumption.
    fn exact_key_count_hint(&self) -> Option<usize> {
        None
    }
}

pub(crate) type OrderedKeyStreamBox = Box<dyn OrderedKeyStream>;

impl<T> OrderedKeyStream for Box<T>
where
    T: OrderedKeyStream + ?Sized,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        self.as_mut().next_key()
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        self.as_ref().exact_key_count_hint()
    }
}

impl<T> OrderedKeyStream for &mut T
where
    T: OrderedKeyStream + ?Sized,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        (**self).next_key()
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        (**self).exact_key_count_hint()
    }
}

///
/// VecOrderedKeyStream
///
/// Adapter that exposes one materialized ordered key vector through the
/// `OrderedKeyStream` interface.
///

#[derive(Debug)]
pub(crate) struct VecOrderedKeyStream {
    keys: std::vec::IntoIter<DataKey>,
    total_len: usize,
}

impl VecOrderedKeyStream {
    #[must_use]
    pub(crate) fn new(keys: Vec<DataKey>) -> Self {
        let total_len = keys.len();

        Self {
            keys: keys.into_iter(),
            total_len,
        }
    }
}

impl OrderedKeyStream for VecOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        Ok(self.keys.next())
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(self.total_len)
    }
}

///
/// BudgetedOrderedKeyStream
///
/// Wrapper that caps upstream key production after a fixed number of emitted keys.
/// Once the budget is exhausted, it never polls the inner stream again.
///

pub(crate) struct BudgetedOrderedKeyStream<S> {
    inner: S,
    remaining: usize,
    total_count_hint: Option<usize>,
}

impl<S> BudgetedOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    #[must_use]
    pub(crate) fn new(inner: S, remaining: usize) -> Self {
        let total_count_hint = inner
            .exact_key_count_hint()
            .map(|count| count.min(remaining));

        Self {
            inner,
            remaining,
            total_count_hint,
        }
    }
}

impl<S> OrderedKeyStream for BudgetedOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        if self.remaining == 0 {
            return Ok(None);
        }

        match self.inner.next_key()? {
            Some(key) => {
                self.remaining = self.remaining.saturating_sub(1);
                Ok(Some(key))
            }
            None => Ok(None),
        }
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        self.total_count_hint
    }
}

///
/// DistinctOrderedKeyStream
///
/// Wrapper that suppresses adjacent duplicate keys from an ordered stream.
/// Correct DISTINCT requires identical keys to be contiguous in the underlying order.
/// This keeps DISTINCT semantics streaming and O(1) memory.
///

pub(crate) struct DistinctOrderedKeyStream<S> {
    inner: S,
    last_emitted: Option<DataKeyWitness>,
    comparator: KeyOrderComparator,
    deduped_keys_counter: Option<Rc<Cell<u64>>>,
}

impl<S> DistinctOrderedKeyStream<S> {
    #[must_use]
    pub(crate) const fn new(inner: S, comparator: KeyOrderComparator) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
            deduped_keys_counter: None,
        }
    }

    #[must_use]
    pub(crate) const fn new_with_dedup_counter(
        inner: S,
        comparator: KeyOrderComparator,
        deduped_keys_counter: Rc<Cell<u64>>,
    ) -> Self {
        Self {
            inner,
            last_emitted: None,
            comparator,
            deduped_keys_counter: Some(deduped_keys_counter),
        }
    }
}

impl<S> OrderedKeyStream for DistinctOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        loop {
            let Some(next) = self.inner.next_key()? else {
                return Ok(None);
            };
            let next_witness = data_key_witness(&next);

            if let Some(last) = self.last_emitted.as_ref()
                && let Some(ordering) = compare_key_witnesses(self.comparator, last, &next_witness)
            {
                if ordering.is_gt() {
                    return Err(InternalError::query_executor_invariant(
                        "distinct ordered stream received non-monotonic key order",
                    ));
                }
                if ordering.is_eq() {
                    if let Some(counter) = self.deduped_keys_counter.as_ref() {
                        counter.set(counter.get().saturating_add(1));
                    }
                    continue;
                }
            }

            self.last_emitted = Some(next_witness);

            return Ok(Some(next));
        }
    }
}

///
/// KeyOrderComparator
///
/// Comparator wrapper for ordered key stream monotonicity and merge decisions.
/// This keeps stream combinators comparator-driven instead of directly branching
/// on traversal direction at each call site.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyOrderComparator {
    direction: Direction,
}

impl KeyOrderComparator {
    #[must_use]
    pub(crate) const fn from_direction(direction: Direction) -> Self {
        Self { direction }
    }

    fn compare(self, left: &DataKey, right: &DataKey) -> Ordering {
        match self.direction {
            Direction::Asc => left.cmp(right),
            Direction::Desc => right.cmp(left),
        }
    }

    fn compare_storage_keys(self, left: &StorageKey, right: &StorageKey) -> Ordering {
        match self.direction {
            Direction::Asc => left.cmp(right),
            Direction::Desc => right.cmp(left),
        }
    }

    fn violates_monotonicity(self, previous: &StorageKey, current: &StorageKey) -> bool {
        self.compare_storage_keys(previous, current).is_gt()
    }

    const fn order_label(self) -> &'static str {
        match self.direction {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
        }
    }
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            data::{DataKey, StorageKey},
            executor::ordered_key_stream::{
                BudgetedOrderedKeyStream, DistinctOrderedKeyStream, IntersectOrderedKeyStream,
                KeyOrderComparator, MergeOrderedKeyStream, OrderedKeyStream, VecOrderedKeyStream,
            },
            identity::EntityName,
            query::plan::Direction,
        },
        error::{ErrorClass, ErrorOrigin, InternalError},
    };
    use std::{cell::Cell, rc::Rc};

    fn data_key(value: u64) -> DataKey {
        let raw = DataKey::raw_from_parts(
            EntityName::try_from_str("ordered_key_stream_tests")
                .expect("test entity name should be valid"),
            StorageKey::Uint(value),
        )
        .expect("test key encoding should succeed");

        DataKey::try_from_raw(&raw).expect("test key decode should succeed")
    }

    struct StaticOrderedKeyStream {
        keys: Vec<DataKey>,
        index: usize,
        fail_at: Option<usize>,
    }

    impl StaticOrderedKeyStream {
        fn new(keys: Vec<DataKey>) -> Self {
            Self {
                keys,
                index: 0,
                fail_at: None,
            }
        }

        fn with_fail_at(keys: Vec<DataKey>, fail_at: usize) -> Self {
            Self {
                keys,
                index: 0,
                fail_at: Some(fail_at),
            }
        }
    }

    impl OrderedKeyStream for StaticOrderedKeyStream {
        fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
            if self.fail_at.is_some_and(|idx| self.index == idx) {
                return Err(InternalError::query_internal("forced stream failure"));
            }
            if self.index >= self.keys.len() {
                return Ok(None);
            }

            let key = self.keys[self.index].clone();
            self.index = self.index.saturating_add(1);

            Ok(Some(key))
        }
    }

    fn collect_stream(stream: &mut impl OrderedKeyStream) -> Result<Vec<DataKey>, InternalError> {
        let mut out = Vec::new();
        while let Some(key) = stream.next_key()? {
            out.push(key);
        }

        Ok(out)
    }

    #[test]
    fn vec_ordered_key_stream_yields_keys_in_input_order() {
        let mut stream =
            VecOrderedKeyStream::new(vec![data_key(3), data_key(1), data_key(2), data_key(1)]);
        let mut out = Vec::new();

        while let Some(key) = stream.next_key().expect("stream next must succeed") {
            out.push(key);
        }

        assert_eq!(
            out,
            vec![data_key(3), data_key(1), data_key(2), data_key(1)]
        );
    }

    #[test]
    fn vec_ordered_key_stream_returns_none_after_exhaustion() {
        let mut stream = VecOrderedKeyStream::new(vec![data_key(9)]);
        let first = stream.next_key().expect("first next must succeed");
        let second = stream.next_key().expect("second next must succeed");
        let third = stream.next_key().expect("third next must succeed");

        assert_eq!(first, Some(data_key(9)));
        assert_eq!(second, None);
        assert_eq!(third, None);
    }

    #[test]
    fn vec_stream_reports_exact_key_count_hint() {
        let mut stream = VecOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
        assert_eq!(
            stream.exact_key_count_hint(),
            Some(2),
            "vec stream must report exact total key count before consumption"
        );

        let _ = stream.next_key().expect("first next must succeed");
        assert_eq!(
            stream.exact_key_count_hint(),
            Some(2),
            "vec stream exact key-count hint must remain stable after consumption"
        );
    }

    #[test]
    fn budgeted_stream_does_not_claim_exact_key_count_hint() {
        let inner = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
        let stream = BudgetedOrderedKeyStream::new(inner, 1);

        assert_eq!(
            stream.exact_key_count_hint(),
            None,
            "budgeted stream must not claim exact counts when inner stream does not provide them"
        );
    }

    #[test]
    fn budgeted_stream_reports_stable_total_hint_when_inner_is_exact() {
        let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let mut stream = BudgetedOrderedKeyStream::new(inner, 2);

        assert_eq!(
            stream.exact_key_count_hint(),
            Some(2),
            "budgeted stream must report min(inner_total, budget) as exact total output"
        );

        let _ = stream.next_key().expect("first key should be available");
        assert_eq!(
            stream.exact_key_count_hint(),
            Some(2),
            "budgeted stream exact key-count hint must remain stable after consumption"
        );
    }

    #[test]
    fn budgeted_stream_total_hint_is_min_of_inner_total_and_budget() {
        let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let budget_limited = BudgetedOrderedKeyStream::new(inner, 2);
        assert_eq!(
            budget_limited.exact_key_count_hint(),
            Some(2),
            "budget-limited stream must report budget when budget is smaller than inner total"
        );

        let inner = VecOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let inner_limited = BudgetedOrderedKeyStream::new(inner, 10);
        assert_eq!(
            inner_limited.exact_key_count_hint(),
            Some(3),
            "budget-limited stream must report inner total when budget exceeds inner total"
        );
    }

    #[test]
    fn budgeted_stream_stops_after_budget_without_polling_inner() {
        let inner =
            StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(2), data_key(3)], 1);
        let mut stream = BudgetedOrderedKeyStream::new(inner, 1);

        assert_eq!(
            stream.next_key().expect("first key should be available"),
            Some(data_key(1))
        );
        assert_eq!(
            stream
                .next_key()
                .expect("exhausted budget should return None"),
            None
        );
        assert_eq!(
            stream
                .next_key()
                .expect("exhausted budget should keep returning None"),
            None
        );
    }

    #[test]
    fn budgeted_stream_with_zero_budget_is_immediately_exhausted() {
        let inner = StaticOrderedKeyStream::with_fail_at(vec![data_key(1)], 0);
        let mut stream = BudgetedOrderedKeyStream::new(inner, 0);

        assert_eq!(
            stream
                .next_key()
                .expect("zero-budget stream should not poll inner"),
            None
        );
        assert_eq!(
            stream
                .next_key()
                .expect("zero-budget stream should stay exhausted"),
            None
        );
    }

    #[test]
    fn distinct_stream_suppresses_consecutive_duplicates() {
        let inner = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(1),
            data_key(2),
            data_key(2),
            data_key(2),
            data_key(3),
        ]);
        let mut stream = DistinctOrderedKeyStream::new(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
        );

        let out = collect_stream(&mut stream).expect("distinct stream should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
    }

    #[test]
    fn distinct_stream_records_deduped_key_count() {
        let inner = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(1),
            data_key(2),
            data_key(2),
            data_key(2),
            data_key(3),
        ]);
        let dedup_counter = Rc::new(Cell::new(0u64));
        let mut stream = DistinctOrderedKeyStream::new_with_dedup_counter(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
            dedup_counter.clone(),
        );

        let out = collect_stream(&mut stream).expect("distinct stream should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
        assert_eq!(
            dedup_counter.get(),
            3,
            "dedup counter should include every suppressed adjacent duplicate key"
        );
    }

    #[test]
    fn distinct_stream_propagates_underlying_errors() {
        let inner = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(1)], 1);
        let mut stream = DistinctOrderedKeyStream::new(
            inner,
            KeyOrderComparator::from_direction(Direction::Asc),
        );

        let err = collect_stream(&mut stream).expect_err("distinct stream should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn distinct_stream_rejects_non_monotonic_keys_for_both_directions() {
        for (direction, values) in [
            (Direction::Asc, vec![data_key(1), data_key(3), data_key(2)]),
            (Direction::Desc, vec![data_key(3), data_key(1), data_key(2)]),
        ] {
            let inner = StaticOrderedKeyStream::new(values);
            let mut stream =
                DistinctOrderedKeyStream::new(inner, KeyOrderComparator::from_direction(direction));

            let err = collect_stream(&mut stream)
                .expect_err("non-monotonic distinct stream input should be rejected");
            assert_eq!(
                err.class,
                ErrorClass::InvariantViolation,
                "distinct monotonicity failures must classify as invariant violations"
            );
            assert!(
                err.message.contains("non-monotonic key order"),
                "distinct monotonicity failure should expose a clear invariant reason"
            );
        }
    }

    #[test]
    fn merge_stream_asc_interleaves_two_ordered_streams() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(5)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(4), data_key(6)]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut merged).expect("merge should succeed");
        assert_eq!(
            out,
            vec![
                data_key(1),
                data_key(2),
                data_key(3),
                data_key(4),
                data_key(5),
                data_key(6)
            ]
        );
    }

    #[test]
    fn merge_stream_deduplicates_shared_keys() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(3), data_key(4)]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut merged).expect("merge should succeed");
        assert_eq!(
            out,
            vec![data_key(1), data_key(2), data_key(3), data_key(4)]
        );
    }

    #[test]
    fn merge_stream_equal_key_discard_path_remains_stable() {
        let left = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(2),
            data_key(3),
            data_key(4),
            data_key(5),
        ]);
        let right = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(2),
            data_key(3),
            data_key(4),
            data_key(5),
        ]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut merged).expect("merge should succeed");
        assert_eq!(
            out,
            vec![
                data_key(1),
                data_key(2),
                data_key(3),
                data_key(4),
                data_key(5)
            ],
            "merge output should remain stable when repeatedly discarding equal right-side lookahead"
        );
    }

    #[test]
    fn merge_stream_desc_interleaves_two_descending_streams() {
        let left = StaticOrderedKeyStream::new(vec![data_key(6), data_key(4), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(5), data_key(3), data_key(1)]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Desc);

        let out = collect_stream(&mut merged).expect("merge should succeed");
        assert_eq!(
            out,
            vec![
                data_key(6),
                data_key(5),
                data_key(4),
                data_key(3),
                data_key(2),
                data_key(1)
            ]
        );
    }

    #[test]
    fn merge_stream_propagates_underlying_errors() {
        let left = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(3)], 1);
        let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(4)]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Asc);

        let err = collect_stream(&mut merged).expect_err("merge should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn merge_stream_rejects_child_direction_mismatch() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
        let mut merged = MergeOrderedKeyStream::new(left, right, Direction::Desc);

        let err = collect_stream(&mut merged).expect_err("merge should fail on direction mismatch");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn intersect_stream_asc_yields_shared_keys() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(5)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4), data_key(5)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut intersected).expect("intersection should succeed");
        assert_eq!(out, vec![data_key(3), data_key(5)]);
    }

    #[test]
    fn intersect_stream_desc_yields_shared_keys() {
        let left = StaticOrderedKeyStream::new(vec![data_key(5), data_key(3), data_key(1)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(6), data_key(5), data_key(3)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Desc);

        let out = collect_stream(&mut intersected).expect("intersection should succeed");
        assert_eq!(out, vec![data_key(5), data_key(3)]);
    }

    #[test]
    fn intersect_stream_returns_empty_when_no_overlap() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut intersected).expect("intersection should succeed");
        assert!(out.is_empty());
    }

    #[test]
    fn intersect_stream_deduplicates_internal_duplicates() {
        let left = StaticOrderedKeyStream::new(vec![
            data_key(1),
            data_key(1),
            data_key(2),
            data_key(3),
            data_key(3),
        ]);
        let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut intersected).expect("intersection should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
    }

    #[test]
    fn intersect_stream_deduplicates_when_both_sides_duplicate() {
        let left =
            StaticOrderedKeyStream::new(vec![data_key(1), data_key(1), data_key(2), data_key(3)]);
        let right =
            StaticOrderedKeyStream::new(vec![data_key(1), data_key(1), data_key(2), data_key(3)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let out = collect_stream(&mut intersected).expect("intersection should succeed");
        assert_eq!(out, vec![data_key(1), data_key(2), data_key(3)]);
    }

    #[test]
    fn intersect_stream_propagates_underlying_errors() {
        let left = StaticOrderedKeyStream::with_fail_at(vec![data_key(1), data_key(3)], 1);
        let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let err = collect_stream(&mut intersected).expect_err("intersection should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn intersect_stream_rejects_child_direction_mismatch() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(3), data_key(4)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Desc);

        let err =
            collect_stream(&mut intersected).expect_err("intersection should fail on mismatch");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn intersect_stream_rejects_non_monotonic_child_sequence() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(1), data_key(2), data_key(3)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let err = collect_stream(&mut intersected)
            .expect_err("intersection should fail when child emits non-monotonic keys");
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }

    #[test]
    fn intersect_stream_rejects_non_monotonic_child_sequence_when_discard_updates_witness() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(2)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(10), data_key(10), data_key(10)]);
        let mut intersected = IntersectOrderedKeyStream::new(left, right, Direction::Asc);

        let err = collect_stream(&mut intersected).expect_err(
            "intersection should fail when repeated discard advances are followed by non-monotonic keys",
        );
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }
}
