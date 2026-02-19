use crate::{
    db::{data::DataKey, query::plan::Direction},
    error::InternalError,
};

///
/// OrderedKeyStream
///
/// Internal pull-based stream contract for deterministic ordered `DataKey`
/// production during load execution.
///

pub(crate) trait OrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError>;
}

pub(crate) type OrderedKeyStreamBox = Box<dyn OrderedKeyStream>;

impl<T> OrderedKeyStream for Box<T>
where
    T: OrderedKeyStream + ?Sized,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        self.as_mut().next_key()
    }
}

impl<T> OrderedKeyStream for &mut T
where
    T: OrderedKeyStream + ?Sized,
{
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        (**self).next_key()
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
    keys: Vec<DataKey>,
    index: usize,
}

impl VecOrderedKeyStream {
    #[must_use]
    pub(crate) const fn new(keys: Vec<DataKey>) -> Self {
        Self { keys, index: 0 }
    }
}

impl OrderedKeyStream for VecOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        if self.index >= self.keys.len() {
            return Ok(None);
        }

        let key = self.keys[self.index].clone();
        self.index = self.index.saturating_add(1);

        Ok(Some(key))
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
}

impl<S> BudgetedOrderedKeyStream<S> {
    #[must_use]
    pub(crate) const fn new(inner: S, remaining: usize) -> Self {
        Self { inner, remaining }
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MergeDirection {
    Asc,
    Desc,
}

impl MergeDirection {
    const fn from_direction(direction: Direction) -> Self {
        match direction {
            Direction::Asc => Self::Asc,
            Direction::Desc => Self::Desc,
        }
    }
}

///
/// OrderedPairState
///
/// Shared lookahead state for left/right ordered stream polling.
/// Keeps one pending key per side for merge/intersection stream combinators.
///

struct OrderedPairState<L, R> {
    left: Option<L>,
    right: Option<R>,
}

impl<L, R> OrderedPairState<L, R> {
    const fn new() -> Self {
        Self {
            left: None,
            right: None,
        }
    }
}

// Ensure one lookahead item is available for one stream side.
#[expect(clippy::too_many_arguments)]
fn ensure_item<S>(
    stream: &mut S,
    stream_item: &mut Option<DataKey>,
    stream_done: &mut bool,
    stream_last_pulled: &mut Option<DataKey>,
    direction: MergeDirection,
    stream_name: &'static str,
    stream_kind: &'static str,
    direction_context: &'static str,
) -> Result<(), InternalError>
where
    S: OrderedKeyStream,
{
    if *stream_done || stream_item.is_some() {
        return Ok(());
    }

    match stream.next_key()? {
        Some(key) => push_key(
            key,
            stream_item,
            stream_last_pulled,
            direction,
            stream_name,
            stream_kind,
            direction_context,
        )?,
        None => *stream_done = true,
    }

    Ok(())
}

// Push one polled key into one stream-side lookahead slot with direction checks.
fn push_key(
    key: DataKey,
    stream_item: &mut Option<DataKey>,
    stream_last_pulled: &mut Option<DataKey>,
    direction: MergeDirection,
    stream_name: &'static str,
    stream_kind: &'static str,
    direction_context: &'static str,
) -> Result<(), InternalError> {
    validate_stream_direction(
        stream_last_pulled.as_ref(),
        &key,
        direction,
        stream_name,
        stream_kind,
        direction_context,
    )?;
    *stream_last_pulled = Some(key.clone());
    *stream_item = Some(key);

    Ok(())
}

// Validate one child stream monotonicity against the configured merge direction.
fn validate_stream_direction(
    previous: Option<&DataKey>,
    current: &DataKey,
    direction: MergeDirection,
    stream_name: &'static str,
    stream_kind: &'static str,
    direction_context: &'static str,
) -> Result<(), InternalError> {
    let Some(previous) = previous else {
        return Ok(());
    };

    let violates_direction = match direction {
        MergeDirection::Asc => current < previous,
        MergeDirection::Desc => current > previous,
    };
    if !violates_direction {
        return Ok(());
    }

    let direction_label = match direction {
        MergeDirection::Asc => "ASC",
        MergeDirection::Desc => "DESC",
    };

    Err(InternalError::query_invariant(format!(
        "executor invariant violated: {stream_kind} stream {stream_name} emitted out-of-order key for {direction_label} {direction_context} (previous: {previous}, current: {current})"
    )))
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
    pair: OrderedPairState<DataKey, DataKey>,
    left_done: bool,
    right_done: bool,
    direction: MergeDirection,
    left_last_pulled: Option<DataKey>,
    right_last_pulled: Option<DataKey>,
    last_emitted: Option<DataKey>,
}

impl<A, B> MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    #[must_use]
    pub(crate) const fn new(left: A, right: B, direction: Direction) -> Self {
        Self {
            left,
            right,
            pair: OrderedPairState::new(),
            left_done: false,
            right_done: false,
            direction: MergeDirection::from_direction(direction),
            left_last_pulled: None,
            right_last_pulled: None,
            last_emitted: None,
        }
    }

    fn ensure_left_item(&mut self) -> Result<(), InternalError> {
        ensure_item(
            &mut self.left,
            &mut self.pair.left,
            &mut self.left_done,
            &mut self.left_last_pulled,
            self.direction,
            "left",
            "merge",
            "merge",
        )
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        ensure_item(
            &mut self.right,
            &mut self.pair.right,
            &mut self.right_done,
            &mut self.right_last_pulled,
            self.direction,
            "right",
            "merge",
            "merge",
        )
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

            if self.pair.left.is_none() && self.pair.right.is_none() {
                return Ok(None);
            }

            let next = match (self.pair.left.as_ref(), self.pair.right.as_ref()) {
                (Some(left_key), Some(right_key)) => {
                    if left_key == right_key {
                        self.pair.right = None;
                        self.pair.left.take()
                    } else {
                        let choose_left = match self.direction {
                            MergeDirection::Asc => left_key < right_key,
                            MergeDirection::Desc => left_key > right_key,
                        };
                        if choose_left {
                            self.pair.left.take()
                        } else {
                            self.pair.right.take()
                        }
                    }
                }
                (Some(_), None) => self.pair.left.take(),
                (None, Some(_)) => self.pair.right.take(),
                (None, None) => None,
            };

            let Some(next) = next else {
                return Ok(None);
            };

            // Suppress duplicate output keys from overlapping streams.
            if self.last_emitted.as_ref().is_some_and(|last| *last == next) {
                continue;
            }

            self.last_emitted = Some(next.clone());
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
    pair: OrderedPairState<DataKey, DataKey>,
    left_done: bool,
    right_done: bool,
    direction: MergeDirection,
    left_last_pulled: Option<DataKey>,
    right_last_pulled: Option<DataKey>,
    last_emitted: Option<DataKey>,
}

impl<A, B> IntersectOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    #[must_use]
    pub(crate) const fn new(left: A, right: B, direction: Direction) -> Self {
        Self {
            left,
            right,
            pair: OrderedPairState::new(),
            left_done: false,
            right_done: false,
            direction: MergeDirection::from_direction(direction),
            left_last_pulled: None,
            right_last_pulled: None,
            last_emitted: None,
        }
    }

    fn ensure_left_item(&mut self) -> Result<(), InternalError> {
        ensure_item(
            &mut self.left,
            &mut self.pair.left,
            &mut self.left_done,
            &mut self.left_last_pulled,
            self.direction,
            "left",
            "intersect",
            "intersection",
        )
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        ensure_item(
            &mut self.right,
            &mut self.pair.right,
            &mut self.right_done,
            &mut self.right_last_pulled,
            self.direction,
            "right",
            "intersect",
            "intersection",
        )
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
            if self.left_done || self.right_done {
                return Ok(None);
            }

            // Maintain one lookahead key on each side.
            self.ensure_left_item()?;
            self.ensure_right_item()?;

            let (Some(left_key), Some(right_key)) =
                (self.pair.left.as_ref(), self.pair.right.as_ref())
            else {
                return Ok(None);
            };

            if left_key == right_key {
                let next = left_key.clone();
                self.pair.left = None;
                self.pair.right = None;

                // Defensively suppress duplicate outputs.
                if self.last_emitted.as_ref().is_some_and(|last| *last == next) {
                    continue;
                }

                self.last_emitted = Some(next.clone());
                return Ok(Some(next));
            }

            let advance_left = match self.direction {
                MergeDirection::Asc => left_key < right_key,
                MergeDirection::Desc => left_key > right_key,
            };
            if advance_left {
                self.pair.left = None;
            } else {
                self.pair.right = None;
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
                BudgetedOrderedKeyStream, IntersectOrderedKeyStream, MergeOrderedKeyStream,
                OrderedKeyStream, VecOrderedKeyStream,
            },
            identity::EntityName,
            query::plan::Direction,
        },
        error::{ErrorClass, ErrorOrigin, InternalError},
    };

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
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Query,
                    "forced stream failure",
                ));
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
}
