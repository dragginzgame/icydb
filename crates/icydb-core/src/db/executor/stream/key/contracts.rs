//! Module: executor::stream::key::contracts
//! Responsibility: ordered key-stream trait contracts and simple adapters.
//! Does not own: physical stream resolution or planner semantics.
//! Boundary: foundational key-stream interfaces used by executor stream modules.

use crate::{
    db::{
        data::DataKey,
        executor::stream::{
            access::{IndexRangeKeyStream, PrimaryRangeKeyStream},
            key::{
                DistinctOrderedKeyStream, IntersectOrderedKeyStream, KeyOrderComparator,
                MergeOrderedKeyStream,
            },
        },
    },
    error::InternalError,
};
use std::{cell::Cell, rc::Rc};

///
/// OrderedKeyStream
///
/// Internal pull-based stream contract for deterministic ordered `DataKey`
/// production during load execution.
///

pub(in crate::db::executor) trait OrderedKeyStream {
    /// Pull the next key from the stream, or `None` when exhausted.
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError>;

    // Return the exact total number of keys this stream can emit.
    // Implementations should keep this stable across stream consumption.
    fn exact_key_count_hint(&self) -> Option<usize> {
        None
    }
}

///
/// KeyStreamLoopControl
///
/// Shared key-stream control-flow contract used by executor runners that
/// coordinate stream prefetch and per-key handling phases.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum KeyStreamLoopControl {
    Skip,
    Emit,
    Stop,
}

///
/// OrderedKeyStreamBox
///
/// Concrete owned ordered-key stream envelope used across executor access and
/// terminal paths.
/// This preserves one shared `OrderedKeyStream` polling contract while
/// replacing the previous boxed trait object with an enum-backed stream shape
/// that keeps common stream polling on direct matches instead of vtable calls.
///

pub(in crate::db::executor) enum OrderedKeyStreamBox {
    Empty(EmptyOrderedKeyStream),
    Single(SingleOrderedKeyStream),
    Materialized(VecOrderedKeyStream),
    PrimaryRange(PrimaryRangeKeyStream),
    IndexRange(IndexRangeKeyStream),
    Distinct(DistinctOrderedKeyStream<Box<Self>>),
    Merge(MergeOrderedKeyStream<Box<Self>, Box<Self>>),
    Intersect(IntersectOrderedKeyStream<Box<Self>, Box<Self>>),
}

impl OrderedKeyStreamBox {
    fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    /// Poll the next key from this owned ordered stream.
    pub(in crate::db::executor) fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        OrderedKeyStream::next_key(self)
    }

    /// Return the exact emitted-key count hint when this stream can prove one.
    #[must_use]
    pub(in crate::db::executor) fn exact_key_count_hint(&self) -> Option<usize> {
        OrderedKeyStream::exact_key_count_hint(self)
    }

    /// Construct one owned empty ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn empty() -> Self {
        Self::Empty(EmptyOrderedKeyStream)
    }

    /// Construct one owned singleton ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn single(key: DataKey) -> Self {
        Self::Single(SingleOrderedKeyStream::new(key))
    }

    /// Construct one owned materialized ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn materialized(keys: Vec<DataKey>) -> Self {
        Self::Materialized(VecOrderedKeyStream::new(keys))
    }

    /// Construct one owned primary-range ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn primary_range(stream: PrimaryRangeKeyStream) -> Self {
        Self::PrimaryRange(stream)
    }

    /// Construct one owned index-range ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn index_range(stream: IndexRangeKeyStream) -> Self {
        Self::IndexRange(stream)
    }

    /// Construct one owned distinct ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn distinct(inner: Self, comparator: KeyOrderComparator) -> Self {
        Self::Distinct(DistinctOrderedKeyStream::new(inner.boxed(), comparator))
    }

    /// Construct one owned distinct ordered key stream with dedup observability.
    #[must_use]
    pub(in crate::db::executor) fn distinct_with_dedup_counter(
        inner: Self,
        comparator: KeyOrderComparator,
        deduped_keys_counter: Rc<Cell<u64>>,
    ) -> Self {
        Self::Distinct(DistinctOrderedKeyStream::new_with_dedup_counter(
            inner.boxed(),
            comparator,
            deduped_keys_counter,
        ))
    }

    /// Construct one owned merge ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn merge(
        left: Self,
        right: Self,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self::Merge(MergeOrderedKeyStream::new_with_comparator(
            left.boxed(),
            right.boxed(),
            comparator,
        ))
    }

    /// Construct one owned intersection ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn intersect(
        left: Self,
        right: Self,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self::Intersect(IntersectOrderedKeyStream::new_with_comparator(
            left.boxed(),
            right.boxed(),
            comparator,
        ))
    }
}

impl OrderedKeyStream for OrderedKeyStreamBox {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        match self {
            Self::Empty(stream) => stream.next_key(),
            Self::Single(stream) => stream.next_key(),
            Self::Materialized(stream) => stream.next_key(),
            Self::PrimaryRange(stream) => stream.next_key(),
            Self::IndexRange(stream) => stream.next_key(),
            Self::Distinct(stream) => stream.next_key(),
            Self::Merge(stream) => stream.next_key(),
            Self::Intersect(stream) => stream.next_key(),
        }
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        match self {
            Self::Empty(stream) => stream.exact_key_count_hint(),
            Self::Single(stream) => stream.exact_key_count_hint(),
            Self::Materialized(stream) => stream.exact_key_count_hint(),
            Self::PrimaryRange(stream) => stream.exact_key_count_hint(),
            Self::IndexRange(stream) => stream.exact_key_count_hint(),
            Self::Distinct(stream) => stream.exact_key_count_hint(),
            Self::Merge(stream) => stream.exact_key_count_hint(),
            Self::Intersect(stream) => stream.exact_key_count_hint(),
        }
    }
}

/// Return one canonical ordered key stream for already-materialized keys.
pub(in crate::db::executor) fn ordered_key_stream_from_materialized_keys(
    mut keys: Vec<DataKey>,
) -> OrderedKeyStreamBox {
    match keys.len() {
        0 => OrderedKeyStreamBox::empty(),
        1 => OrderedKeyStreamBox::single(
            keys.pop()
                .expect("single-element key stream must contain one key"),
        ),
        _ => OrderedKeyStreamBox::materialized(keys),
    }
}

/// Return the exact emitted key count after applying one optional scan budget.
#[must_use]
pub(in crate::db::executor) fn exact_output_key_count_hint<S>(
    key_stream: &S,
    budget: Option<usize>,
) -> Option<usize>
where
    S: OrderedKeyStream + ?Sized,
{
    let exact = key_stream.exact_key_count_hint()?;

    Some(match budget {
        Some(budget) => exact.min(budget),
        None => exact,
    })
}

/// Return whether one explicit scan budget is already implied by the stream.
#[must_use]
pub(in crate::db::executor) fn key_stream_budget_is_redundant<S>(
    key_stream: &S,
    budget: usize,
) -> bool
where
    S: OrderedKeyStream + ?Sized,
{
    key_stream
        .exact_key_count_hint()
        .is_some_and(|exact| exact <= budget)
}

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
/// EmptyOrderedKeyStream
///
/// Zero-allocation ordered key stream for already-proven empty key results.
/// This keeps empty traversal/materialization cases out of the vector-backed
/// adapter and preserves an exact zero-count hint for downstream budgeting.
///

#[derive(Debug, Default)]
pub(in crate::db::executor) struct EmptyOrderedKeyStream;

impl OrderedKeyStream for EmptyOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        Ok(None)
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(0)
    }
}

///
/// SingleOrderedKeyStream
///
/// Single-key ordered stream for already-materialized singleton access results.
/// This avoids wrapping one key in a vector-backed adapter while keeping the
/// stable exact-count contract used by budgeted executor paths.
///

#[derive(Debug)]
pub(in crate::db::executor) struct SingleOrderedKeyStream {
    key: Option<DataKey>,
}

impl SingleOrderedKeyStream {
    /// Construct one singleton ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn new(key: DataKey) -> Self {
        Self { key: Some(key) }
    }
}

impl OrderedKeyStream for SingleOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        Ok(self.key.take())
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(1)
    }
}

///
/// VecOrderedKeyStream
///
/// Adapter that exposes one materialized ordered key vector through the
/// `OrderedKeyStream` interface.
///

#[derive(Debug)]
pub(in crate::db::executor) struct VecOrderedKeyStream {
    keys: std::vec::IntoIter<DataKey>,
    total_len: usize,
}

impl VecOrderedKeyStream {
    /// Construct a stream adapter over one materialized key vector.
    #[must_use]
    pub(in crate::db::executor) fn new(keys: Vec<DataKey>) -> Self {
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

pub(in crate::db::executor) struct BudgetedOrderedKeyStream<S> {
    inner: S,
    remaining: usize,
    total_count_hint: Option<usize>,
}

impl<S> BudgetedOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    /// Construct a budgeted adapter that emits at most `remaining` keys.
    #[must_use]
    pub(in crate::db::executor) fn new(inner: S, remaining: usize) -> Self {
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
