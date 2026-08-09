//! Module: executor::stream::key::contracts
//! Responsibility: ordered key-stream trait contracts and simple adapters.
//! Does not own: physical stream resolution or planner semantics.
//! Boundary: foundational key-stream interfaces used by executor stream modules.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow, StoreVisit},
        executor::{
            budget::charge_current_execution_budget,
            stream::{
                FlatMergeSiblingSet,
                access::{IndexRangeKeyStream, PrimaryRangeKeyStream, SeekableIndexRangeKeyStream},
                key::{
                    ConcatOrderedKeyStream, DistinctOrderedKeyStream, FlatMergeOrderedKeyStream,
                    IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
                },
            },
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{
    cell::{Cell, RefCell},
    mem::size_of,
    rc::Rc,
};

///
/// OrderedKeyStream
///
/// Internal pull-based stream contract for deterministic ordered `DecodedDataStoreKey`
/// production during load execution.
///

pub(in crate::db::executor) trait OrderedKeyStream {
    /// Pull the next key from the stream, or `None` when exhausted.
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError>;

    // Return the exact total number of keys this stream can emit.
    // Implementations should keep this stable across stream consumption.
    fn exact_key_count_hint(&self) -> Option<usize> {
        None
    }

    // Return a cheap access-candidate count when this stream already knows one.
    // Implementations must not scan storage or consume upstream work to answer
    // this hint; unknown counts are reported by downstream consumed-key metrics.
    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        self.exact_key_count_hint()
    }

    // Return a maintained upper bound on physical access entries one
    // `next_key` call may visit while page-aware leaf refills are active.
    // `None` keeps routes without a proof outside production page-unit
    // admission rather than guessing an unsafe bound.
    fn page_access_entry_bound(&self) -> Option<usize> {
        None
    }
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
    SeekableIndexRange(SeekableIndexRangeKeyStream),
    Budgeted(BudgetedOrderedKeyStream<Box<Self>>),
    Observed(ObservedOrderedKeyStream<Box<Self>>),
    Distinct(DistinctOrderedKeyStream<Box<Self>>),
    Concat(ConcatOrderedKeyStream<Self>),
    Merge(MergeOrderedKeyStream<Box<Self>, Box<Self>>),
    FlatMerge(FlatMergeOrderedKeyStream<Self>),
    Intersect(IntersectOrderedKeyStream<Box<Self>, Box<Self>>),
}

impl OrderedKeyStreamBox {
    fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    /// Poll the next key from this owned ordered stream.
    pub(in crate::db::executor) fn next_key(
        &mut self,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        OrderedKeyStream::next_key(self)
    }

    /// Return the access-candidate count represented by this stream.
    #[must_use]
    pub(in crate::db::executor) fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        OrderedKeyStream::cheap_access_candidate_count_hint(self)
    }

    /// Return the proven physical-entry bound for one page candidate pull.
    #[must_use]
    pub(in crate::db::executor) fn page_access_entry_bound(&self) -> Option<usize> {
        OrderedKeyStream::page_access_entry_bound(self)
    }

    /// Visit one unconsumed ASC primary leaf through a single physical row
    /// traversal, preserving transparent budget and progress observers.
    pub(in crate::db::executor) fn try_visit_primary_rows_direct(
        &mut self,
        begin_row: &mut dyn FnMut() -> Result<bool, InternalError>,
        visit_row: &mut dyn for<'row> FnMut(
            DecodedDataStoreKey,
            &'row RawRow,
        ) -> Result<StoreVisit, InternalError>,
    ) -> Result<Option<()>, InternalError> {
        match self {
            Self::PrimaryRange(stream) => stream.try_visit_rows_direct(begin_row, visit_row),
            Self::Budgeted(stream) => {
                let remaining = Cell::new(stream.remaining);
                let mut budgeted_begin = || {
                    if remaining.get() == 0 {
                        return Ok(false);
                    }
                    begin_row()
                };
                let mut budgeted_visit = |key: DecodedDataStoreKey, row: &RawRow| {
                    let visit = visit_row(key, row)?;
                    remaining.set(remaining.get().saturating_sub(1));
                    Ok(visit)
                };
                let outcome = stream
                    .inner
                    .try_visit_primary_rows_direct(&mut budgeted_begin, &mut budgeted_visit)?;
                stream.remaining = remaining.get();
                Ok(outcome)
            }
            Self::Observed(stream) => {
                let last_emitted = Rc::clone(&stream.last_emitted);
                let mut observed_visit = |key: DecodedDataStoreKey, row: &RawRow| {
                    *last_emitted
                        .try_borrow_mut()
                        .map_err(|_| InternalError::query_executor_invariant())? =
                        Some(key.clone());
                    visit_row(key, row)
                };
                stream
                    .inner
                    .try_visit_primary_rows_direct(begin_row, &mut observed_visit)
            }
            _ => Ok(None),
        }
    }

    /// Construct one owned empty ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn empty() -> Self {
        Self::Empty(EmptyOrderedKeyStream)
    }

    /// Construct one owned singleton ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn single(key: DecodedDataStoreKey) -> Self {
        Self::Single(SingleOrderedKeyStream::new(key))
    }

    /// Construct one owned materialized ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn materialized(keys: Vec<DecodedDataStoreKey>) -> Self {
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

    /// Construct one physically seekable index-range stream.
    #[must_use]
    pub(in crate::db::executor) const fn seekable_index_range(
        stream: SeekableIndexRangeKeyStream,
    ) -> Self {
        Self::SeekableIndexRange(stream)
    }

    /// Construct one owned budgeted ordered key stream.
    #[must_use]
    pub(in crate::db::executor) fn budgeted(inner: Self, remaining: usize) -> Self {
        Self::Budgeted(BudgetedOrderedKeyStream::new(inner.boxed(), remaining))
    }

    /// Observe the last key emitted through one route-attempt stream.
    #[must_use]
    pub(in crate::db::executor) fn observed(
        inner: Self,
        last_emitted: Rc<RefCell<Option<DecodedDataStoreKey>>>,
    ) -> Self {
        Self::Observed(ObservedOrderedKeyStream::new(inner.boxed(), last_emitted))
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

    /// Construct one ordered concatenation from already branch-ordered streams.
    #[must_use]
    pub(in crate::db::executor) fn concat_all(mut streams: Vec<Self>) -> Self {
        match streams.len() {
            0 => Self::empty(),
            1 => streams.pop().unwrap_or_else(Self::empty),
            _ => Self::Concat(ConcatOrderedKeyStream::new(streams)),
        }
    }

    /// Construct one owned merge ordered key stream.
    fn merge(
        left: Self,
        right: Self,
        comparator: KeyOrderComparator,
    ) -> Result<Self, InternalError> {
        let retained_bytes = size_of::<Self>()
            .checked_mul(2)
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(retained_bytes).unwrap_or(u64::MAX),
        )?;

        Ok(Self::Merge(MergeOrderedKeyStream::new_with_comparator(
            left.boxed(),
            right.boxed(),
            comparator,
        )))
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

    /// Construct one balanced merge tree from already ordered streams.
    pub(in crate::db::executor) fn merge_all(
        streams: Vec<Self>,
        comparator: KeyOrderComparator,
    ) -> Result<Self, InternalError> {
        match FlatMergeSiblingSet::from_vec(streams) {
            FlatMergeSiblingSet::Empty => Ok(Self::empty()),
            FlatMergeSiblingSet::Single(stream) => Ok(stream),
            FlatMergeSiblingSet::Pair(left, right) => Self::merge(left, right, comparator),
            FlatMergeSiblingSet::Many(streams) => {
                FlatMergeOrderedKeyStream::try_new_with_comparator(streams, comparator)
                    .map(Self::FlatMerge)
            }
        }
    }

    /// Construct one balanced intersection tree from already ordered streams.
    pub(in crate::db::executor) fn intersect_all(
        mut streams: Vec<Self>,
        comparator: KeyOrderComparator,
    ) -> Result<Self, InternalError> {
        if streams.is_empty() {
            return Ok(Self::empty());
        }

        let pair_count = streams.len().saturating_sub(1);
        let boxed_bytes = pair_count
            .checked_mul(2)
            .and_then(|boxes| boxes.checked_mul(size_of::<Self>()))
            .ok_or_else(InternalError::executor_invariant)?;
        let mut round_len = streams.len();
        let mut reduction_slots = 0usize;
        while round_len > 1 {
            round_len = round_len.div_ceil(2);
            reduction_slots = reduction_slots
                .checked_add(round_len)
                .ok_or_else(InternalError::executor_invariant)?;
        }
        let reduction_bytes = reduction_slots
            .checked_mul(size_of::<Self>())
            .ok_or_else(InternalError::executor_invariant)?;
        let topology_bytes = boxed_bytes
            .checked_add(reduction_bytes)
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(topology_bytes).unwrap_or(u64::MAX),
        )?;

        while streams.len() > 1 {
            let capacity = streams.len().div_ceil(2);
            let mut next_round = Vec::new();
            next_round
                .try_reserve_exact(capacity)
                .map_err(|_| InternalError::executor_internal())?;
            let extra_capacity = next_round.capacity().saturating_sub(capacity);
            if extra_capacity != 0 {
                let extra_bytes = extra_capacity
                    .checked_mul(size_of::<Self>())
                    .ok_or_else(InternalError::executor_invariant)?;
                charge_current_execution_budget(
                    DiagnosticExecutionBudgetResource::TemporaryBytes,
                    u64::try_from(extra_bytes).unwrap_or(u64::MAX),
                )?;
            }
            let mut iter = streams.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    next_round.push(Self::intersect(left, right, comparator));
                } else {
                    next_round.push(left);
                }
            }
            streams = next_round;
        }

        streams.pop().ok_or_else(InternalError::executor_invariant)
    }
}

impl OrderedKeyStream for OrderedKeyStreamBox {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        match self {
            Self::Empty(stream) => stream.next_key(),
            Self::Single(stream) => stream.next_key(),
            Self::Materialized(stream) => stream.next_key(),
            Self::PrimaryRange(stream) => stream.next_key(),
            Self::IndexRange(stream) => stream.next_key(),
            Self::SeekableIndexRange(stream) => stream.next_key(),
            Self::Budgeted(stream) => stream.next_key(),
            Self::Observed(stream) => stream.next_key(),
            Self::Distinct(stream) => stream.next_key(),
            Self::Concat(stream) => stream.next_key(),
            Self::Merge(stream) => stream.next_key(),
            Self::FlatMerge(stream) => stream.next_key(),
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
            Self::SeekableIndexRange(stream) => stream.exact_key_count_hint(),
            Self::Budgeted(stream) => stream.exact_key_count_hint(),
            Self::Observed(stream) => stream.exact_key_count_hint(),
            Self::Distinct(stream) => stream.exact_key_count_hint(),
            Self::Concat(stream) => stream.exact_key_count_hint(),
            Self::Merge(stream) => stream.exact_key_count_hint(),
            Self::FlatMerge(stream) => stream.exact_key_count_hint(),
            Self::Intersect(stream) => stream.exact_key_count_hint(),
        }
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        match self {
            Self::Empty(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Single(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Materialized(stream) => stream.cheap_access_candidate_count_hint(),
            Self::PrimaryRange(stream) => stream.cheap_access_candidate_count_hint(),
            Self::IndexRange(stream) => stream.cheap_access_candidate_count_hint(),
            Self::SeekableIndexRange(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Budgeted(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Observed(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Distinct(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Concat(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Merge(stream) => stream.cheap_access_candidate_count_hint(),
            Self::FlatMerge(stream) => stream.cheap_access_candidate_count_hint(),
            Self::Intersect(stream) => stream.cheap_access_candidate_count_hint(),
        }
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        match self {
            Self::Empty(stream) => stream.page_access_entry_bound(),
            Self::Single(stream) => stream.page_access_entry_bound(),
            Self::Materialized(stream) => stream.page_access_entry_bound(),
            Self::PrimaryRange(stream) => stream.page_access_entry_bound(),
            Self::IndexRange(stream) => stream.page_access_entry_bound(),
            Self::SeekableIndexRange(stream) => stream.page_access_entry_bound(),
            Self::Budgeted(stream) => stream.page_access_entry_bound(),
            Self::Observed(stream) => stream.page_access_entry_bound(),
            Self::Distinct(stream) => stream.page_access_entry_bound(),
            Self::Concat(stream) => stream.page_access_entry_bound(),
            Self::Merge(stream) => stream.page_access_entry_bound(),
            Self::FlatMerge(stream) => stream.page_access_entry_bound(),
            Self::Intersect(stream) => stream.page_access_entry_bound(),
        }
    }
}

/// Return one canonical ordered key stream for already-materialized keys.
pub(in crate::db::executor) fn ordered_key_stream_from_materialized_keys(
    mut keys: Vec<DecodedDataStoreKey>,
) -> OrderedKeyStreamBox {
    match keys.len() {
        0 => OrderedKeyStreamBox::empty(),
        1 => match keys.pop() {
            Some(key) => OrderedKeyStreamBox::single(key),
            None => OrderedKeyStreamBox::empty(),
        },
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
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        self.as_mut().next_key()
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        self.as_ref().exact_key_count_hint()
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        self.as_ref().cheap_access_candidate_count_hint()
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        self.as_ref().page_access_entry_bound()
    }
}

impl<T> OrderedKeyStream for &mut T
where
    T: OrderedKeyStream + ?Sized,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        (**self).next_key()
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        (**self).exact_key_count_hint()
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        (**self).cheap_access_candidate_count_hint()
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        (**self).page_access_entry_bound()
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
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        Ok(None)
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(0)
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
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
    key: Option<DecodedDataStoreKey>,
}

impl SingleOrderedKeyStream {
    /// Construct one singleton ordered key stream.
    #[must_use]
    pub(in crate::db::executor) const fn new(key: DecodedDataStoreKey) -> Self {
        Self { key: Some(key) }
    }
}

impl OrderedKeyStream for SingleOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        Ok(self.key.take())
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(1)
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        Some(0)
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
    keys: std::vec::IntoIter<DecodedDataStoreKey>,
    total_len: usize,
}

impl VecOrderedKeyStream {
    /// Construct a stream adapter over one materialized key vector.
    #[must_use]
    pub(in crate::db::executor) fn new(keys: Vec<DecodedDataStoreKey>) -> Self {
        let total_len = keys.len();

        Self {
            keys: keys.into_iter(),
            total_len,
        }
    }
}

impl OrderedKeyStream for VecOrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        Ok(self.keys.next())
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        Some(self.total_len)
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        Some(0)
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
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
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

    fn page_access_entry_bound(&self) -> Option<usize> {
        self.inner.page_access_entry_bound()
    }
}

/// Route-attempt observer that retains only the last emitted physical row key.
///
/// The observer sits outside page candidate limiting, so a published physical
/// continuation can never advance past a key that the scalar row loop did not
/// examine.
pub(in crate::db::executor) struct ObservedOrderedKeyStream<S> {
    inner: S,
    last_emitted: Rc<RefCell<Option<DecodedDataStoreKey>>>,
}

impl<S> ObservedOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    const fn new(inner: S, last_emitted: Rc<RefCell<Option<DecodedDataStoreKey>>>) -> Self {
        Self {
            inner,
            last_emitted,
        }
    }
}

impl<S> OrderedKeyStream for ObservedOrderedKeyStream<S>
where
    S: OrderedKeyStream,
{
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        let key = self.inner.next_key()?;
        if let Some(key) = key.as_ref() {
            *self
                .last_emitted
                .try_borrow_mut()
                .map_err(|_| InternalError::query_executor_invariant())? = Some(key.clone());
        }

        Ok(key)
    }

    fn exact_key_count_hint(&self) -> Option<usize> {
        self.inner.exact_key_count_hint()
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        self.inner.cheap_access_candidate_count_hint()
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        self.inner.page_access_entry_bound()
    }
}
