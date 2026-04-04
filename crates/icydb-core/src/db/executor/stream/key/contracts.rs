//! Module: executor::stream::key::contracts
//! Responsibility: ordered key-stream trait contracts and simple adapters.
//! Does not own: physical stream resolution or planner semantics.
//! Boundary: foundational key-stream interfaces used by executor stream modules.

use crate::{db::data::DataKey, error::InternalError};

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

// Shared key-stream driver used by scalar and grouped aggregate runners.
// Callers inject pre-fetch and per-key callbacks so stream control flow is
// centralized without coupling to lane-specific semantics.
pub(in crate::db::executor) fn drive_key_stream_with_control_flow(
    key_stream: &mut dyn OrderedKeyStream,
    pre_fetch: &mut dyn FnMut() -> KeyStreamLoopControl,
    on_key: &mut dyn FnMut(DataKey) -> Result<KeyStreamLoopControl, InternalError>,
) -> Result<(), InternalError> {
    loop {
        match pre_fetch() {
            KeyStreamLoopControl::Skip => continue,
            KeyStreamLoopControl::Emit => {}
            KeyStreamLoopControl::Stop => break,
        }

        let Some(key) = key_stream.next_key()? else {
            break;
        };

        match on_key(key)? {
            KeyStreamLoopControl::Skip | KeyStreamLoopControl::Emit => {}
            KeyStreamLoopControl::Stop => break,
        }
    }

    Ok(())
}

pub(in crate::db::executor) type OrderedKeyStreamBox = Box<dyn OrderedKeyStream>;

/// Return one canonical ordered key stream for already-materialized keys.
pub(in crate::db::executor) fn ordered_key_stream_from_materialized_keys(
    mut keys: Vec<DataKey>,
) -> OrderedKeyStreamBox {
    match keys.len() {
        0 => Box::new(EmptyOrderedKeyStream),
        1 => Box::new(SingleOrderedKeyStream::new(
            keys.pop()
                .expect("single-element key stream must contain one key"),
        )),
        _ => Box::new(VecOrderedKeyStream::new(keys)),
    }
}

/// Return the exact emitted key count after applying one optional scan budget.
#[must_use]
pub(in crate::db::executor) fn exact_output_key_count_hint(
    key_stream: &dyn OrderedKeyStream,
    budget: Option<usize>,
) -> Option<usize> {
    let exact = key_stream.exact_key_count_hint()?;

    Some(match budget {
        Some(budget) => exact.min(budget),
        None => exact,
    })
}

/// Return whether one explicit scan budget is already implied by the stream.
#[must_use]
pub(in crate::db::executor) fn key_stream_budget_is_redundant(
    key_stream: &dyn OrderedKeyStream,
    budget: usize,
) -> bool {
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
