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
