use crate::{db::data::DataKey, error::InternalError};
use std::collections::VecDeque;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
enum MergeDirection {
    Asc,
    Desc,
}

///
/// MergeOrderedKeyStream
///
/// Pull-based merger over two ordered key streams.
/// Produces one canonical ordered stream while suppressing duplicate keys.
///

#[allow(dead_code)]
pub(crate) struct MergeOrderedKeyStream<A, B> {
    left: A,
    right: B,
    left_buffer: VecDeque<DataKey>,
    right_buffer: VecDeque<DataKey>,
    left_done: bool,
    right_done: bool,
    direction: Option<MergeDirection>,
    last_emitted: Option<DataKey>,
}

#[allow(dead_code)]
impl<A, B> MergeOrderedKeyStream<A, B>
where
    A: OrderedKeyStream,
    B: OrderedKeyStream,
{
    #[must_use]
    pub(crate) const fn new(left: A, right: B) -> Self {
        Self {
            left,
            right,
            left_buffer: VecDeque::new(),
            right_buffer: VecDeque::new(),
            left_done: false,
            right_done: false,
            direction: None,
            last_emitted: None,
        }
    }

    fn ensure_left_item(&mut self) -> Result<(), InternalError> {
        if self.left_done || !self.left_buffer.is_empty() {
            return Ok(());
        }

        match self.left.next_key()? {
            Some(key) => self.left_buffer.push_back(key),
            None => self.left_done = true,
        }

        Ok(())
    }

    fn ensure_right_item(&mut self) -> Result<(), InternalError> {
        if self.right_done || !self.right_buffer.is_empty() {
            return Ok(());
        }

        match self.right.next_key()? {
            Some(key) => self.right_buffer.push_back(key),
            None => self.right_done = true,
        }

        Ok(())
    }

    fn infer_direction_from_left(&mut self) -> Result<Option<MergeDirection>, InternalError> {
        self.ensure_left_item()?;
        let Some(first_key) = self.left_buffer.front().cloned() else {
            return Ok(None);
        };

        loop {
            if let Some(next_distinct) = self.left_buffer.iter().find(|key| **key != first_key) {
                return Ok(Some(direction_from_pair(&first_key, next_distinct)));
            }
            if self.left_done {
                return Ok(None);
            }

            match self.left.next_key()? {
                Some(key) => self.left_buffer.push_back(key),
                None => self.left_done = true,
            }
        }
    }

    fn infer_direction_from_right(&mut self) -> Result<Option<MergeDirection>, InternalError> {
        self.ensure_right_item()?;
        let Some(first_key) = self.right_buffer.front().cloned() else {
            return Ok(None);
        };

        loop {
            if let Some(next_distinct) = self.right_buffer.iter().find(|key| **key != first_key) {
                return Ok(Some(direction_from_pair(&first_key, next_distinct)));
            }
            if self.right_done {
                return Ok(None);
            }

            match self.right.next_key()? {
                Some(key) => self.right_buffer.push_back(key),
                None => self.right_done = true,
            }
        }
    }

    fn ensure_direction(&mut self) -> Result<MergeDirection, InternalError> {
        if let Some(direction) = self.direction {
            return Ok(direction);
        }

        // Infer from left stream first, then right stream. If both streams are
        // exhausted (or all observed keys are equal), default to ASC.
        if let Some(direction) = self.infer_direction_from_left()? {
            self.direction = Some(direction);
            return Ok(direction);
        }
        if let Some(direction) = self.infer_direction_from_right()? {
            self.direction = Some(direction);
            return Ok(direction);
        }

        self.direction = Some(MergeDirection::Asc);
        Ok(MergeDirection::Asc)
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

            if self.left_buffer.is_empty() && self.right_buffer.is_empty() {
                return Ok(None);
            }

            let direction = self.ensure_direction()?;
            let next = match (self.left_buffer.front(), self.right_buffer.front()) {
                (Some(left_key), Some(right_key)) => {
                    if left_key == right_key {
                        let _ = self.right_buffer.pop_front();
                        self.left_buffer.pop_front()
                    } else {
                        let choose_left = match direction {
                            MergeDirection::Asc => left_key < right_key,
                            MergeDirection::Desc => left_key > right_key,
                        };
                        if choose_left {
                            self.left_buffer.pop_front()
                        } else {
                            self.right_buffer.pop_front()
                        }
                    }
                }
                (Some(_), None) => self.left_buffer.pop_front(),
                (None, Some(_)) => self.right_buffer.pop_front(),
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

#[allow(dead_code)]
fn direction_from_pair(first: &DataKey, second: &DataKey) -> MergeDirection {
    if first < second {
        MergeDirection::Asc
    } else {
        MergeDirection::Desc
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
                MergeOrderedKeyStream, OrderedKeyStream, VecOrderedKeyStream,
            },
            identity::EntityName,
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
    fn merge_stream_asc_interleaves_two_ordered_streams() {
        let left = StaticOrderedKeyStream::new(vec![data_key(1), data_key(3), data_key(5)]);
        let right = StaticOrderedKeyStream::new(vec![data_key(2), data_key(4), data_key(6)]);
        let mut merged = MergeOrderedKeyStream::new(left, right);

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
        let mut merged = MergeOrderedKeyStream::new(left, right);

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
        let mut merged = MergeOrderedKeyStream::new(left, right);

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
        let mut merged = MergeOrderedKeyStream::new(left, right);

        let err = collect_stream(&mut merged).expect_err("merge should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Query);
    }
}
