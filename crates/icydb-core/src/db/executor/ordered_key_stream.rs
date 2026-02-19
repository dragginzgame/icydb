use crate::{db::data::DataKey, error::InternalError};

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
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        data::{DataKey, StorageKey},
        executor::ordered_key_stream::{OrderedKeyStream, VecOrderedKeyStream},
        identity::EntityName,
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
}
