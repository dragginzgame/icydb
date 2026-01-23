use crate::{key::Key, model::index::IndexModel, value::Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPath {
    ByKey(Key),
    ByKeys(Vec<Key>),
    KeyRange {
        start: Key,
        end: Key,
    },
    IndexPrefix {
        index: IndexModel,
        values: Vec<Value>,
    },
    FullScan,
}
