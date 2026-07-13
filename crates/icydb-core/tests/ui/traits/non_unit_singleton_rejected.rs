use icydb_core::{
    db::EntityKey,
    entity::{EntityValue, SingletonEntity},
    traits::{AuthoredFieldProjection, FieldProjection},
    types::Id,
    value::{InputValue, Value},
};

struct MultiRowEntity {
    id: u64,
}

impl EntityKey for MultiRowEntity {
    type Key = u64;
}

impl AuthoredFieldProjection for MultiRowEntity {
    fn get_input_value_by_index(&self, _index: usize) -> Option<InputValue> {
        None
    }
}

impl FieldProjection for MultiRowEntity {
    fn get_value_by_index(&self, _index: usize) -> Option<Value> {
        None
    }
}

impl EntityValue for MultiRowEntity {
    fn id(&self) -> Id<Self> {
        Id::from_key(self.id)
    }
}

impl SingletonEntity for MultiRowEntity {}

fn main() {}
