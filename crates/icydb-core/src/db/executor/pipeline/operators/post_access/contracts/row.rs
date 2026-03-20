use crate::{
    db::executor::{OrderReadableRow, delete::DeleteRow},
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

impl<E> OrderReadableRow for (Id<E>, E)
where
    E: EntityKind + EntityValue,
{
    fn read_order_slot(&self, slot: usize) -> Option<Value> {
        self.1.get_value_by_index(slot)
    }
}

impl<E> OrderReadableRow for DeleteRow<E>
where
    E: EntityKind + EntityValue,
{
    fn read_order_slot(&self, slot: usize) -> Option<Value> {
        self.entity_ref().get_value_by_index(slot)
    }
}
