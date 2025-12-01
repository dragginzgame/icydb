use crate::prelude::*;

///
/// Value
///

#[derive(Clone, Debug, Serialize)]
pub struct Value {
    pub cardinality: Cardinality,
    pub item: Item,
}

impl ValidateNode for Value {}

impl VisitableNode for Value {
    fn drive<V: Visitor>(&self, v: &mut V) {
        self.item.accept(v);
    }
}
