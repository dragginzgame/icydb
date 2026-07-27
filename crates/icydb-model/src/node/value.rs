use crate::prelude::*;

///
/// Value
///
/// Canonical value descriptor pairing cardinality with one item definition.
///

#[derive(Clone, Debug, Serialize)]
pub struct Value {
    cardinality: Cardinality,
    item: Item,
}

impl Value {
    /// Build one value descriptor from its schema cardinality and item target.
    #[must_use]
    pub const fn new(cardinality: Cardinality, item: Item) -> Self {
        Self { cardinality, item }
    }

    #[must_use]
    pub const fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    #[must_use]
    pub const fn item(&self) -> &Item {
        &self.item
    }
}

impl ValidateNode for Value {}

impl VisitableNode for Value {
    fn drive<V: Visitor>(&self, v: &mut V) {
        self.item().accept(v);
    }
}
