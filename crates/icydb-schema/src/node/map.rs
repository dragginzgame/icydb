use crate::prelude::*;

///
/// Map
///
/// Schema node describing a map collection with key/value descriptors and one
/// canonical runtime type.
///

#[derive(Clone, Debug, Serialize)]
pub struct Map {
    def: Def,
    key: Item,
    value: Value,
    ty: Type,
}

impl Map {
    /// Creates a map node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, key: Item, value: Value, ty: Type) -> Self {
        Self {
            def,
            key,
            value,
            ty,
        }
    }

    /// Returns the definition metadata for this map node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the key descriptor.
    #[must_use]
    pub const fn key(&self) -> &Item {
        &self.key
    }

    /// Returns the value descriptor.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Returns the canonical runtime type descriptor.
    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for Map {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Map {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Map {}

impl VisitableNode for Map {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.key().accept(v);
        self.value().accept(v);
        self.ty().accept(v);
    }
}
