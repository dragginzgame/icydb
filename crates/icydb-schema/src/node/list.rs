use crate::prelude::*;

///
/// List
///
/// Schema node describing a list collection with one item descriptor and one
/// canonical runtime type.
///

#[derive(Clone, Debug, Serialize)]
pub struct List {
    def: Def,
    item: Item,
    ty: Type,
}

impl List {
    /// Creates a list node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, item: Item, ty: Type) -> Self {
        Self { def, item, ty }
    }

    /// Returns the definition metadata for this list node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the list item descriptor.
    #[must_use]
    pub const fn item(&self) -> &Item {
        &self.item
    }

    /// Returns the canonical runtime type descriptor.
    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for List {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for List {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for List {}

impl VisitableNode for List {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.item().accept(v);
        self.ty().accept(v);
    }
}
