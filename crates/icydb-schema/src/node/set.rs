use crate::prelude::*;

///
/// Set
///

#[derive(Clone, Debug, Serialize)]
pub struct Set {
    def: Def,
    item: Item,
    ty: Type,
}

impl Set {
    /// Creates a set node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, item: Item, ty: Type) -> Self {
        Self { def, item, ty }
    }

    /// Returns the definition metadata for this set node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the set item descriptor.
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

impl MacroNode for Set {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Set {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Set {}

impl VisitableNode for Set {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.item().accept(v);
        self.ty().accept(v);
    }
}
