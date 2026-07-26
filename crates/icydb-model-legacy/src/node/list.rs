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
    source_key: &'static str,
    item: Item,
    ty: Type,
}

impl List {
    /// Creates a list node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, source_key: &'static str, item: Item, ty: Type) -> Self {
        Self {
            def,
            source_key,
            item,
            ty,
        }
    }

    /// Returns the definition metadata for this list node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the immutable type source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
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

impl ValidateNode for List {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "list type",
            self.source_key(),
            icydb_schema::TypeSourceKey::try_new,
        );
        errs.result()
    }
}

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
