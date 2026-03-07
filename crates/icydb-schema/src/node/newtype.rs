use crate::prelude::*;

///
/// Newtype
///

#[derive(Clone, Debug, Serialize)]
pub struct Newtype {
    def: Def,
    item: Item,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    default: Option<Arg>,

    ty: Type,
}

impl Newtype {
    /// Creates a newtype node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, item: Item, default: Option<Arg>, ty: Type) -> Self {
        Self {
            def,
            item,
            default,
            ty,
        }
    }

    /// Returns the definition metadata for this newtype node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the wrapped item descriptor.
    #[must_use]
    pub const fn item(&self) -> &Item {
        &self.item
    }

    /// Returns the optional default value descriptor.
    #[must_use]
    pub const fn default(&self) -> Option<&Arg> {
        self.default.as_ref()
    }

    /// Returns the canonical runtime type descriptor.
    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for Newtype {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Newtype {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Newtype {}

impl VisitableNode for Newtype {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.item().accept(v);
        if let Some(node) = self.default() {
            node.accept(v);
        }
        self.ty().accept(v);
    }
}
