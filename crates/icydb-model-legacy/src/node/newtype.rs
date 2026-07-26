use crate::prelude::*;

///
/// Newtype
///

#[derive(Clone, Debug, Serialize)]
pub struct Newtype {
    def: Def,
    source_key: &'static str,
    item: Item,

    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<Arg>,

    ty: Type,
}

impl Newtype {
    /// Creates a newtype node from its canonical schema parts.
    #[must_use]
    pub const fn new(
        def: Def,
        source_key: &'static str,
        item: Item,
        default: Option<Arg>,
        ty: Type,
    ) -> Self {
        Self {
            def,
            source_key,
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

    /// Returns the immutable type source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
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

impl ValidateNode for Newtype {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "newtype",
            self.source_key(),
            icydb_schema::TypeSourceKey::try_new,
        );
        errs.result()
    }
}

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
