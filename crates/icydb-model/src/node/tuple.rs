use crate::prelude::*;

///
/// Tuple
///

#[derive(Clone, Debug, Serialize)]
pub struct Tuple {
    def: Def,
    source_key: &'static str,
    values: &'static [Value],
    ty: Type,
}

impl Tuple {
    /// Creates a tuple node from its canonical schema parts.
    #[must_use]
    pub const fn new(
        def: Def,
        source_key: &'static str,
        values: &'static [Value],
        ty: Type,
    ) -> Self {
        Self {
            def,
            source_key,
            values,
            ty,
        }
    }

    /// Returns the definition metadata for this tuple node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the immutable type source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
    }

    /// Returns the tuple value descriptors.
    #[must_use]
    pub const fn values(&self) -> &'static [Value] {
        self.values
    }

    /// Returns the canonical runtime type descriptor.
    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for Tuple {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Tuple {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "tuple type",
            self.source_key(),
            icydb_schema::TypeSourceKey::try_new,
        );
        errs.result()
    }
}

impl VisitableNode for Tuple {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        for node in self.values() {
            node.accept(v);
        }
        self.ty().accept(v);
    }
}
