use crate::prelude::*;

///
/// Enum
///

#[derive(Clone, Debug, Serialize)]
pub struct Enum {
    def: Def,
    name: &'static str,
    variants: &'static [EnumVariant],
    ty: Type,
}

impl Enum {
    #[must_use]
    pub const fn new(
        def: Def,
        name: &'static str,
        variants: &'static [EnumVariant],
        ty: Type,
    ) -> Self {
        Self {
            def,
            name,
            variants,
            ty,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Borrow the current declared enum type name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn variants(&self) -> &'static [EnumVariant] {
        self.variants
    }

    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for Enum {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl ValidateNode for Enum {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_name(
            &mut errs,
            "enum type",
            self.name(),
            icydb_schema::TypeSourceKey::try_new,
        );
        let mut seen = std::collections::BTreeSet::new();
        for variant in self.variants() {
            if !seen.insert(variant.name()) {
                err!(errs, "duplicate enum variant name '{}'", variant.name(),);
            }
        }
        errs.result()
    }
}

impl VisitableNode for Enum {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        for node in self.variants() {
            node.accept(v);
        }
        self.ty().accept(v);
    }
}

///
/// EnumVariant
///

#[derive(Clone, Debug, Serialize)]
pub struct EnumVariant {
    name: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

impl EnumVariant {
    #[must_use]
    pub const fn new(name: &'static str, value: Option<Value>) -> Self {
        Self { name, value }
    }

    /// Borrow the current declared variant name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    #[must_use]
    pub const fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}

impl ValidateNode for EnumVariant {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_name(
            &mut errs,
            "enum variant",
            self.name(),
            icydb_schema::TypeSourceKey::try_new,
        );
        errs.result()
    }
}

impl VisitableNode for EnumVariant {
    fn drive<V: Visitor>(&self, v: &mut V) {
        if let Some(node) = self.value() {
            node.accept(v);
        }
    }
}
