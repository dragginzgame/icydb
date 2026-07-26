use crate::prelude::*;

///
/// Enum
///

#[derive(Clone, Debug, Serialize)]
pub struct Enum {
    def: Def,
    source_key: &'static str,
    variants: &'static [EnumVariant],
    ty: Type,
}

impl Enum {
    #[must_use]
    pub const fn new(
        def: Def,
        source_key: &'static str,
        variants: &'static [EnumVariant],
        ty: Type,
    ) -> Self {
        Self {
            def,
            source_key,
            variants,
            ty,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Borrow the immutable enum type source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
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
        validate_source_key(
            &mut errs,
            "enum type",
            self.source_key(),
            icydb_schema::TypeSourceKey::try_new,
        );
        let mut seen = std::collections::BTreeSet::new();
        for variant in self.variants() {
            if !seen.insert(variant.source_key()) {
                err!(
                    errs,
                    "duplicate enum variant source key '{}'",
                    variant.source_key(),
                );
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
    source_key: &'static str,
    ident: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

impl EnumVariant {
    #[must_use]
    pub const fn new(source_key: &'static str, ident: &'static str, value: Option<Value>) -> Self {
        Self {
            source_key,
            ident,
            value,
        }
    }

    /// Borrow the immutable variant source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
    }

    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    #[must_use]
    pub const fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}

impl ValidateNode for EnumVariant {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "enum variant",
            self.source_key(),
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
