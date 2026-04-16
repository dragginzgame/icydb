use crate::prelude::*;
use std::ops::Not;

///
/// Enum
///

#[derive(Clone, Debug, Serialize)]
pub struct Enum {
    def: Def,
    variants: &'static [EnumVariant],
    ty: Type,
}

impl Enum {
    #[must_use]
    pub const fn new(def: Def, variants: &'static [EnumVariant], ty: Type) -> Self {
        Self { def, variants, ty }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
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

impl TypeNode for Enum {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Enum {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
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
    ident: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Value>,

    #[serde(skip_serializing_if = "Not::not")]
    default: bool,

    #[serde(skip_serializing_if = "Not::not")]
    unspecified: bool,
}

impl EnumVariant {
    #[must_use]
    pub const fn new(
        ident: &'static str,
        value: Option<Value>,
        default: bool,
        unspecified: bool,
    ) -> Self {
        Self {
            ident,
            value,
            default,
            unspecified,
        }
    }

    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    #[must_use]
    pub const fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    #[must_use]
    pub const fn default(&self) -> bool {
        self.default
    }

    #[must_use]
    pub const fn unspecified(&self) -> bool {
        self.unspecified
    }
}

impl ValidateNode for EnumVariant {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

impl VisitableNode for EnumVariant {
    fn drive<V: Visitor>(&self, v: &mut V) {
        if let Some(node) = self.value() {
            node.accept(v);
        }
    }
}
