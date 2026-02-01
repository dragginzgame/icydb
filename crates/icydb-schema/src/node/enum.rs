use crate::prelude::*;
use std::ops::Not;

///
/// Enum
///

#[derive(Clone, Debug, Serialize)]
pub struct Enum {
    pub def: Def,
    pub variants: &'static [EnumVariant],
    pub ty: Type,
}

impl MacroNode for Enum {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Enum {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for Enum {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

impl VisitableNode for Enum {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        for node in self.variants {
            node.accept(v);
        }
        self.ty.accept(v);
    }
}

///
/// EnumVariant
///

#[derive(Clone, Debug, Serialize)]
pub struct EnumVariant {
    pub ident: &'static str,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,

    #[serde(default, skip_serializing_if = "Not::not")]
    pub default: bool,

    #[serde(default, skip_serializing_if = "Not::not")]
    pub unspecified: bool,
}

impl ValidateNode for EnumVariant {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

impl VisitableNode for EnumVariant {
    fn drive<V: Visitor>(&self, v: &mut V) {
        if let Some(node) = &self.value {
            node.accept(v);
        }
    }
}
