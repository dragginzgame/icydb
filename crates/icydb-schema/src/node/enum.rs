use crate::prelude::*;
use canic_utils::case::{Case, Casing};
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
        let mut errs = ErrorTree::new();

        // check variants for unspecified
        let mut un_count = 0;
        let mut un_first = None;
        for (i, variant) in self.variants.iter().enumerate() {
            if variant.unspecified {
                un_count += 1;
                if un_first.is_none() {
                    un_first = Some(i);
                }
            }
        }

        // Check if there's more than one unspecified variant
        if un_count > 1 {
            err!(
                errs,
                "there should not be more than one unspecified variant"
            );
        }

        // Check if the unspecified variant is not the first in the list
        if let Some(index) = un_first
            && index != 0
        {
            err!(
                errs,
                "the unspecified variant must be the first in the list"
            );
        }

        // Check for multiple defaults
        let default_count = self.variants.iter().filter(|v| v.default).count();
        if default_count > 1 {
            err!(
                errs,
                "exactly one variant must be marked as default, found {default_count}"
            );
        }

        errs.result()
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
        let mut errs = ErrorTree::new();

        // name
        if !self.ident.is_case(Case::UpperCamel) {
            err!(
                errs,
                "variant ident '{}' must be in UpperCamelCase",
                self.ident
            );
        }

        errs.result()
    }
}

impl VisitableNode for EnumVariant {
    fn drive<V: Visitor>(&self, v: &mut V) {
        if let Some(node) = &self.value {
            node.accept(v);
        }
    }
}
