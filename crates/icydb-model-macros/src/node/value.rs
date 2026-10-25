//! Module: node::value
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

///
/// Value
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct Value {
    #[darling(default)]
    pub(crate) opt: bool,

    #[darling(default)]
    pub(crate) many: bool,

    pub(crate) item: Item,
}

impl Value {
    pub fn validate(&self) -> Result<(), DarlingError> {
        if self.opt && self.many {
            return Err(DarlingError::custom(
                "cardinality cannot be opt and many at the same time",
            ));
        }

        self.item.validate()
    }

    // cardinality
    pub fn cardinality(&self) -> Cardinality {
        debug_assert!(
            !(self.opt && self.many),
            "cardinality cannot be opt and many at the same time"
        );

        if self.many {
            Cardinality::Many
        } else if self.opt {
            Cardinality::Opt
        } else {
            Cardinality::One
        }
    }
}

impl HasSchemaPart for Value {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let cardinality = self.cardinality().generated_tokens(paths);
        let item = &self.item.schema_part(paths);

        // quote
        quote!(
            #model::node::Value::new(#cardinality, #item)
        )
    }
}

impl HasTypeExpr for Value {
    fn type_expr(&self, paths: &CratePaths) -> TokenStream {
        let item = &self.item.type_expr(paths);

        match self.cardinality() {
            Cardinality::One => quote!(#item),
            Cardinality::Opt => quote!(::core::option::Option<#item>),
            Cardinality::Many => quote!(::std::vec::Vec<#item>),
        }
    }
}
