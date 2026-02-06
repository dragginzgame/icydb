use crate::prelude::*;

///
/// Value
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct Value {
    #[darling(default)]
    pub opt: bool,

    #[darling(default)]
    pub many: bool,

    pub item: Item,
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
    fn schema_part(&self) -> TokenStream {
        let cardinality = &self.cardinality();
        let item = &self.item.schema_part();

        // quote
        quote!(
            ::icydb::schema::node::Value {
                cardinality: #cardinality,
                item: #item,
            }
        )
    }
}

impl HasTypeExpr for Value {
    fn type_expr(&self) -> TokenStream {
        let item = &self.item.type_expr();

        match self.cardinality() {
            Cardinality::One => quote!(#item),
            Cardinality::Opt => quote!(Option<#item>),
            Cardinality::Many => {
                if let Some(relation) = &self.item.relation {
                    quote!(::icydb::types::IdSet<#relation>)
                } else {
                    quote!(::icydb::types::OrderedList<#item>)
                }
            }
        }
    }
}
