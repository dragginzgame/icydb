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
    // cardinality
    pub fn cardinality(&self) -> Cardinality {
        match (&self.opt, &self.many) {
            (false, false) => Cardinality::One,
            (true, false) => Cardinality::Opt,
            (false, true) => Cardinality::Many,
            (true, true) => panic!("cardinality cannot be opt and many"),
        }
    }
}

impl HasSchemaPart for Value {
    fn schema_part(&self) -> TokenStream {
        let cardinality = &self.cardinality();
        let item = &self.item.schema_part();

        // quote
        let sp = paths().schema;
        quote!(
            #sp::node::Value {
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
            Cardinality::Many => quote!(Vec<#item>),
        }
    }
}
