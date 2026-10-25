//! Module: imp::partial_ord
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::prelude::*;

///
/// PartialOrdTrait
///

pub struct PartialOrdTrait {}

///
/// Newtype
///

impl Imp<Newtype> for PartialOrdTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let primitive = node.item.primitive.as_ref()?; // bail early if no primitive
        let ident = &node.def.ident();
        let prim = &crate::types::primitive_type_tokens(*primitive, paths);
        Some(TraitStrategy::from_impl(newtype_partial_ord_tokens(
            ident, prim,
        )))
    }
}

fn newtype_partial_ord_tokens(ident: &Ident, prim: &TokenStream) -> TokenStream {
    quote! {
        impl ::core::cmp::PartialOrd<#prim> for #ident {
            fn partial_cmp(&self, other: &#prim) -> ::core::option::Option<::core::cmp::Ordering> {
                <#prim as ::core::cmp::PartialOrd>::partial_cmp(&self.0, other)
            }
        }

        impl ::core::cmp::PartialOrd<#ident> for #prim {
            fn partial_cmp(&self, other: &#ident) -> ::core::option::Option<::core::cmp::Ordering> {
                <#prim as ::core::cmp::PartialOrd>::partial_cmp(self, &other.0)
            }
        }
    }
}
