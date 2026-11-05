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
        impl ::std::cmp::PartialOrd<#prim> for #ident {
            fn partial_cmp(&self, other: &#prim) -> ::std::option::Option<::std::cmp::Ordering> {
                <#prim as ::std::cmp::PartialOrd>::partial_cmp(&self.0, other)
            }
        }

        impl ::std::cmp::PartialOrd<#ident> for #prim {
            fn partial_cmp(&self, other: &#ident) -> ::std::option::Option<::std::cmp::Ordering> {
                <#prim as ::std::cmp::PartialOrd>::partial_cmp(self, &other.0)
            }
        }
    }
}
