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
        let primitive = node.primitive.as_ref()?; // bail early if no primitive
        let ident = &node.def.ident();
        let prim = &primitive.as_type();
        Some(TraitStrategy::from_impl(newtype_partial_ord_tokens(
            ident, prim,
        )))
    }
}

fn newtype_partial_ord_tokens(ident: &Ident, prim: &TokenStream) -> TokenStream {
    quote! {
        impl PartialOrd<#prim> for #ident {
            fn partial_cmp(&self, other: &#prim) -> Option<::std::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }

        impl PartialOrd<#ident> for #prim {
            fn partial_cmp(&self, other: &#ident) -> Option<::std::cmp::Ordering> {
                self.partial_cmp(&other.0)
            }
        }
    }
}
