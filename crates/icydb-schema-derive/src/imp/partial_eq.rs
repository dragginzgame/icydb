use crate::prelude::*;

///
/// PartialEqTrait
///

pub struct PartialEqTrait {}

///
/// Newtype
///

impl Imp<Newtype> for PartialEqTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let ident = &node.def.ident();
        let prim = &primitive.as_type();
        Some(TraitStrategy::from_impl(newtype_partial_eq_tokens(
            ident, prim,
        )))
    }
}

fn newtype_partial_eq_tokens(ident: &Ident, prim: &TokenStream) -> TokenStream {
    quote! {
        impl PartialEq<#prim> for #ident {
            fn eq(&self, other: &#prim) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<#ident> for #prim {
            fn eq(&self, other: &#ident) -> bool {
                *self == other.0
            }
        }
    }
}
