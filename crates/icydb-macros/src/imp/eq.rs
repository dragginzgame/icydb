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

        // quote
        let tokens = quote! {
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
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
