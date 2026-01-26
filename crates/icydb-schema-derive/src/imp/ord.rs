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

        // quote
        let tokens = quote! {
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
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
