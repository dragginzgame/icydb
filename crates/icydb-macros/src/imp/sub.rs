use crate::prelude::*;

///
/// SubTrait
///

pub struct SubTrait {}

impl Imp<Newtype> for SubTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let ident = &node.def.ident();
        let prim = &primitive.as_type();

        // quote
        let tokens = quote! {
            impl ::icydb::core::traits::Sub<Self> for #ident {
                type Output = Self;

                fn sub(self, other: Self) -> Self::Output {
                    Self(self.0 - other.0)
                }
            }

            impl ::icydb::core::traits::Sub<#prim> for #ident {
                type Output = Self;

                fn sub(self, other: #prim) -> Self::Output {
                    Self(self.0 - other)
                }
            }

            impl ::icydb::core::traits::Sub<#ident> for #prim {
                type Output = #ident;

                fn sub(self, other: #ident) -> Self::Output {
                    #ident(self - other.0)
                }
            }

        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// SubAssignTrait
///

pub struct SubAssignTrait {}

impl Imp<Newtype> for SubAssignTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let ident = &node.def.ident();
        let prim = &primitive.as_type();

        // quote
        let tokens = quote! {
            impl ::icydb::core::traits::SubAssign<#prim> for #ident {
                fn sub_assign(&mut self, other: #prim) {
                    self.0 -= other;
                }
            }

            impl ::icydb::core::traits::SubAssign<#ident> for #ident {
                fn sub_assign(&mut self, other: #ident) {
                    self.0 -= other.0;
                }
            }
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
