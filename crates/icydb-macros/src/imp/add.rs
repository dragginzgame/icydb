use crate::prelude::*;

///
/// AddTrait
///

pub struct AddTrait {}

impl Imp<Newtype> for AddTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let ident = &node.def.ident();
        let prim = &primitive.as_type();

        // quote
        let cp = paths().core;
        let tokens = quote! {
            impl #cp::traits::Add<Self> for #ident {
                type Output = Self;

                fn add(self, other: Self) -> Self::Output {
                    Self(self.0 + other.0)
                }
            }

            impl #cp::traits::Add<#prim> for #ident {
                type Output = Self;

                fn add(self, other: #prim) -> Self::Output {
                    Self(self.0 + other)
                }
            }

            impl #cp::traits::Add<#ident> for #prim {
                type Output = #ident;

                fn add(self, other: #ident) -> Self::Output {
                    #ident(self + other.0)
                }
            }
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// AddAssignTrait
///

pub struct AddAssignTrait {}

impl Imp<Newtype> for AddAssignTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        let ident = &node.def.ident();
        let prim = &primitive.as_type();

        // quote
        let cp = paths().core;
        let tokens = quote! {
            impl #cp::traits::AddAssign<#prim> for #ident {
                fn add_assign(&mut self, other: #prim) {
                    self.0 += other;
                }
            }

            impl #cp::traits::AddAssign<#ident> for #ident {
                fn add_assign(&mut self, other: #ident) {
                    self.0 += other.0;
                }
            }
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
