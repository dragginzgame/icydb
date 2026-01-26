use crate::prelude::*;

///
/// InnerTrait
///

pub struct InnerTrait {}

///
/// Newtype
///

impl Imp<Newtype> for InnerTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let primitive = node.primitive.as_ref()?; // bail early if no primitive

        // otherwise create the trait
        let ty = primitive.as_type();
        let q = quote! {
            fn inner(&self) -> &#ty {
                self.0.inner()
            }

            fn into_inner(self) -> #ty {
                self.0.into_inner()
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inner)
            .add_trait_generic(quote!(#ty))
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
