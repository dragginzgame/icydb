use crate::prelude::*;

///
/// UpdateViewTrait
///

pub struct UpdateViewTrait {}

impl Imp<Entity> for UpdateViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let update_ident = node.update_ident();

        // Build the trait implementation
        let q = quote! {
            type UpdateViewType = #update_ident;
        };

        let update_impl = Implementor::new(node.def(), TraitKind::CreateView)
            .set_tokens(q)
            .to_token_stream();

        // Merge both impls
        let tokens = quote! {
            #update_impl
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
