use crate::prelude::*;

///
/// UpdateViewTrait
///

pub struct UpdateViewTrait {}

impl Imp<Entity> for UpdateViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(update_impl_fields(node, |n| {
            n.iter_editable_fields().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<Record> for UpdateViewTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(update_impl_fields(node, |n| {
            n.fields.iter().map(|f| f.ident.clone()).collect()
        }))
    }
}

impl Imp<List> for UpdateViewTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Set> for UpdateViewTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Map> for UpdateViewTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

impl Imp<Newtype> for UpdateViewTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        Some(update_impl_delegate(node))
    }
}

///
/// Enum
///

impl Imp<Enum> for UpdateViewTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let update_ident = node.update_ident();

        let q = quote! {
            type UpdateViewType = #update_ident;

            fn merge(&mut self, update: Self::UpdateViewType) {
                *self = update.into();
            }
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::UpdateView)
                .set_tokens(q)
                .to_token_stream(),
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for UpdateViewTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let update_ident = node.update_ident();
        let values = &node.values;

        // paths
        let cp = paths().core;
        let merge_parts = values.iter().enumerate().map(|(i, _)| {
            let idx = syn::Index::from(i);
            quote! {
                if let Some(v) = update.#idx {
                    #cp::traits::UpdateView::merge(&mut self.#idx, v);
                }
            }
        });

        // quote
        let q = quote! {
            type UpdateViewType = #update_ident;

            fn merge(&mut self, update: Self::UpdateViewType) {
                #(#merge_parts)*
            }
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::UpdateView)
                .set_tokens(q)
                .to_token_stream(),
        ))
    }
}

///
/// Shared Generators
///

fn update_impl_fields<N, F>(node: &N, iter_fields: F) -> TraitStrategy
where
    N: HasType,
    F: Fn(&N) -> Vec<syn::Ident>,
{
    let update_ident = node.update_ident();
    let field_idents = iter_fields(node);

    // paths
    let cp = paths().core;
    let merge_pairs: Vec<_> = field_idents
        .iter()
        .map(|ident| {
            quote! {
                if let Some(v) = update.#ident {
                    #cp::traits::UpdateView::merge(&mut self.#ident, v);
                }
            }
        })
        .collect();

    let q = quote! {
        type UpdateViewType = #update_ident;

        fn merge(&mut self, update: Self::UpdateViewType) {
            #(#merge_pairs)*
        }
    };

    let update_impl = Implementor::new(node.def(), TraitKind::UpdateView)
        .set_tokens(q)
        .to_token_stream();

    let tokens = quote! {
        #update_impl
    };

    TraitStrategy::from_impl(tokens)
}

fn update_impl_delegate(node: &impl HasType) -> TraitStrategy {
    let update_ident = node.update_ident();

    // quote
    let cp = paths().core;
    let q = quote! {
        type UpdateViewType = #update_ident;

        fn merge(&mut self, update: Self::UpdateViewType) {
            // Forward to the inner collection (Vec, HashSet, HashMap)
            #cp::traits::UpdateView::merge(&mut self.0, update);
        }
    };

    TraitStrategy::from_impl(
        Implementor::new(node.def(), TraitKind::UpdateView)
            .set_tokens(q)
            .to_token_stream(),
    )
}
