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

            fn merge(
                &mut self,
                update: Self::UpdateViewType,
            ) -> ::core::result::Result<(), ::icydb::traits::Error> {
                *self = update.into();

                Ok(())
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
        let merge_parts = values.iter().enumerate().map(|(i, _)| {
            let idx = syn::Index::from(i);
            quote! {
                if let Some(v) = update.#idx {
                    ::icydb::traits::UpdateView::merge(&mut next.#idx, v)
                        .map_err(::icydb::traits::Error::from)
                        .map_err(|err| err.with_index(#i))?;
                }
            }
        });

        // quote
        let q = quote! {
            type UpdateViewType = #update_ident;

            fn merge(
                &mut self,
                update: Self::UpdateViewType,
            ) -> ::core::result::Result<(), ::icydb::traits::Error> {
                let mut next = self.clone();
                #(#merge_parts)*
                *self = next;

                Ok(())
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
    let merge_pairs: Vec<_> = field_idents
        .iter()
        .map(|ident| {
            quote! {
                if let Some(v) = update.#ident {
                    ::icydb::traits::UpdateView::merge(&mut next.#ident, v)
                        .map_err(::icydb::traits::Error::from)
                        .map_err(|err| err.with_field(stringify!(#ident)))?;
                }
            }
        })
        .collect();

    let q = quote! {
        type UpdateViewType = #update_ident;

        fn merge(
            &mut self,
            update: Self::UpdateViewType,
        ) -> ::core::result::Result<(), ::icydb::traits::Error> {
            let mut next = self.clone();
            #(#merge_pairs)*
            *self = next;

            Ok(())
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
    let q = quote! {
        type UpdateViewType = #update_ident;

        fn merge(
            &mut self,
            update: Self::UpdateViewType,
        ) -> ::core::result::Result<(), ::icydb::traits::Error> {
            // Forward to the inner collection (Vec, BTreeSet, BTreeMap)
            let mut next = self.clone();
            ::icydb::traits::UpdateView::merge(&mut next.0, update)
                .map_err(::icydb::traits::Error::from)
                .map_err(|err| err.with_index(0))?;
            *self = next;

            Ok(())
        }
    };

    TraitStrategy::from_impl(
        Implementor::new(node.def(), TraitKind::UpdateView)
            .set_tokens(q)
            .to_token_stream(),
    )
}
