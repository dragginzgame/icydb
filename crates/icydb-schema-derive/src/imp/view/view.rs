use crate::prelude::*;

///
/// AsViewTrait
///

pub struct AsViewTrait {}

///
/// Entity
///

impl Imp<Entity> for AsViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let ident = node.def.ident();
        let view_ident = &node.view_ident();

        // tokens
        let q = field_list(view_ident, &node.fields);
        let view_impl = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();
        let conversions = owned_view_conversions(&ident, view_ident);
        let tokens = quote! {
            #view_impl
            #conversions
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Enum
///

impl Imp<Enum> for AsViewTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let ident = node.def.ident();
        let view_ident = &node.view_ident();

        // as_view_arms
        let as_view_arms = node.variants.iter().map(|variant| {
            let variant_ident = &variant.ident;

            if variant.value.is_some() {
                quote! {
                    Self::#variant_ident(v) => Self::ViewType::#variant_ident(v.as_view())
                }
            } else {
                quote! {
                    Self::#variant_ident => Self::ViewType::#variant_ident
                }
            }
        });

        // from_view_arms
        let from_view_arms = node.variants.iter().map(|variant| {
            let variant_ident = &variant.ident;

            if variant.value.is_some() {
                quote! {
                    Self::ViewType::#variant_ident(v) => {
                        Self::#variant_ident(::icydb::traits::AsView::from_view(v))
                    }
                }
            } else {
                quote! {
                    Self::ViewType::#variant_ident => Self::#variant_ident
                }
            }
        });

        let q = quote! {
                type ViewType = #view_ident;

                fn as_view(&self) -> Self::ViewType {
                    match self {
                        #(#as_view_arms),*
                    }
                }

                fn from_view(view: Self::ViewType) -> Self {
                    match view {
                        #(#from_view_arms),*
                    }
                }
        };

        // tokens
        let view_impl = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();
        let conversions = owned_view_conversions(&ident, view_ident);
        let tokens = quote! {
            #view_impl
            #conversions
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for AsViewTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let view_ident = &node.view_ident();
        let q = quote_view_delegate(view_ident);

        let tokens = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Map
///

impl Imp<Map> for AsViewTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let view_ident = &node.view_ident();
        let q = quote_view_delegate(view_ident);

        let tokens = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for AsViewTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let view_ident = &node.view_ident();
        let q = quote_view_delegate(view_ident);

        let tokens = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for AsViewTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let ident = node.def.ident();
        let view_ident = &node.view_ident();
        let q = field_list(view_ident, &node.fields);

        let view_impl = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();
        let conversions = owned_view_conversions(&ident, view_ident);
        let tokens = quote! {
            #view_impl
            #conversions
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Set
///

impl Imp<Set> for AsViewTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let view_ident = &node.view_ident();
        let q = quote_view_delegate(view_ident);

        let tokens = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for AsViewTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let view_ident = node.view_ident();
        let indices: Vec<_> = (0..node.values.len()).collect();

        let as_view_fields = indices.iter().map(|i| {
            let index = syn::Index::from(*i);
            quote! {
                ::icydb::traits::AsView::as_view(&self.#index)
            }
        });

        let from_view_fields = indices.iter().map(|i| {
            let index = syn::Index::from(*i);
            quote! {
                ::icydb::traits::AsView::from_view(view.#index)
            }
        });

        let q = quote! {
            type ViewType = #view_ident;

            fn as_view(&self) -> Self::ViewType {
                (
                    #(#as_view_fields),*
                )
            }

            fn from_view(view: Self::ViewType) -> Self {
                Self(
                    #(#from_view_fields),*
                )
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::AsView)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Helpers
///

// field_list
fn field_list(view_ident: &Ident, fields: &FieldList) -> TokenStream {
    let to_pairs: Vec<_> = fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            quote! {
                #ident: ::icydb::traits::AsView::as_view(&self.#ident)
            }
        })
        .collect();

    let from_pairs: Vec<_> = fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            quote! {
                #ident: ::icydb::traits::AsView::from_view(view.#ident)
            }
        })
        .collect();

    quote! {
        type ViewType = #view_ident;

        fn as_view(&self) -> Self::ViewType {
            #view_ident {
                #(#to_pairs),*
            }
        }

        fn from_view(view: Self::ViewType) -> Self {
            Self {
                #(#from_pairs),*
            }
        }
    }
}

fn owned_view_conversions(ident: &Ident, view_ident: &Ident) -> TokenStream {
    quote! {
        impl From<#ident> for #view_ident {
            fn from(value: #ident) -> Self {
                ::icydb::traits::AsView::as_view(&value)
            }
        }

        impl From<&#ident> for #view_ident {
            fn from(value: &#ident) -> Self {
                ::icydb::traits::AsView::as_view(value)
            }
        }

        impl From<#view_ident> for #ident {
            fn from(view: #view_ident) -> Self {
                ::icydb::traits::AsView::from_view(view)
            }
        }
    }
}

fn quote_view_delegate(view_ident: &Ident) -> TokenStream {
    quote! {
        type ViewType = #view_ident;

        fn as_view(&self) -> Self::ViewType {
            ::icydb::traits::AsView::as_view(&self.0)
        }

        fn from_view(view: Self::ViewType) -> Self {
            Self(::icydb::traits::AsView::from_view(view))
        }
    }
}
